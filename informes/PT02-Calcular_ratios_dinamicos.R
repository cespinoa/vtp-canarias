# ==============================================================================
# SCRIPT 2: calcular_ratios_dinamicos.R
# Objetivo: Motor dinámico con tipado por diccionario y fallo explícito
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("--- INICIANDO MOTOR DE CÁLCULO DINÁMICO ---\n")

# 1. Cargar datos base y metadatos del Diccionario
df_trabajo <- dbGetQuery(con, "SELECT * FROM base_snapshots") %>% 
  mutate(across(where(is.numeric), as.numeric))

# Traemos todo el diccionario para tener fórmulas y formatos
diccionario_completo <- dbGetQuery(con, "SELECT id_campo, formula, orden_de_calculo, formato FROM diccionario_de_datos")

# Filtramos las que tienen fórmula para el motor de cálculo y ORDENAMOS
diccionario_formulas <- diccionario_completo %>% 
  filter(!is.na(formula) & formula != "") %>% 
  arrange(orden_de_calculo)

# 2. Separar Fórmulas Literales de Benchmarks
formulas_literales <- diccionario_formulas %>% filter(!str_detect(formula, "avg\\(|max\\("))
formulas_bench     <- diccionario_formulas %>% filter(str_detect(formula, "avg\\(|max\\("))

# 3. Ejecutar Cálculos Literales fila a fila
cat("Calculando ratios literales por orden de precedencia...\n")
for(i in 1:nrow(formulas_literales)) {
    v_col <- formulas_literales$id_campo[i]
    v_for <- formulas_literales$formula[i]
    cat("  - Procesando:", v_col, "\n")
    df_trabajo <- df_trabajo %>% mutate(!!v_col := eval(parse(text = v_for)))
}

# 4. Calcular Benchmarks (Medias y Máximos)
cat("Calculando benchmarks segmentados por ámbito y tipo...\n")
df_final <- df_trabajo %>% group_by(ambito, tipo_municipio)

for(i in 1:nrow(formulas_bench)) {
    v_col <- formulas_bench$id_campo[i]
    v_raw <- formulas_bench$formula[i]
    campo_base <- str_extract(v_raw, "(?<=avg\\(|max\\().*?(?=\\))")
    
    cat("  - Referenciando benchmark:", v_col, "\n")
    
    excluir_100 <- str_detect(v_raw, fixed("| Excluyendo valores 100"))

    if(str_detect(v_raw, "avg")) {
        df_final <- df_final %>% mutate(!!v_col := {
            x <- .data[[campo_base]]
            if(excluir_100) x <- x[x < 100]
            x <- x[!is.na(x) & is.finite(x)]
            if(length(x) == 0) NA_real_ else mean(x)
        })
    } else if(str_detect(v_raw, "max")) {
        df_final <- df_final %>% mutate(!!v_col := {
            x <- .data[[campo_base]]
            if(excluir_100) x <- x[x < 100]
            x <- x[!is.na(x) & is.finite(x)]
            if(length(x) == 0) NA_real_ else max(x)
        })
    }
}
df_final <- df_final %>% ungroup()

# --- 5. LIMPIEZA Y TIPADO POR DICCIONARIO ---
cat("Aplicando formatos según el diccionario...\n")

campos_enteros <- diccionario_completo %>% 
  filter(formato == 'entero') %>% 
  pull(id_campo)

df_post <- df_final %>%
  mutate(
    across(c(isla_id, municipio_id, localidad_id), ~ if_else(is.na(.) | . == 0, NA_integer_, as.integer(.))),
    across(where(is.numeric), ~ if_else(is.finite(.), ., NA_real_)),
    across(any_of(campos_enteros), ~ as.integer(round(coalesce(., 0))))
  )

# --- 6. GESTIÓN DE DUPLICADOS EN EL HISTÓRICO ---
# Extraemos la fecha única de nuestro proceso actual (está en la columna fecha_calculo)
fecha_proceso <- as.character(unique(df_post$fecha_calculo)[1])
fecha_a_borrar <- shQuote(paste0(as.character(fecha_proceso), " 00:00:00"), type = "sh")

if (!is.na(fecha_a_borrar)) {
    cat("Comprobando si existe snapshot previo para la fecha:", fecha_a_borrar, "...\n")
    
    # Ejecutamos el borrado preventivo en full_snapshots
    # Usamos un casting explícito a ::date o ::timestamp según tu tabla
    n_borrados <- dbExecute(con, glue::glue("DELETE FROM full_snapshots WHERE fecha_calculo = {fecha_a_borrar}"))
    
    if (n_borrados > 0) {
        cat("  - Se han eliminado", n_borrados, "registros antiguos para evitar duplicados.\n")
    } else {
        cat("  - No existen registros previos para esta fecha. Procediendo...\n")
    }
}

# --- 7. VOLCADO FINAL (Fallo explícito si falta columna en DB)
cat("Volcando datos a full_snapshots...\n")
# Eliminamos la columna 'id' para que PostgreSQL use su propio serial auto-incremental
df_envio <- df_post %>% select(-any_of("id"))

dbWriteTable(con, "full_snapshots", df_envio, append = TRUE, row.names = FALSE)
cat("¡Proceso completado con éxito!\n")
