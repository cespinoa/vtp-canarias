#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: PT02-Calcular_ratios_dinamicos.R
# Lee base_snapshots y el diccionario_de_datos para calcular ratios y benchmarks.
# Vuelca el resultado a full_snapshots.
#
# La fecha de proceso se obtiene de base_snapshots (escrita por PT01).
# PT01 ya eliminó los registros previos de full_snapshots para esa fecha.
#
# Modo histórico (parámetro opcional):
#   Rscript informes/PT02-Calcular_ratios_dinamicos.R 2025-06-30
#   La fecha debe existir en full_snapshots. En este modo los datos base se
#   leen directamente desde full_snapshots (no desde base_snapshots) y los
#   campos calculados se recalculan con las fórmulas actuales del diccionario.
#   Útil para propagar nuevos ratios a snapshots históricos.
#   NOTA: los campos cuya base no existía en el snapshot histórico (ej. campos
#   añadidos posteriormente) producirán NULL, lo cual es el comportamiento
#   correcto.
#
# Uso:
#   Rscript informes/PT02-Calcular_ratios_dinamicos.R
#   Rscript informes/PT02-Calcular_ratios_dinamicos.R 2025-06-30
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("PT02 — Cálculo de ratios dinámicos\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

# --- 1. CARGAR DATOS BASE Y DICCIONARIO ---
args <- commandArgs(trailingOnly = TRUE)

modo_historico <- length(args) > 0

if (modo_historico) {
  fecha_param <- args[1]
  if (is.na(as.Date(fecha_param, format = "%Y-%m-%d")))
    stop("Fecha no válida. Use el formato YYYY-MM-DD")

  n_existentes <- dbGetQuery(con, paste0(
    "SELECT COUNT(*) AS n FROM full_snapshots ",
    "WHERE fecha_calculo::date = '", fecha_param, "'"))$n
  if (n_existentes == 0)
    stop("La fecha ", fecha_param, " no existe en full_snapshots.")

  cat("Modo histórico — fecha:", fecha_param, "\n")
  cat("Leyendo datos base desde full_snapshots...\n")

  campos_base <- dbGetQuery(con,
    "SELECT id_campo FROM diccionario_de_datos WHERE tipo = 'base'")$id_campo
  campos_base_calculado <- dbGetQuery(con,
    "SELECT id_campo FROM diccionario_de_datos WHERE tipo = 'base_calculado'")$id_campo

  df_trabajo <- dbGetQuery(con, paste0(
    "SELECT * FROM full_snapshots WHERE fecha_calculo::date = '", fecha_param, "'")) %>%
    mutate(across(where(is.numeric), as.numeric))

  fecha_proceso <- fecha_param
  cat("Filas leídas:", nrow(df_trabajo), "\n\n")

} else {
  df_trabajo <- dbGetQuery(con, "SELECT * FROM base_snapshots") %>%
    mutate(across(where(is.numeric), as.numeric))

  if (nrow(df_trabajo) == 0)
    stop("base_snapshots está vacío. Ejecute PT01 primero.")

  fecha_proceso <- as.character(unique(df_trabajo$fecha_calculo)[1])
  cat("Modo normal\n")
  cat("Filas en base_snapshots:", nrow(df_trabajo), "\n\n")
}

cat("Fecha de proceso:", fecha_proceso, "\n\n")

escribir_log("PT02_INICIO", paste(
  "fecha_proceso:", fecha_proceso,
  "| modo:", if (modo_historico) "historico" else "normal",
  "| filas base:", nrow(df_trabajo)))

diccionario_completo <- dbGetQuery(con,
  "SELECT id_campo, formula, orden_de_calculo, formato FROM diccionario_de_datos")

diccionario_formulas <- diccionario_completo %>%
  filter(!is.na(formula) & formula != "") %>%
  arrange(orden_de_calculo)

# --- 2. SEPARAR FÓRMULAS LITERALES Y BENCHMARKS ---
formulas_literales <- diccionario_formulas %>% filter(!str_detect(formula, "avg\\(|max\\("))
formulas_bench     <- diccionario_formulas %>% filter( str_detect(formula, "avg\\(|max\\("))

# --- 3. CALCULAR FÓRMULAS LITERALES ---
cat("Calculando ratios literales por orden de precedencia...\n")
for (i in 1:nrow(formulas_literales)) {
  v_col <- formulas_literales$id_campo[i]
  v_for <- formulas_literales$formula[i]
  cat("  -", v_col, "\n")
  df_trabajo <- df_trabajo %>% mutate(!!v_col := eval(parse(text = v_for)))
}

# --- 4. CALCULAR BENCHMARKS ---
cat("\nCalculando benchmarks segmentados por ámbito y tipo...\n")
df_final <- df_trabajo %>% group_by(ambito, tipo_municipio)

for (i in 1:nrow(formulas_bench)) {
  v_col      <- formulas_bench$id_campo[i]
  v_raw      <- formulas_bench$formula[i]
  campo_base <- str_extract(v_raw, "(?<=avg\\(|max\\().*?(?=\\))")

  cat("  -", v_col, "\n")

  if (str_detect(v_raw, "avg")) {
    formula_max_equiv <- formulas_bench %>%
      filter(str_detect(formula, paste0("max\\(", campo_base, "\\)"))) %>%
      pull(formula)
    excluir_100 <- length(formula_max_equiv) > 0 &&
      str_detect(formula_max_equiv[1], fixed("| Excluyendo valores 100"))

    val_canarias <- {
      x <- df_final %>% ungroup() %>% filter(ambito == "canarias") %>% pull(!!sym(campo_base))
      if (excluir_100) x <- x[x < 100]
      x <- x[!is.na(x) & is.finite(x)]
      if (length(x) == 0) NA_real_ else x[1]
    }
    df_final <- df_final %>% mutate(!!v_col := {
      if (ambito[1] == "isla") {
        val_canarias
      } else {
        x <- .data[[campo_base]]
        if (excluir_100) x <- x[x < 100]
        x <- x[!is.na(x) & is.finite(x)]
        if (length(x) == 0) NA_real_ else mean(x)
      }
    })
  } else if (str_detect(v_raw, "max")) {
    excluir_100 <- str_detect(v_raw, fixed("| Excluyendo valores 100"))
    df_final <- df_final %>% mutate(!!v_col := {
      x <- .data[[campo_base]]
      if (excluir_100) x <- x[x < 100]
      x <- x[!is.na(x) & is.finite(x)]
      if (length(x) == 0) NA_real_ else max(x)
    })
  }
}
df_final <- df_final %>% ungroup()

# --- 5. TIPADO POR DICCIONARIO ---
cat("\nAplicando formatos según el diccionario...\n")

campos_enteros <- diccionario_completo %>%
  filter(formato == "entero") %>%
  pull(id_campo)

df_post <- df_final %>%
  mutate(
    across(c(isla_id, municipio_id, localidad_id),
           ~ if_else(is.na(.) | . == 0, NA_integer_, as.integer(.))),
    across(where(is.numeric), ~ if_else(is.finite(.), ., NA_real_)),
    across(any_of(campos_enteros), ~ as.integer(round(coalesce(., 0))))
  )

# --- 6. VOLCADO A full_snapshots ---
# PT01 ya eliminó los registros previos para esta fecha.
# Comprobación de seguridad por si PT02 se ejecuta en solitario.
n_previos <- dbGetQuery(con, paste0(
  "SELECT COUNT(*) AS n FROM full_snapshots ",
  "WHERE fecha_calculo = '", fecha_proceso, " 00:00:00'"))$n
if (n_previos > 0) {
  cat("Aviso: eliminando", n_previos, "registros previos (PT02 ejecutado en solitario)...\n")
  dbExecute(con, paste0(
    "DELETE FROM full_snapshots WHERE fecha_calculo = '", fecha_proceso, " 00:00:00'"))
}

cat("Volcando", nrow(df_post), "filas a full_snapshots...\n")
df_envio <- df_post %>% select(-any_of("id"))
dbWriteTable(con, "full_snapshots", df_envio, append = TRUE, row.names = FALSE)

escribir_log("PT02_FIN", paste(
  "fecha_proceso:", fecha_proceso,
  "| modo:", if (modo_historico) "historico" else "normal",
  "| filas volcadas:", nrow(df_post)))

dbDisconnect(con)
cat("\n✓ PT02 completado —", nrow(df_post), "filas en full_snapshots.\n")
