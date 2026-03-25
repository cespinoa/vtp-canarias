# ==============================================================================
# SCRIPT: PT04-importar_historico_islas.R
# Objetivo: Rescatar datos de VV de 2019, 2023 y 2024 desde la tabla islas
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

# --- 0. CONEXIÓN ---
source("importar_gobcan/helper.R")
con <- conecta_db()

cat("--- INICIANDO IMPORTACIÓN DE DATOS HISTÓRICOS DESDE TABLA ISLAS ---\n")

# 1. Configuración de periodos: Columna en DB -> Fecha de Snapshot
periodos <- list(
  list(col_isla = "diciembre_2019", fecha = "2019-12-31"),
  list(col_isla = "junio_2023",     fecha = "2023-06-30"),
  list(col_isla = "abril_2024",     fecha = "2024-04-30")
)

# 2. Obtener datos básicos de las islas
# Traemos el ID, el nombre y las columnas de valores históricos
islas_data <- dbGetQuery(con, "SELECT id, nombre, diciembre_2019, junio_2023, abril_2024 FROM islas")

historico_total <- list()

for(p in periodos) {
  cat("Procesando fecha:", p$fecha, "\n")
  
  # A. Registros para cada ISLA
  df_islas <- islas_data %>%
    transmute(
      ambito = "isla",
      isla_id = as.integer(id),
      municipio_id = NA_integer_,
      localidad_id = NA_integer_,
      etiqueta = nombre,
      uds_vv_total = as.integer(!!sym(p$col_isla)),
      fecha_calculo = as.Date(p$fecha),
      tipo_municipio = "General",
      etiqueta_ambito_superior = "Canarias"
    )
  
  # B. Registro para CANARIAS (Sumatorio de lo anterior)
  df_canarias <- data.frame(
    ambito = "canarias",
    isla_id = NA_integer_,
    municipio_id = NA_integer_,
    localidad_id = NA_integer_,
    etiqueta = "Canarias",
    uds_vv_total = as.integer(sum(df_islas$uds_vv_total, na.rm = TRUE)),
    fecha_calculo = as.Date(p$fecha),
    tipo_municipio = "General",
    etiqueta_ambito_superior = NA_character_
  )
  
  historico_total[[length(historico_total) + 1]] <- bind_rows(df_canarias, df_islas)
}

# 3. Consolidar y Volcar
df_historico_final <- bind_rows(historico_total)

# Limpieza preventiva: borramos si ya existieran estas fechas para evitar duplicados
fechas_str <- paste0("'", sapply(periodos, function(x) x$fecha), "'", collapse = ", ")
dbExecute(con, glue::glue("DELETE FROM full_snapshots WHERE fecha_calculo IN ({fechas_str})"))

# Inserción en full_snapshots
# Dejamos que el ID de la tabla se genere solo
dbWriteTable(con, "full_snapshots", df_historico_final, append = TRUE, row.names = FALSE)

cat("¡Éxito! Se han insertado", nrow(df_historico_final), "registros históricos.\n")
