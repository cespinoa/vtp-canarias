#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_estancia_reglada.R
# Carga el histórico de estancia media del alojamiento turístico reglado
# (ISTAC C00065A_000039) desde el CSV descargado por istac_estancia_reglada.py.
#
# La estancia media se calcula en el script Python como:
#   ESTANCIA_MEDIA = PERNOCTACIONES_total / VIAJEROS_ENTRADOS_total
# sumando las 27 nacionalidades disponibles (el dataset no publica _T).
#
# Uso:
#   Rscript descarga_datos/importar_estancia_reglada.R
#   Rscript descarga_datos/importar_estancia_reglada.R ruta/al/fichero.csv
#
# Cobertura: anual 2009–año más reciente publicado. Canarias + 7 islas.
# Estrategia: TRUNCATE + reload completo.
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/estancia_reglada_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
islas_db <- dbGetQuery(con, "SELECT id, geo_code, nombre FROM islas")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     territorio_codigo = col_character(),
                     ejercicio         = col_integer(),
                     estancia_media    = col_double()
                   ))

ejercicios <- sort(unique(df_raw$ejercicio))
cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Ejercicios:", paste(ejercicios, collapse = " "), "\n\n")

# --- 4. ASIGNAR ÁMBITO E IDS ---
tabla_final <- df_raw %>%
  mutate(
    ambito  = if_else(territorio_codigo == "ES70", "canarias", "isla"),
    isla_id = NA_integer_
  )

islas_join <- df_raw %>%
  filter(territorio_codigo != "ES70") %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  select(territorio_codigo, ejercicio, id)

tabla_final <- tabla_final %>%
  left_join(islas_join, by = c("territorio_codigo", "ejercicio")) %>%
  mutate(isla_id = if_else(ambito == "isla", id, NA_integer_)) %>%
  select(ejercicio, ambito, isla_id, estancia_media)

# Diagnóstico
sin_emparejar <- df_raw %>%
  filter(territorio_codigo != "ES70",
         !territorio_codigo %in% islas_db$geo_code) %>%
  pull(territorio_codigo) %>% unique()
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — territorios sin emparejar:", paste(sin_emparejar, collapse = " "), "\n")
} else {
  cat("OK: todos los territorios emparejados.\n")
}

cat("\nRegistros a cargar por ámbito:\n")
print(tabla_final %>% count(ambito))
cat("Total:", nrow(tabla_final), "\n\n")

# --- 5. CARGA (TRUNCATE + reload) ---
cat("TRUNCATE + carga...\n")
dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE historico_estancia_media_reglada")
  dbWriteTable(con, "historico_estancia_media_reglada", tabla_final,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 6. RESUMEN FINAL ---
cat("\nEvolución Canarias (estancia media reglada, días):\n")
print(dbGetQuery(con,
  "SELECT ejercicio, estancia_media
   FROM historico_estancia_media_reglada
   WHERE ambito = 'canarias'
   ORDER BY ejercicio"))

dbDisconnect(con)
cat("Proceso completado.\n")
