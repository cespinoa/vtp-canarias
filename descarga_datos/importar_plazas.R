#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_plazas.R
# Carga el histórico de plazas turísticas regladas por isla y año
# (ISTAC C00065A_000033) desde el CSV descargado por istac_plazas.py.
#
# Uso:
#   Rscript descarga_datos/importar_plazas.R
#   Rscript descarga_datos/importar_plazas.R ruta/al/fichero.csv
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
  candidatos <- Sys.glob("descarga_datos/tmp/plazas_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
islas_db <- dbGetQuery(con, "SELECT id, geo_code, nombre FROM islas")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     territorio_codigo  = col_character(),
                     ejercicio          = col_integer(),
                     plazas             = col_integer(),
                     tasa_ocupacion_plaza = col_double()
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

# Para las islas, cruzar con la tabla islas
islas_join <- df_raw %>%
  filter(territorio_codigo != "ES70") %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  select(territorio_codigo, ejercicio, id)

tabla_final <- tabla_final %>%
  left_join(islas_join, by = c("territorio_codigo", "ejercicio")) %>%
  mutate(isla_id = if_else(ambito == "isla", id, NA_integer_)) %>%
  select(ejercicio, ambito, isla_id, plazas)

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

# Separar columna tasa antes de construir tabla_final de plazas
df_tasa <- df_raw %>% filter(!is.na(tasa_ocupacion_plaza))

# --- 5. VALIDACIÓN: suma islas ≈ canarias ---
cat("Validando suma islas vs Canarias (últimos 3 ejercicios):\n")
suma_islas <- tabla_final %>%
  filter(ambito == "isla") %>%
  group_by(ejercicio) %>%
  summarise(suma_islas = sum(plazas), .groups = "drop")

canarias_val <- tabla_final %>%
  filter(ambito == "canarias") %>%
  select(ejercicio, plazas_canarias = plazas)

check <- inner_join(canarias_val, suma_islas, by = "ejercicio") %>%
  mutate(diff = plazas_canarias - suma_islas,
         pct  = round(100 * diff / plazas_canarias, 2)) %>%
  arrange(desc(ejercicio)) %>% head(3)
print(check)

# --- 6. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE historico_plazas_regladas")
  dbWriteTable(con, "historico_plazas_regladas", tabla_final,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 7. CARGAR TASA EN historico_tasa_ocupacion_reglada ---
cat("\nCargando tasa de ocupación por plaza...\n")

tabla_tasa <- df_tasa %>%
  mutate(
    ambito  = if_else(territorio_codigo == "ES70", "canarias", "isla"),
    isla_id = NA_integer_
  )

islas_join_tasa <- df_tasa %>%
  filter(territorio_codigo != "ES70") %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  select(territorio_codigo, ejercicio, id)

tabla_tasa <- tabla_tasa %>%
  left_join(islas_join_tasa, by = c("territorio_codigo", "ejercicio")) %>%
  mutate(isla_id = if_else(ambito == "isla", id, NA_integer_)) %>%
  select(ejercicio, ambito, isla_id, tasa = tasa_ocupacion_plaza)

cat("Registros a cargar:\n")
print(tabla_tasa %>% count(ambito))

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE historico_tasa_ocupacion_reglada")
  dbWriteTable(con, "historico_tasa_ocupacion_reglada", tabla_tasa,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_tasa), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga de tasa: ", conditionMessage(e))
})

# --- 8. RESUMEN FINAL ---
cat("\nEvolución Canarias (plazas totales):\n")
print(dbGetQuery(con,
  "SELECT ejercicio, plazas
   FROM historico_plazas_regladas
   WHERE ambito = 'canarias'
   ORDER BY ejercicio"))

cat("\nEvolución Canarias (tasa ocupación plaza %):\n")
print(dbGetQuery(con,
  "SELECT ejercicio, tasa
   FROM historico_tasa_ocupacion_reglada
   WHERE ambito = 'canarias'
   ORDER BY ejercicio"))

dbDisconnect(con)
cat("Proceso completado.\n")
