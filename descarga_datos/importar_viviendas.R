#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_viviendas.R
# Carga los datos de viviendas por municipio del INE (tabla 59531, Censo 2021)
# desde el CSV descargado por ine_viviendas.py.
#
# Uso:
#   Rscript descarga_datos/importar_viviendas.R
#   Rscript descarga_datos/importar_viviendas.R ruta/al/fichero.csv
#
# La tabla viviendas_municipios almacena los tres ámbitos (canarias, isla,
# municipio). Los niveles isla y canarias se calculan aquí por agregación.
#
# Campos: total, vacias, esporadicas, habituales (= total - vacias - esporadicas)
# La tabla tiene CHECK: total = vacias + esporadicas + habituales.
#
# Estrategia: TRUNCATE + reload completo (snapshot único, sin dimensión temporal).
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/ine_viviendas_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
municipios_db <- dbGetQuery(con, "SELECT id, isla_id, codigo_ine FROM municipios WHERE codigo_ine IS NOT NULL")
islas_db      <- dbGetQuery(con, "SELECT id, nombre FROM islas")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     codigo_ine   = col_character(),
                     nombre       = col_character(),
                     total        = col_integer(),
                     vacias       = col_integer(),
                     esporadicas  = col_integer()
                   ))

cat("Municipios en CSV:", nrow(df_raw), "\n")

# Diagnóstico: códigos sin emparejar
sin_emparejar <- setdiff(df_raw$codigo_ine, municipios_db$codigo_ine)
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — códigos INE sin emparejar:\n")
  print(sin_emparejar)
} else {
  cat("OK: todos los códigos emparejados.\n")
}

# --- 4. NIVEL MUNICIPIO ---
mun <- df_raw %>%
  inner_join(municipios_db, by = "codigo_ine") %>%
  mutate(
    ambito       = "municipio",
    municipio_id = id,
    habituales   = total - vacias - esporadicas
  ) %>%
  select(ambito, isla_id, municipio_id, total, vacias, esporadicas, habituales)

# --- 5. NIVEL ISLA (agregación) ---
isla <- mun %>%
  group_by(isla_id) %>%
  summarise(
    total       = sum(total),
    vacias      = sum(vacias),
    esporadicas = sum(esporadicas),
    .groups = "drop"
  ) %>%
  mutate(
    ambito       = "isla",
    municipio_id = NA_integer_,
    habituales   = total - vacias - esporadicas
  ) %>%
  select(ambito, isla_id, municipio_id, total, vacias, esporadicas, habituales)

# --- 6. NIVEL CANARIAS (agregación) ---
canarias <- mun %>%
  summarise(
    total       = sum(total),
    vacias      = sum(vacias),
    esporadicas = sum(esporadicas)
  ) %>%
  mutate(
    ambito       = "canarias",
    isla_id      = NA_integer_,
    municipio_id = NA_integer_,
    habituales   = total - vacias - esporadicas
  ) %>%
  select(ambito, isla_id, municipio_id, total, vacias, esporadicas, habituales)

tabla_final <- bind_rows(canarias, isla, mun)

cat("\nRegistros a cargar por ámbito:\n")
print(tabla_final %>% count(ambito))
cat("Total:", nrow(tabla_final), "\n\n")

# --- 7. VALIDACIÓN ---
cat("Validando CHECK: total = vacias + esporadicas + habituales...\n")
invalidos <- tabla_final %>%
  filter(total != vacias + esporadicas + habituales)
if (nrow(invalidos) > 0) {
  cat("ERROR — filas que violan el CHECK:\n")
  print(invalidos)
  stop("Abortando carga.")
} else {
  cat("OK: todas las filas satisfacen el CHECK.\n")
}

cat("\nSuma por isla (municipios vs total isla):\n")
comp <- isla %>%
  left_join(islas_db, by = c("isla_id" = "id")) %>%
  select(nombre, total_isla = total, vacias_isla = vacias, esporadicas_isla = esporadicas) %>%
  arrange(desc(total_isla))
print(comp)

# --- 8. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE viviendas_municipios")
  dbWriteTable(con, "viviendas_municipios", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 9. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT ambito, count(*) n, sum(total) total, sum(vacias) vacias,
          sum(esporadicas) esporadicas, sum(habituales) habituales
   FROM viviendas_municipios
   GROUP BY ambito ORDER BY ambito"))

dbDisconnect(con)
cat("Proceso completado.\n")
