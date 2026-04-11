#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_tamano_hogar_ccaa.R
# Carga el tamaño medio del hogar por CCAA y trimestre
# (INE tabla 60132, ECH) desde el CSV descargado por ine_tamano_hogar_ccaa.py.
#
# Uso:
#   Rscript descarga_datos/importar_tamano_hogar_ccaa.R
#   Rscript descarga_datos/importar_tamano_hogar_ccaa.R ruta/al/fichero.csv
#
# Cobertura: Q1 2021 – trimestre más reciente. Total nacional + 19 CCAA.
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
  candidatos <- Sys.glob("descarga_datos/tmp/tamano_hogar_ccaa_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. LEER CSV ---
df <- read_csv(csv_path, show_col_types = FALSE,
               col_types = cols(
                 ccaa_cod    = col_character(),
                 ccaa_nombre = col_character(),
                 anyo        = col_integer(),
                 trimestre   = col_integer(),
                 miembros    = col_double()
               ))

cat("Filas en CSV:", nrow(df), "\n")
cat("CCAA:", n_distinct(df$ccaa_cod), "\n")
cat("Cobertura:",
    paste0(min(df$anyo), "-T", df$trimestre[which.min(df$anyo)]),
    "→",
    paste0(max(df$anyo), "-T", df$trimestre[which.max(df$anyo)]), "\n\n")

# --- 3. VALIDACIÓN ---
# Verificar que Canarias está presente
if (!"05" %in% df$ccaa_cod) stop("CCAA '05' (Canarias) no encontrada en el CSV.")

# Spot-check: el total nacional debe existir
if (!"00" %in% df$ccaa_cod) warning("Código '00' (Total Nacional) no encontrado.")

# --- 4. CARGA (TRUNCATE + reload) ---
cat("TRUNCATE + carga...\n")
dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE ech_tamano_hogar_ccaa")
  dbWriteTable(con, "ech_tamano_hogar_ccaa", df,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(df), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 5. RESUMEN FINAL ---
cat("\nEvolución Canarias vs Total Nacional (T4 de cada año):\n")
print(dbGetQuery(con, "
  SELECT anyo,
    MAX(CASE WHEN ccaa_cod = '00' THEN miembros END) AS nacional,
    MAX(CASE WHEN ccaa_cod = '05' THEN miembros END) AS canarias
  FROM ech_tamano_hogar_ccaa
  WHERE trimestre = 4
  GROUP BY anyo
  ORDER BY anyo
"))

cat("\nÚltimo trimestre disponible — todas las CCAA:\n")
print(dbGetQuery(con, "
  SELECT ccaa_cod, ccaa_nombre, anyo, trimestre, miembros
  FROM ech_tamano_hogar_ccaa
  WHERE (anyo, trimestre) = (
    SELECT anyo, trimestre FROM ech_tamano_hogar_ccaa
    ORDER BY anyo DESC, trimestre DESC LIMIT 1
  )
  ORDER BY ccaa_cod
"))

dbDisconnect(con)
cat("Proceso completado.\n")
