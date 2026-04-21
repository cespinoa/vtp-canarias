#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_ipv.R
# Carga el Índice de Precios de la Vivienda (IPV, base 2015) del INE
# (tabla 25171) desde el CSV descargado por ine_ipv.py.
#
# Territorios: Nacional (00) y Canarias (05).
# Tipos: general, nueva, segunda_mano.
# Cobertura: Q4 2007 – trimestre más reciente publicado.
#
# Estrategia: TRUNCATE + reload completo (el INE revisa datos retroactivos).
#
# Uso:
#   Rscript descarga_datos/importar_ipv.R
#   Rscript descarga_datos/importar_ipv.R ruta/al/fichero.csv
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/ipv_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE)

anyos <- sort(unique(df_raw$anyo))
cat("Años:", paste(anyos[1], anyos[length(anyos)], sep = "–"), "\n")
cat("Territorios:", paste(sort(unique(df_raw$territorio_codigo)), collapse = ", "), "\n")
cat("Filas:", nrow(df_raw), "\n\n")

# --- 3. TRUNCATE + RELOAD ---
cat("Truncando ipv_vivienda...\n")
dbExecute(con, "TRUNCATE TABLE ipv_vivienda RESTART IDENTITY")

cat("Insertando", nrow(df_raw), "filas...\n")
dbWriteTable(con, "ipv_vivienda", df_raw, append = TRUE, row.names = FALSE)

# --- 4. VERIFICACIÓN ---
n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM ipv_vivienda")$n
cat(sprintf("✓ %d filas en ipv_vivienda.\n\n", as.integer(n)))

muestra <- dbGetQuery(con, "
  SELECT territorio_codigo, anyo, trimestre, tipo_vivienda,
         indice, variacion_anual, variacion_trimestral
  FROM ipv_vivienda
  WHERE territorio_codigo = '05'
    AND (anyo, trimestre) = (SELECT anyo, MAX(trimestre) FROM ipv_vivienda
                             WHERE anyo = (SELECT MAX(anyo) FROM ipv_vivienda)
                             GROUP BY anyo)
  ORDER BY tipo_vivienda")
cat("Canarias, último trimestre disponible:\n")
print(muestra)

dbDisconnect(con)
