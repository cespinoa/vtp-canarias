#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_vivienda.R
# Carga los datos de viviendas iniciadas y terminadas (ISTAC E25004A_000001)
# desde el CSV descargado por istac_vivienda.py.
#
# Uso:
#   Rscript descarga_datos/importar_vivienda.R
#   Rscript descarga_datos/importar_vivienda.R ruta/al/fichero.csv
#
# Territorios: ES70 (Canarias), ES701 (Las Palmas), ES702 (SCT)
# Períodos: anuales (YYYY) y mensuales (YYYY-Mxx), desde 2002
# Medidas: terminadas/iniciadas × total/libre/protegida (6 columnas)
#
# Estrategia: TRUNCATE + reload completo. El ISTAC revisa valores históricos
# en cada publicación, igual que en PTE.
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/vivienda_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     territorio_codigo               = col_character(),
                     periodo                         = col_character(),
                     tipo_periodo                    = col_character(),
                     year                            = col_integer(),
                     mes                             = col_integer(),
                     viviendas_terminadas            = col_integer(),
                     viviendas_terminadas_libres     = col_integer(),
                     viviendas_terminadas_protegidas = col_integer(),
                     viviendas_iniciadas             = col_integer(),
                     viviendas_iniciadas_libres      = col_integer(),
                     viviendas_iniciadas_protegidas  = col_integer()
                   ))

cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Territorios:", paste(sort(unique(df_raw$territorio_codigo)), collapse = " "), "\n")
cat("Años (anuales):", paste(sort(unique(df_raw$year[df_raw$tipo_periodo == "anual"])),
                              collapse = " "), "\n\n")

# --- 3. DIAGNÓSTICO ---
cat("Filas por tipo de período y territorio:\n")
print(df_raw %>% count(territorio_codigo, tipo_periodo) %>% arrange(territorio_codigo, tipo_periodo))

# --- 4. VALIDACIÓN: anuales ES70 recientes ---
cat("\nES70 anual — últimos 5 años (terminadas vs iniciadas):\n")
check <- df_raw %>%
  filter(territorio_codigo == "ES70", tipo_periodo == "anual") %>%
  arrange(desc(year)) %>% head(5) %>%
  select(year, viviendas_terminadas, viviendas_terminadas_libres,
         viviendas_terminadas_protegidas, viviendas_iniciadas)
print(check)

# Coherencia interna: total ≈ libre + protegida (cuando los tres tienen dato)
cat("\nVerificando coherencia total = libre + protegida...\n")
incoherencias <- df_raw %>%
  filter(!is.na(viviendas_terminadas),
         !is.na(viviendas_terminadas_libres),
         !is.na(viviendas_terminadas_protegidas)) %>%
  mutate(diff_term = viviendas_terminadas - viviendas_terminadas_libres - viviendas_terminadas_protegidas) %>%
  filter(abs(diff_term) > 0)

if (nrow(incoherencias) > 0) {
  cat("ADVERTENCIA — incoherencias terminadas:\n")
  print(incoherencias %>% select(territorio_codigo, periodo, viviendas_terminadas,
                                  viviendas_terminadas_libres, viviendas_terminadas_protegidas, diff_term))
} else {
  cat("OK: coherencia terminadas correcta.\n")
}

incoherencias_ini <- df_raw %>%
  filter(!is.na(viviendas_iniciadas),
         !is.na(viviendas_iniciadas_libres),
         !is.na(viviendas_iniciadas_protegidas)) %>%
  mutate(diff_ini = viviendas_iniciadas - viviendas_iniciadas_libres - viviendas_iniciadas_protegidas) %>%
  filter(abs(diff_ini) > 0)

if (nrow(incoherencias_ini) > 0) {
  cat("ADVERTENCIA — incoherencias iniciadas:\n")
  print(incoherencias_ini %>% select(territorio_codigo, periodo, viviendas_iniciadas,
                                      viviendas_iniciadas_libres, viviendas_iniciadas_protegidas, diff_ini))
} else {
  cat("OK: coherencia iniciadas correcta.\n")
}

# --- 5. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE vivienda_iniciada_terminada_canarias")
  dbWriteTable(con, "vivienda_iniciada_terminada_canarias", df_raw,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(df_raw), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 6. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT territorio_codigo, tipo_periodo,
          count(*) n,
          min(year) anyo_min, max(year) anyo_max
   FROM vivienda_iniciada_terminada_canarias
   GROUP BY territorio_codigo, tipo_periodo
   ORDER BY territorio_codigo, tipo_periodo"))

dbDisconnect(con)
cat("Proceso completado.\n")
