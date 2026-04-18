#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_egt_estancia.R
# Carga la estancia media EGT (NOCHES_PERNOCTADAS / TURISTAS) por isla y año
# desde el CSV descargado por istac_egt_estancia.py.
#
# Fuente: ISTAC C00028A (Encuesta sobre el Gasto Turístico)
#   C00028A_000003 → TURISTAS por isla, anual 2010–presente
#   C00028A_000004 → NOCHES_PERNOCTADAS por isla, anual 2010–presente
#
# Territorios: ES70 (Canarias), ES704 (Fuerteventura), ES705 (Gran Canaria),
#   ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife).
#
# Uso:
#   Rscript descarga_datos/importar_egt_estancia.R
#   Rscript descarga_datos/importar_egt_estancia.R ruta/al/fichero.csv
#
# Estrategia: TRUNCATE + reload completo (el ISTAC revisa datos retroactivos).
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- c(Sys.glob("descarga_datos/tmp/egt_estancia_????????.csv"),
                  Sys.glob("tmp/egt_estancia_????????.csv"))
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
islas_db <- dbGetQuery(con, "SELECT id, geo_code, nombre FROM islas")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     geo_code       = col_character(),
                     year           = col_integer(),
                     estancia_media = col_double()
                   ))

cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Años:", min(df_raw$year), "–", max(df_raw$year), "\n")
cat("Territorios:", paste(sort(unique(df_raw$geo_code)), collapse = " "), "\n\n")

# --- 4. CONSTRUIR TABLA CON AMBITO ---
canarias_df <- df_raw %>%
  filter(geo_code == "ES70") %>%
  transmute(ambito = "canarias", isla_id = NA_integer_, year,
            estancia_media = round(estancia_media, 2))

sin_emparejar <- setdiff(df_raw$geo_code[df_raw$geo_code != "ES70"], islas_db$geo_code)
if (length(sin_emparejar) > 0)
  cat("ADVERTENCIA — geo_codes sin emparejar:", paste(sin_emparejar, collapse = " "), "\n")

islas_df <- df_raw %>%
  filter(geo_code != "ES70") %>%
  inner_join(islas_db, by = "geo_code") %>%
  transmute(ambito = "isla", isla_id = id, year,
            estancia_media = round(estancia_media, 2))

tabla_final <- bind_rows(canarias_df, islas_df)
cat("Registros a cargar:", nrow(tabla_final), "\n\n")

# --- 5. VALIDACIÓN ---
cat("Últimos 3 años — Canarias y comparación con islas:\n")
resumen <- tabla_final %>%
  filter(year >= max(year) - 2) %>%
  left_join(islas_db %>% select(id, nombre), by = c("isla_id" = "id")) %>%
  mutate(territorio = if_else(ambito == "canarias", "Canarias", nombre)) %>%
  select(year, territorio, estancia_media) %>%
  arrange(year, territorio)
print(resumen)

# --- 6. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")
dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE egt_estancia_media")
  dbWriteTable(con, "egt_estancia_media", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 7. RESUMEN FINAL ---
cat("\nSerie Canarias en BD:\n")
print(dbGetQuery(con,
  "SELECT year, estancia_media FROM egt_estancia_media
   WHERE ambito = 'canarias' ORDER BY year"))

dbDisconnect(con)
cat("Proceso completado.\n")
