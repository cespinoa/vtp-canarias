#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_turistas.R
# Carga el total de turistas llegados por isla y mes (ISTAC E16028B_000011)
# desde el CSV descargado por istac_turistas.py.
#
# Uso:
#   Rscript descarga_datos/importar_turistas.R
#   Rscript descarga_datos/importar_turistas.R ruta/al/fichero.csv
#
# Cobertura: mensual 2010-M01 hasta el mes más reciente publicado.
# Islas: Fuerteventura (ES704), Gran Canaria (ES705), La Palma (ES707),
#        Lanzarote (ES708), Tenerife (ES709).
#        El Hierro y La Gomera no están en el dataset del ISTAC.
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
  candidatos <- Sys.glob("descarga_datos/tmp/turistas_????????.csv")
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
                     year              = col_integer(),
                     mes               = col_integer(),
                     turistas          = col_integer()
                   ))

periodos <- sort(unique(paste0(df_raw$year, "-M", sprintf("%02d", df_raw$mes))))
cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Períodos:", length(periodos), "—", head(periodos, 1), "→", tail(periodos, 1), "\n")
cat("Territorios:", paste(sort(unique(df_raw$territorio_codigo)), collapse = " "), "\n\n")

# --- 4. CRUZAR CON BD ---
sin_emparejar <- setdiff(df_raw$territorio_codigo, islas_db$geo_code)
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — territorios sin emparejar:", paste(sin_emparejar, collapse = " "), "\n")
} else {
  cat("OK: todos los territorios emparejados.\n")
}

tabla_final <- df_raw %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  transmute(isla_id = id, year, mes, turistas)

cat("Registros a cargar:", nrow(tabla_final), "\n\n")

# --- 5. VALIDACIÓN: totales por isla año reciente ---
cat("Turistas anuales por isla (últimos 3 años completos):\n")
resumen <- tabla_final %>%
  group_by(isla_id, year) %>%
  filter(n() == 12) %>%                        # solo años completos (12 meses)
  summarise(turistas_anual = sum(turistas), .groups = "drop") %>%
  left_join(islas_db %>% select(id, nombre), by = c("isla_id" = "id")) %>%
  filter(year >= max(year) - 2) %>%
  select(nombre, year, turistas_anual) %>%
  arrange(nombre, year)
print(resumen)

# --- 6. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE turistas_llegadas")
  dbWriteTable(con, "turistas_llegadas", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 7. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT i.nombre, count(*) meses,
          min(year*100+mes) periodo_min, max(year*100+mes) periodo_max,
          sum(turistas) turistas_total
   FROM turistas_llegadas t
   JOIN islas i ON t.isla_id = i.id
   GROUP BY i.nombre ORDER BY turistas_total DESC"))

dbDisconnect(con)
cat("Proceso completado.\n")
