#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_frontur.R
# Carga el total de turistas FRONTUR llegados por territorio y mes
# (ISTAC E16028B_000016) desde el CSV descargado por frontur_canarias.py.
#
# Uso:
#   Rscript descarga_datos/importar_frontur.R
#   Rscript descarga_datos/importar_frontur.R ruta/al/fichero.csv
#
# Cobertura: mensual 2010-M01 hasta el mes más reciente publicado.
# Territorios: ES70 (Canarias), ES704 (Fuerteventura), ES705 (Gran Canaria),
#   ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife).
#   El Hierro y La Gomera no están en el dataset del ISTAC.
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
  candidatos <- c(Sys.glob("descarga_datos/tmp/frontur_????????.csv"),
                  Sys.glob("tmp/frontur_????????.csv"))
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

# --- 4. CONSTRUIR TABLA CON AMBITO ---
# ES70 → canarias (isla_id = NA); el resto → isla (cruzado con tabla islas)
canarias_raw <- df_raw %>%
  filter(territorio_codigo == "ES70") %>%
  transmute(ambito = "canarias", isla_id = NA_integer_, year, mes, turistas)

islas_raw <- df_raw %>%
  filter(territorio_codigo != "ES70")

sin_emparejar <- setdiff(islas_raw$territorio_codigo, islas_db$geo_code)
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — territorios sin emparejar:", paste(sin_emparejar, collapse = " "), "\n")
} else {
  cat("OK: todos los territorios de isla emparejados.\n")
}

islas_final <- islas_raw %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  transmute(ambito = "isla", isla_id = id, year, mes, turistas)

tabla_final <- bind_rows(canarias_raw, islas_final)
cat("Registros a cargar:", nrow(tabla_final), "\n\n")

# --- 5. VALIDACIÓN: totales anuales (canarias) ---
cat("FRONTUR Canarias — turistas anuales (últimos 3 años completos):\n")
tabla_final %>%
  filter(ambito == "canarias") %>%
  group_by(year) %>%
  filter(n() == 12) %>%
  summarise(turistas_anual = sum(turistas), .groups = "drop") %>%
  filter(year >= max(year) - 2) %>%
  print()

cat("\nFRONTUR por isla (último año completo):\n")
ultimo_completo <- tabla_final %>%
  filter(ambito == "isla") %>%
  group_by(isla_id, year) %>%
  filter(n() == 12) %>%
  summarise(turistas_anual = sum(turistas), .groups = "drop") %>%
  filter(year == max(year)) %>%
  left_join(islas_db %>% select(id, nombre), by = c("isla_id" = "id")) %>%
  arrange(desc(turistas_anual))
print(ultimo_completo)

# --- 6. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE frontur_turistas")
  dbWriteTable(con, "frontur_turistas", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 7. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT ambito, i.nombre, count(*) meses,
          min(year*100+mes) periodo_min, max(year*100+mes) periodo_max,
          sum(turistas) turistas_total
   FROM frontur_turistas f
   LEFT JOIN islas i ON f.isla_id = i.id
   GROUP BY ambito, i.nombre ORDER BY ambito, turistas_total DESC"))

dbDisconnect(con)
cat("Proceso completado.\n")
