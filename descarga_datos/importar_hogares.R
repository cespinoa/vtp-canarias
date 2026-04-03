#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_hogares.R
# Carga los datos censales de hogares (ISTAC C00025A_000001)
# desde el CSV descargado por istac_hogares.py.
#
# Uso:
#   Rscript descarga_datos/importar_hogares.R
#   Rscript descarga_datos/importar_hogares.R ruta/al/fichero.csv
#
# Niveles cargados: canarias, isla, municipio
# Campos: hogares (total), miembros (tamaño medio)
# Cobertura: 22 ediciones censales 1768–2021 (solo las que tienen dato)
#
# Estrategia: TRUNCATE + reload completo. La tabla es pequeña y los datos
# censales no cambian (salvo revisiones en la publicación ISTAC).
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/hogares_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
islas_db      <- dbGetQuery(con, "SELECT id, geo_code FROM islas")
municipios_db <- dbGetQuery(con, "SELECT id, isla_id, codigo_ine FROM municipios WHERE codigo_ine IS NOT NULL")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     territorio_codigo    = col_character(),
                     periodo              = col_integer(),
                     hogares              = col_double(),
                     hogares_tamanio_medio = col_double()
                   ))

cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Periodos:", paste(sort(unique(df_raw$periodo)), collapse = " "), "\n\n")

# --- 4. CLASIFICAR POR ÁMBITO ---

# A. Canarias
hog_canarias <- df_raw %>%
  filter(territorio_codigo == "ES70") %>%
  transmute(
    ambito       = "canarias",
    isla_id      = NA_integer_,
    municipio_id = NA_integer_,
    hogares      = as.integer(hogares),
    miembros     = hogares_tamanio_medio,
    year         = as.Date(paste0(periodo, "-12-31"))
  )

# B. Islas
hog_islas <- df_raw %>%
  filter(territorio_codigo %in% islas_db$geo_code) %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  transmute(
    ambito       = "isla",
    isla_id      = id,
    municipio_id = NA_integer_,
    hogares      = as.integer(hogares),
    miembros     = hogares_tamanio_medio,
    year         = as.Date(paste0(periodo, "-12-31"))
  )

# C. Municipios
hog_municipios <- df_raw %>%
  filter(!territorio_codigo %in% c("ES70", islas_db$geo_code)) %>%
  inner_join(municipios_db, by = c("territorio_codigo" = "codigo_ine")) %>%
  transmute(
    ambito       = "municipio",
    isla_id,
    municipio_id = id,
    hogares      = as.integer(hogares),
    miembros     = hogares_tamanio_medio,
    year         = as.Date(paste0(periodo, "-12-31"))
  )

# Diagnóstico: códigos sin emparejar
sin_emparejar <- df_raw %>%
  filter(!territorio_codigo %in% c("ES70", islas_db$geo_code,
                                   municipios_db$codigo_ine)) %>%
  pull(territorio_codigo) %>% unique()
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — códigos sin emparejar:\n")
  print(sin_emparejar)
} else {
  cat("OK: todos los códigos emparejados.\n")
}

# --- 5. ENSAMBLAJE ---
tabla_final <- bind_rows(hog_canarias, hog_islas, hog_municipios) %>%
  filter(!is.na(hogares) | !is.na(miembros))

cat("\nRegistros a cargar por ámbito:\n")
print(tabla_final %>% count(ambito))
cat("Total:", nrow(tabla_final), "\n\n")

# --- 6. VALIDACIÓN: municipios suman isla en 2021 ---
cat("Validando integridad hogares 2021 (municipios vs isla)...\n")
suma_mun <- tabla_final %>%
  filter(ambito == "municipio", year == as.Date("2021-12-31")) %>%
  group_by(isla_id) %>%
  summarise(suma_mun = sum(hogares, na.rm = TRUE), .groups = "drop")

isla_val <- tabla_final %>%
  filter(ambito == "isla", year == as.Date("2021-12-31")) %>%
  select(isla_id, isla = hogares)

descuadres <- inner_join(isla_val, suma_mun, by = "isla_id") %>%
  mutate(diff = isla - suma_mun) %>%
  filter(abs(diff) > 0)

if (nrow(descuadres) > 0) {
  cat("ADVERTENCIA — descuadres isla/municipio:\n")
  islas_nom <- dbGetQuery(con, "SELECT id, nombre FROM islas")
  print(descuadres %>% left_join(islas_nom, by = c("isla_id" = "id")))
} else {
  cat("OK: integridad isla/municipio correcta en 2021.\n")
}

# --- 7. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE hogares")
  dbWriteTable(con, "hogares", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 8. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT ambito, count(*) n, min(year) anyo_min, max(year) anyo_max
   FROM hogares GROUP BY ambito ORDER BY ambito"))

dbDisconnect(con)
cat("Proceso completado.\n")
