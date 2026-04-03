#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_poblacion.R
# Carga la Población de Derecho (ISTAC C00025A_000002)
# desde el CSV descargado por istac_poblacion.py.
#
# Uso:
#   Rscript descarga_datos/importar_poblacion.R
#   Rscript descarga_datos/importar_poblacion.R ruta/al/fichero.csv
#
# Estrategia: TRUNCATE + reload completo. El ON CONFLICT no funciona con
# NULL en las columnas de la clave única (canarias/isla tienen isla_id o
# municipio_id NULL), por lo que se hace recarga total. El dataset es
# pequeño (≈3.500 filas) y los años históricos rara vez cambian.
#
# Nota Frontera (El Hierro):
#   El ISTAC usa 38013_1912 (años hasta 2007) y 38013_2007 (años desde 2008)
#   para el municipio de Frontera, que en nuestra BD tiene codigo_ine=38013.
#   Ambos códigos se mapean a 38013; no se solapan.
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/poblacion_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)   # el más reciente por nombre
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
islas_db      <- dbGetQuery(con, "SELECT id, geo_code FROM islas")
municipios_db <- dbGetQuery(con, "SELECT id, isla_id, codigo_ine FROM municipios WHERE codigo_ine IS NOT NULL")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE) %>%
  mutate(
    year = as.numeric(periodo),
    # Mapear códigos históricos de Frontera (El Hierro) a codigo_ine estándar
    territorio_codigo = recode(territorio_codigo,
      "38013_1912" = "38013",
      "38013_2007" = "38013"
    )
  ) %>%
  filter(!is.na(poblacion))

anyos <- sort(unique(df_raw$year))
cat("Años en CSV:", paste(anyos, collapse = " "), "\n")
cat("Filas con valor:", nrow(df_raw), "\n\n")

# --- 4. CLASIFICAR POR ÁMBITO ---

# A. Canarias
pob_canarias <- df_raw %>%
  filter(territorio_codigo == "ES70") %>%
  transmute(
    ambito       = "canarias",
    isla_id      = NA_integer_,
    municipio_id = NA_integer_,
    year,
    valor        = poblacion,
    fuente       = "ISTAC C00025A_000002"
  )

# B. Islas
pob_islas <- df_raw %>%
  filter(territorio_codigo %in% islas_db$geo_code) %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  transmute(
    ambito       = "isla",
    isla_id      = id,
    municipio_id = NA_integer_,
    year,
    valor        = poblacion,
    fuente       = "ISTAC C00025A_000002"
  )

# C. Municipios (códigos INE de 5 dígitos)
pob_municipios <- df_raw %>%
  filter(!territorio_codigo %in% c("ES70", islas_db$geo_code)) %>%
  inner_join(municipios_db, by = c("territorio_codigo" = "codigo_ine")) %>%
  transmute(
    ambito       = "municipio",
    isla_id,
    municipio_id = id,
    year,
    valor        = poblacion,
    fuente       = "ISTAC C00025A_000002"
  )

# Diagnóstico: códigos no emparejados
codigos_mun_csv <- df_raw %>%
  filter(!territorio_codigo %in% c("ES70", islas_db$geo_code)) %>%
  pull(territorio_codigo) %>% unique()

sin_emparejar <- setdiff(codigos_mun_csv, municipios_db$codigo_ine)
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — códigos sin emparejar:\n")
  print(sin_emparejar)
} else {
  cat("OK: todos los códigos de municipio emparejados.\n")
}

# --- 5. ENSAMBLAJE ---
tabla_final <- bind_rows(pob_canarias, pob_islas, pob_municipios) %>%
  filter(!is.na(valor))

cat("\nRegistros a cargar por ámbito:\n")
print(tabla_final %>% count(ambito))
cat("Total:", nrow(tabla_final), "\n\n")

# --- 6. VALIDACIÓN: municipios suman isla, islas suman Canarias ---
cat("Validando integridad (municipios vs isla vs Canarias)...\n")

# Isla vs suma municipios
descuadres_isla <- tabla_final %>%
  filter(ambito %in% c("isla", "municipio")) %>%
  group_by(isla_id, ambito, year) %>%
  summarise(total = sum(valor), .groups = "drop") %>%
  pivot_wider(names_from = ambito, values_from = total) %>%
  mutate(diff = round(isla - municipio, 0)) %>%
  filter(abs(diff) > 5)   # tolerancia: diferencias menores a 5 habitantes

if (nrow(descuadres_isla) > 0) {
  cat("ADVERTENCIA — descuadres isla/municipio (>5 hab):\n")
  print(descuadres_isla)
} else {
  cat("OK: integridad isla/municipio correcta.\n")
}

# Canarias vs suma islas
sum_islas <- tabla_final %>%
  filter(ambito == "isla") %>%
  group_by(year) %>%
  summarise(suma_islas = sum(valor), .groups = "drop")

sum_canarias <- tabla_final %>%
  filter(ambito == "canarias") %>%
  select(year, canarias = valor)

descuadres_canarias <- sum_islas %>%
  inner_join(sum_canarias, by = "year") %>%
  mutate(diff = round(canarias - suma_islas, 0)) %>%
  filter(abs(diff) > 5)

if (nrow(descuadres_canarias) > 0) {
  cat("ADVERTENCIA — descuadres Canarias/islas (>5 hab):\n")
  print(descuadres_canarias)
} else {
  cat("OK: integridad Canarias/islas correcta.\n")
}

# --- 7. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE poblacion")
  dbWriteTable(con, "poblacion", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

dbDisconnect(con)
cat("Proceso completado.\n")
