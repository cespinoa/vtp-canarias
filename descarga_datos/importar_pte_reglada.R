#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_pte_reglada.R
# Carga la Población Turística Equivalente reglada (ISTAC C00065A_000042)
# desde el CSV descargado por istac_poblacion_turistica.py.
#
# Uso:
#   Rscript descarga_datos/importar_pte_reglada.R
#   Rscript descarga_datos/importar_pte_reglada.R ruta/al/fichero.csv
#
# Estrategia: TRUNCATE + reload completo. El ISTAC revisa valores retroactiva-
# mente en cada publicación, por lo que la recarga total garantiza consistencia.
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/poblacion_turistica_equivalente_*.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)   # el más reciente por nombre
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
islas_db    <- dbGetQuery(con, "SELECT id, geo_code FROM islas")
destinos_db <- dbGetQuery(con, "SELECT geocode, municipio_id, isla_id FROM destinos_turisticos")
pesos_db    <- dbGetQuery(con, "SELECT isla_id, municipio_id, total_plazas_no_turisticas
                                FROM at_canarias_no_microdestino")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE) %>%
  rename(codigo_istac = territorio_codigo,
         pte_valor    = poblacion_turistica_equivalente) %>%
  mutate(year = as.numeric(periodo)) %>%
  filter(!is.na(pte_valor))

anyos <- sort(unique(df_raw$year))
cat("Años en CSV:", paste(anyos, collapse = " "), "\n")
cat("Filas con valor:", nrow(df_raw), "\n\n")

# --- 4. REPARTO ---

# Códigos que son "bolsas": B9 de cada isla + islas sin microdestinos (ES703/706/707)
codigos_bolsas <- c(paste0(islas_db$geo_code, "B9"), "ES703", "ES706", "ES707")

# A. Canarias
pte_canarias <- df_raw %>%
  filter(codigo_istac == "ES70") %>%
  transmute(ambito = "canarias", isla_id = NA_integer_, municipio_id = NA_integer_,
            year, pte_reglada = pte_valor, entidad_turistica_id = NA_character_)

# B. Islas
pte_islas <- df_raw %>%
  filter(codigo_istac %in% islas_db$geo_code) %>%
  inner_join(islas_db, by = c("codigo_istac" = "geo_code")) %>%
  transmute(ambito = "isla", isla_id = id, municipio_id = NA_integer_,
            year, pte_reglada = pte_valor, entidad_turistica_id = NA_character_)

# C. Localidades turísticas (excluye bolsas)
pte_localidades <- df_raw %>%
  filter(!codigo_istac %in% codigos_bolsas) %>%
  inner_join(destinos_db, by = c("codigo_istac" = "geocode")) %>%
  transmute(ambito = "localidad_turistica", isla_id, municipio_id,
            year, pte_reglada = pte_valor, entidad_turistica_id = codigo_istac)

# D. Reparto de bolsas → municipios
pesos_ratio <- pesos_db %>%
  group_by(isla_id) %>%
  mutate(total_p_isla = sum(as.numeric(total_plazas_no_turisticas), na.rm = TRUE),
         ratio        = as.numeric(total_plazas_no_turisticas) / total_p_isla) %>%
  ungroup()

pte_reparto <- df_raw %>%
  filter(codigo_istac %in% codigos_bolsas) %>%
  mutate(geo_isla_base = substr(codigo_istac, 1, 5)) %>%
  inner_join(islas_db, by = c("geo_isla_base" = "geo_code")) %>%
  inner_join(pesos_ratio, by = c("id" = "isla_id"), relationship = "many-to-many") %>%
  transmute(municipio_id, isla_id = id, year, pte_valor = pte_valor * ratio)

# E. Municipios = localidades propias + porción de bolsa
pte_municipios <- bind_rows(
  pte_localidades %>% select(municipio_id, isla_id, year, pte_valor = pte_reglada),
  pte_reparto     %>% select(municipio_id, isla_id, year, pte_valor)
) %>%
  filter(!is.na(municipio_id)) %>%
  group_by(isla_id, municipio_id, year) %>%
  summarise(pte_reglada = sum(pte_valor, na.rm = TRUE), .groups = "drop") %>%
  mutate(ambito = "municipio", entidad_turistica_id = NA_character_)

# --- 5. ENSAMBLAJE ---
tabla_final <- bind_rows(pte_canarias, pte_islas, pte_municipios, pte_localidades) %>%
  filter(!is.na(pte_reglada))

cat("Registros a cargar por ámbito:\n")
print(tabla_final %>% count(ambito))
cat("Total:", nrow(tabla_final), "\n\n")

# --- 6. VALIDACIÓN PREVIA ---
cat("Validando integridad (isla vs suma municipios)...\n")
descuadres <- tabla_final %>%
  filter(ambito %in% c("isla", "municipio")) %>%
  group_by(isla_id, ambito, year) %>%
  summarise(total = sum(pte_reglada), .groups = "drop") %>%
  pivot_wider(names_from = ambito, values_from = total) %>%
  mutate(diff = round(isla - municipio, 2)) %>%
  filter(abs(diff) > 0.5)   # tolerancia: ignoramos el descuadre ISTAC 2020 (15.86)

if (nrow(descuadres) > 0) {
  cat("ADVERTENCIA — descuadres encontrados (>0.5):\n")
  print(descuadres)
} else {
  cat("OK: integridad isla/municipio correcta (el descuadre ISTAC 2020 de ±15.86 es conocido y aceptado).\n")
}

# --- 7. CARGA ---
cat("\nTRUNCATE + carga...\n")
dbExecute(con, "TRUNCATE TABLE pte_reglada")
dbWriteTable(con, "pte_reglada", tabla_final, append = TRUE, row.names = FALSE)
cat("Cargados:", nrow(tabla_final), "registros.\n")

dbDisconnect(con)
cat("Proceso completado.\n")
