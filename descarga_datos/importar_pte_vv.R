#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_pte_vv.R
# Carga la Población Turística Equivalente Vacacional (PTEv) desde el CSV
# descargado por istac_pte_vv.py (ISTAC C00065A_000061).
#
# Uso:
#   Rscript descarga_datos/importar_pte_vv.R
#   Rscript descarga_datos/importar_pte_vv.R ruta/al/fichero.csv
#
# Niveles cargados: canarias, isla, municipio
# Cobertura: mensual desde 2019-M01
#
# Metodología PTEv (Turismo de Islas Canarias):
#   noches_vv = plazas_disponibles × (tasa_vivienda_reservada / 100) × dias_mes
#   ptev      = noches_vv / dias_mes
#             = plazas_disponibles × (tasa_vivienda_reservada / 100)
#   PTEv es la media diaria de plazas VV ocupadas en el mes.
#
# Estrategia: TRUNCATE + reload completo. El ISTAC revisa valores históricos
# retroactivamente en cada publicación (igual que PTE reglada).
# ==============================================================================

library(tidyverse)
library(lubridate)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/pte_vv_????????.csv")
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
                     ambito               = col_character(),
                     time_period          = col_character(),
                     year                 = col_integer(),
                     mes                  = col_integer(),
                     plazas_disponibles   = col_double(),
                     viviendas_disponibles = col_double(),
                     viviendas_reservadas  = col_double(),
                     tasa_vivienda_reservada = col_double(),
                     estancia_media       = col_double(),
                     ingresos_totales     = col_double()
                   ))

periodos <- sort(unique(df_raw$time_period))
cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Períodos:", length(periodos), "—", head(periodos, 1), "→", tail(periodos, 1), "\n\n")

# --- 4. CRUZAR IDs DE BD ---

# A. Canarias
vv_canarias <- df_raw %>%
  filter(ambito == "canarias") %>%
  transmute(
    ambito, isla_id = NA_integer_, municipio_id = NA_integer_,
    year, mes,
    plazas_disponibles   = as.integer(plazas_disponibles),
    viviendas_disponibles = as.integer(viviendas_disponibles),
    viviendas_reservadas  = as.integer(viviendas_reservadas),
    tasa_vivienda_reservada, estancia_media, ingresos_totales
  )

# B. Islas
vv_islas <- df_raw %>%
  filter(ambito == "isla") %>%
  inner_join(islas_db, by = c("territorio_codigo" = "geo_code")) %>%
  transmute(
    ambito, isla_id = id, municipio_id = NA_integer_,
    year, mes,
    plazas_disponibles   = as.integer(plazas_disponibles),
    viviendas_disponibles = as.integer(viviendas_disponibles),
    viviendas_reservadas  = as.integer(viviendas_reservadas),
    tasa_vivienda_reservada, estancia_media, ingresos_totales
  )

# C. Municipios
vv_municipios <- df_raw %>%
  filter(ambito == "municipio") %>%
  inner_join(municipios_db, by = c("territorio_codigo" = "codigo_ine")) %>%
  transmute(
    ambito, isla_id, municipio_id = id,
    year, mes,
    plazas_disponibles   = as.integer(plazas_disponibles),
    viviendas_disponibles = as.integer(viviendas_disponibles),
    viviendas_reservadas  = as.integer(viviendas_reservadas),
    tasa_vivienda_reservada, estancia_media, ingresos_totales
  )

# Diagnóstico: territorios sin emparejar
sin_isla <- df_raw %>%
  filter(ambito == "isla", !territorio_codigo %in% islas_db$geo_code) %>%
  pull(territorio_codigo) %>% unique()

sin_muni <- df_raw %>%
  filter(ambito == "municipio", !territorio_codigo %in% municipios_db$codigo_ine) %>%
  pull(territorio_codigo) %>% unique()

if (length(sin_isla) > 0) cat("ADVERTENCIA — islas sin emparejar:", paste(sin_isla, collapse = " "), "\n")
if (length(sin_muni) > 0) cat("ADVERTENCIA — municipios sin emparejar:", paste(sin_muni, collapse = " "), "\n")
if (length(sin_isla) == 0 && length(sin_muni) == 0) cat("OK: todos los territorios emparejados.\n")

# --- 5. CÁLCULO DE PTEv ---
# noches_vv = plazas_disponibles × (tasa / 100) × dias_mes
# ptev      = noches_vv / dias_mes = plazas_disponibles × (tasa / 100)

tabla_base <- bind_rows(vv_canarias, vv_islas, vv_municipios) %>%
  filter(!is.na(plazas_disponibles), !is.na(tasa_vivienda_reservada)) %>%
  mutate(
    dias_mes  = days_in_month(make_date(year, mes, 1L)),
    noches_vv = plazas_disponibles * (tasa_vivienda_reservada / 100) * dias_mes,
    ptev      = noches_vv / dias_mes
  ) %>%
  select(ambito, isla_id, municipio_id, year, mes, dias_mes,
         viviendas_disponibles, plazas_disponibles, viviendas_reservadas,
         tasa_vivienda_reservada, estancia_media, ingresos_totales,
         noches_vv, ptev)

cat("\nRegistros a cargar por ámbito:\n")
print(tabla_base %>% count(ambito))
cat("Total:", nrow(tabla_base), "\n\n")

# --- 6. VALIDACIÓN: municipios vs isla en el último mes completo ---
ultimo_mes <- tabla_base %>%
  filter(ambito == "municipio") %>%
  slice_max(order_by = year * 100 + mes, n = 1) %>%
  distinct(year, mes)

cat(sprintf("Validando sumas municipio vs isla (%d-M%02d)...\n",
            ultimo_mes$year, ultimo_mes$mes))

suma_mun <- tabla_base %>%
  filter(ambito == "municipio",
         year == ultimo_mes$year, mes == ultimo_mes$mes) %>%
  group_by(isla_id) %>%
  summarise(suma_plazas_mun = sum(plazas_disponibles, na.rm = TRUE), .groups = "drop")

isla_val <- tabla_base %>%
  filter(ambito == "isla",
         year == ultimo_mes$year, mes == ultimo_mes$mes) %>%
  select(isla_id, plazas_isla = plazas_disponibles)

descuadres <- inner_join(isla_val, suma_mun, by = "isla_id") %>%
  mutate(diff = plazas_isla - suma_plazas_mun,
         pct  = round(100 * diff / plazas_isla, 2)) %>%
  filter(abs(diff) > 0)

if (nrow(descuadres) > 0) {
  islas_nom <- dbGetQuery(con, "SELECT id, nombre FROM islas")
  cat("ADVERTENCIA — descuadres isla/municipio (plazas):\n")
  print(descuadres %>% left_join(islas_nom, by = c("isla_id" = "id")) %>%
          select(nombre, plazas_isla, suma_plazas_mun, diff, pct))
} else {
  cat("OK: sumas municipio = isla.\n")
}

# --- 7. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE pte_vacacional")
  dbWriteTable(con, "pte_vacacional", tabla_base, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_base), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 8. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT ambito, count(*) n,
          min(year*100+mes) periodo_min, max(year*100+mes) periodo_max,
          round(avg(ptev)::numeric, 1) ptev_medio
   FROM pte_vacacional
   GROUP BY ambito ORDER BY ambito"))

dbDisconnect(con)
cat("Proceso completado.\n")
