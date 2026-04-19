#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_serpavi.R
# Carga los datos del SERPAVI (Sistema Estatal de Referencia del Precio del
# Alquiler de Vivienda) para los municipios canarios, desde el Excel publicado
# por el MIVAU.
#
# Fuente: https://cdn.mivau.gob.es/portal-web-mivau/vivienda/serpavi/
#         2026-03-09_bd_SERPAVI_2011-2024 - DEFINITIVO WEB.xlsx
#         (descarga manual; actualización anual)
#
# Uso:
#   Rscript descarga_datos/importar_serpavi.R
#   Rscript descarga_datos/importar_serpavi.R ruta/al/fichero.xlsx
#
# Campos almacenados por municipio y año:
#   n_contratos     Número de contratos de arrendamiento (TVC)
#   n_viviendas     Número de viviendas únicas en alquiler habitual (TVU)
#   alq_m2_media    Precio medio del alquiler €/m² (perspectiva VU)
#   alq_m2_p25      Percentil 25 del alquiler €/m²
#   alq_m2_p75      Percentil 75 del alquiler €/m²
#   alq_anual_media Renta total media anual en € (perspectiva VU)
#   superficie_media Superficie media de la vivienda en m² (perspectiva VU)
#
# Nota sobre VU vs VC:
#   VU (viviendas únicas): cada vivienda cuenta una vez aunque haya tenido
#   varios contratos en el año. Es la perspectiva para ratios de parque.
#   VC (viviendas/contratos): cuenta cada contrato. Se guarda como n_contratos.
#
# Nota sobre cobertura:
#   54 de 88 municipios canarios tienen datos (los 34 restantes quedan en NA
#   por secreto estadístico al tener menos de X contratos).
#
# Estrategia: TRUNCATE + reload completo (el MIVAU revisa datos retroactivos).
# ==============================================================================

library(tidyverse)
library(readxl)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR EXCEL ---
args <- commandArgs(trailingOnly = TRUE)
xlsx_path <- if (length(args) >= 1) {
  args[1]
} else {
  candidatos <- Sys.glob("descarga_datos/tmp/serpavi_*.xlsx")
  if (length(candidatos) == 0) {
    candidatos <- Sys.glob("descarga_datos/tmp/*SERPAVI*.xlsx")
  }
  if (length(candidatos) == 0) stop(paste(
    "No se encontró ningún Excel SERPAVI en descarga_datos/tmp/\n",
    "Descargarlo de:\n",
    "https://cdn.mivau.gob.es/portal-web-mivau/vivienda/serpavi/",
    "y guardarlo en descarga_datos/tmp/ con nombre serpavi_YYYYMMDD.xlsx"
  ))
  tail(sort(candidatos), 1)
}

cat("Fuente:", xlsx_path, "\n")

# --- 2. LEER HOJA MUNICIPIOS ---
df_raw <- read_excel(xlsx_path, sheet = "Municipios")
cat("Total municipios en Excel:", nrow(df_raw), "\n")

# --- 3. FILTRAR CANARIAS ---
df_can <- df_raw %>%
  filter(CPRO %in% c("35", "38")) %>%
  mutate(codigo_ine = paste0(CPRO, substr(CUMUN, 3, 5)))

cat("Municipios canarios en Excel:", nrow(df_can), "\n")

# --- 4. TABLAS MAESTRAS ---
municipios_db <- dbGetQuery(con,
  "SELECT id AS municipio_id, isla_id, codigo_ine FROM municipios WHERE codigo_ine IS NOT NULL"
)

df_can <- df_can %>%
  left_join(municipios_db, by = "codigo_ine")

sin_emparejar <- df_can %>% filter(is.na(municipio_id)) %>% pull(NMUN)
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — municipios sin emparejar:", paste(sin_emparejar, collapse = ", "), "\n")
} else {
  cat("OK: todos los municipios canarios emparejados.\n")
}

# --- 5. PIVOTAR DE ANCHO A LARGO ---
# Años disponibles: 2011–2024 (sufijos _11 a _24)
anyos <- 11:24

df_long <- map_dfr(anyos, function(yy) {
  sufijo <- sprintf("_%02d", yy)
  anyo   <- 2000 + yy

  col_tvc  <- paste0("BI_ALVHEPCO_TVC",    sufijo)
  col_tvu  <- paste0("BI_ALVHEPCO_TVU",    sufijo)
  col_m    <- paste0("ALQM2_LV_M_VU",      sufijo)
  col_p25  <- paste0("ALQM2_LV_25_VU",     sufijo)
  col_p75  <- paste0("ALQM2_LV_75_VU",     sufijo)
  col_anu  <- paste0("ALQTBID12_M_VU",     sufijo)
  col_sup  <- paste0("SLVM2_M_VU",         sufijo)

  df_can %>%
    select(
      municipio_id, isla_id,
      n_contratos     = all_of(col_tvc),
      n_viviendas     = all_of(col_tvu),
      alq_m2_media    = all_of(col_m),
      alq_m2_p25      = all_of(col_p25),
      alq_m2_p75      = all_of(col_p75),
      alq_anual_media = all_of(col_anu),
      superficie_media = all_of(col_sup)
    ) %>%
    mutate(anyo = anyo)
})

# Eliminar filas completamente vacías (municipios sin ningún dato ese año)
df_long <- df_long %>%
  filter(!is.na(municipio_id)) %>%
  filter(!(is.na(n_contratos) & is.na(n_viviendas) & is.na(alq_m2_media)))

cat("Registros a cargar:", nrow(df_long),
    "(", n_distinct(df_long$municipio_id), "municipios ×",
    n_distinct(df_long$anyo), "años)\n")

# --- 6. CREAR TABLA SI NO EXISTE ---
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS serpavi_alquiler (
    municipio_id     INTEGER NOT NULL REFERENCES municipios(id),
    isla_id          INTEGER REFERENCES islas(id),
    anyo             SMALLINT NOT NULL,
    n_contratos      INTEGER,
    n_viviendas      INTEGER,
    alq_m2_media     NUMERIC(6,2),
    alq_m2_p25       NUMERIC(6,2),
    alq_m2_p75       NUMERIC(6,2),
    alq_anual_media  NUMERIC(8,2),
    superficie_media NUMERIC(6,1),
    PRIMARY KEY (municipio_id, anyo)
  );
")

# --- 7. CARGAR ---
dbExecute(con, "TRUNCATE TABLE serpavi_alquiler")

dbWriteTable(con, "serpavi_alquiler", df_long,
             append = TRUE, row.names = FALSE)

# Verificación
resumen <- dbGetQuery(con, "
  SELECT COUNT(*)::integer                               AS total,
         COUNT(DISTINCT municipio_id)::integer           AS municipios,
         MIN(anyo)                                       AS anyo_min,
         MAX(anyo)                                       AS anyo_max,
         COUNT(*) FILTER (WHERE alq_m2_media IS NOT NULL)::integer AS con_precio
  FROM serpavi_alquiler
")

cat("\nCargados:", resumen$total, "registros |",
    resumen$municipios, "municipios |",
    "años", resumen$anyo_min, "–", resumen$anyo_max, "|",
    resumen$con_precio, "con precio €/m²\n")

cat("\nMuestra (municipios con dato 2024, ordenados por alq_m2_media desc):\n")
muestra <- dbGetQuery(con, "
  SELECT m.nombre, s.anyo, s.n_viviendas, s.alq_m2_media, s.alq_anual_media
  FROM serpavi_alquiler s
  JOIN municipios m ON m.id = s.municipio_id
  WHERE s.anyo = 2024 AND s.alq_m2_media IS NOT NULL
  ORDER BY s.alq_m2_media DESC
  LIMIT 10
")
print(muestra)

dbDisconnect(con)
cat("\nListo.\n")
