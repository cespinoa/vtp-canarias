#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P12-informe_de_auditoria.R
# Genera el informe final de calidad sobre la tabla alojamientos:
#   0. Gestión de duplicados
#   1. Totales y modalidades
#   2. Origen de la geocodificación
#   3. Precisión fuzzy (callejero)
#   4. Distribución por proximidad a núcleo
#   5. Integridad de plazas
#   6. Balance de correcciones municipales
#
# Uso:
#   Rscript importar_gobcan/P12-informe_de_auditoria.R
# ==============================================================================

library(DBI)
library(dplyr)
library(stringr)
source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P12 — Informe de auditoría final\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P12_INICIO", "Informe de auditoría iniciado")

# --- 0. DUPLICADOS ---
cat("### 0. Gestión de duplicados\n")
dups <- dbGetQuery(con, "
  SELECT
    COUNT(*) FILTER (WHERE audit_nota ~ 'duplicados_eliminados:[1-9]')::int
      AS establecimientos_con_duplicados,
    COALESCE(SUM(
      CAST(SUBSTRING(audit_nota FROM 'duplicados_eliminados:([0-9]+)') AS INTEGER)
    ), 0)::int AS total_descartados
  FROM alojamientos")
cat("Establecimientos con duplicidad en origen:", dups$establecimientos_con_duplicados, "\n")
cat("Registros descartados por duplicidad     :", dups$total_descartados, "\n\n")

# --- 1. TOTALES Y MODALIDADES ---
cat("### 1. Totales y modalidades\n")
res <- dbGetQuery(con, "
  SELECT
    COUNT(*)::int                                                          AS total,
    COUNT(*) FILTER (WHERE fecha_baja IS NULL)::int                       AS activos,
    COUNT(*) FILTER (WHERE modalidad_original = 'Hotelera')::int          AS hoteles,
    COUNT(*) FILTER (WHERE tipologia_original = 'Vivienda Vacacional')::int AS vv,
    COUNT(*) FILTER (WHERE audit_nota LIKE '%COORDS_FUERA_RANGO%')::int   AS geo_corregidas
  FROM alojamientos")

cat("Total establecimientos :", res$total,         "\n")
cat("Activos (sin baja)     :", res$activos,        "\n")
cat("Hoteleros              :", res$hoteles,         "\n")
cat("Viviendas vacacionales :", res$vv,              "\n")
cat("Extrahoteleros (excl VV):", res$total - res$hoteles - res$vv, "\n")
cat("Coords GobCan corregidas:", res$geo_corregidas, "\n\n")

# --- 2. ORIGEN DE LA GEOCODIFICACIÓN ---
cat("### 2. Origen de la geocodificación\n")
print(dbGetQuery(con, "
  SELECT
    COALESCE(fuente_geocodigo, 'sin_posicion') AS fuente,
    COUNT(*)::int AS registros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
  FROM alojamientos
  GROUP BY fuente ORDER BY registros DESC"))

# --- 3. PRECISIÓN FUZZY ---
cat("\n### 3. Precisión del callejero fuzzy\n")
fuzzy <- dbGetQuery(con, "
  SELECT
    ROUND(distancia_fuzzy::numeric, 1) AS similitud,
    COUNT(*)::int AS registros
  FROM alojamientos
  WHERE distancia_fuzzy IS NOT NULL
  GROUP BY similitud ORDER BY similitud DESC")
print(fuzzy)

cat("\nMuestra: direcciones con menor similitud (primeros 10):\n")
print(dbGetQuery(con, "
  SELECT muni_detectado_geo AS municipio,
         direccion_original AS fuente,
         direccion_match    AS match,
         ROUND(distancia_fuzzy::numeric, 2) AS similitud
  FROM alojamientos
  WHERE distancia_fuzzy IS NOT NULL AND direccion_match IS NOT NULL
  ORDER BY distancia_fuzzy ASC
  LIMIT 10"))

# --- 4. DISTRIBUCIÓN POR PROXIMIDAD AL NÚCLEO ---
cat("\n### 4. Proximidad al núcleo de población\n")
loc_data <- dbGetQuery(con, "SELECT metodo_localidad, audit_nota FROM alojamientos")

loc_resumen <- loc_data %>%
  mutate(dist_m = as.numeric(str_extract(audit_nota, "(?<=DIST_LOC: )\\d+"))) %>%
  mutate(rango = case_when(
    metodo_localidad == "directa" ~ "En núcleo (intersección directa)",
    is.na(dist_m)                 ~ "En núcleo (sin distancia)",
    dist_m <= 100                 ~ "Muy cerca (<100m)",
    dist_m <= 500                 ~ "Cerca (100-500m)",
    TRUE                          ~ "Alejado (>500m)"
  )) %>%
  count(metodo_localidad, rango, name = "registros") %>%
  arrange(desc(registros))

print(loc_resumen)

# --- 5. INTEGRIDAD DE PLAZAS ---
cat("\n### 5. Integridad de plazas\n")
print(dbGetQuery(con, "
  SELECT
    CASE WHEN plazas_estimadas THEN 'Estimadas' ELSE 'Oficiales' END AS origen,
    COUNT(*)::int   AS registros,
    SUM(plazas)::int AS total_plazas,
    ROUND(AVG(plazas), 1) AS promedio
  FROM alojamientos
  GROUP BY plazas_estimadas
  ORDER BY registros DESC"))

# --- 6. BALANCE DE CORRECCIONES MUNICIPALES ---
cat("\n### 6. Balance de correcciones municipales\n")
transfer_res <- dbGetQuery(con, "
  WITH transferencias AS (
    SELECT
      TRIM(UPPER(unaccent(muni_original_gobcan))) AS origen,
      TRIM(UPPER(unaccent(muni_detectado_geo)))   AS destino,
      COUNT(*)::int   AS n_est,
      SUM(plazas)::int AS s_plz
    FROM alojamientos
    WHERE TRIM(UPPER(unaccent(muni_original_gobcan))) <> TRIM(UPPER(unaccent(muni_detectado_geo)))
      AND muni_original_gobcan IS NOT NULL
      AND muni_detectado_geo   IS NOT NULL
    GROUP BY 1, 2
  ),
  universo AS (
    SELECT origen AS muni FROM transferencias
    UNION
    SELECT destino FROM transferencias
  ),
  ganadas  AS (SELECT destino AS muni, SUM(n_est) AS est_g, SUM(s_plz) AS plz_g FROM transferencias GROUP BY 1),
  perdidas AS (SELECT origen  AS muni, SUM(n_est) AS est_p, SUM(s_plz) AS plz_p FROM transferencias GROUP BY 1)
  SELECT
    u.muni                                              AS municipio,
    COALESCE(g.est_g, 0)                                AS recibidos,
    COALESCE(p.est_p, 0)                                AS cedidos,
    COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)        AS saldo_est,
    COALESCE(g.plz_g, 0) - COALESCE(p.plz_p, 0)        AS saldo_plazas
  FROM universo u
  LEFT JOIN ganadas  g ON u.muni = g.muni
  LEFT JOIN perdidas p ON u.muni = p.muni
  WHERE COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0) <> 0
  ORDER BY ABS(COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)) DESC")

if (nrow(transfer_res) > 0) {
  print(transfer_res)
  fecha_slug <- format(Sys.Date(), "%Y-%m-%d")
  csv_path   <- file.path("importar_gobcan/logs",
                           paste0("balance_municipal_", fecha_slug, ".csv"))
  write.csv(transfer_res, csv_path, row.names = FALSE)
  cat("\nBalance municipal exportado a:", csv_path, "\n")
  escribir_log("P12_BALANCE", paste(nrow(transfer_res), "municipios con correcciones. CSV:", csv_path))
} else {
  cat("Sin discrepancias municipales detectadas.\n")
}

escribir_log("P12_FIN", paste(
  "Total:", res$total,
  "| Activos:", res$activos,
  "| Geocod GobCan:", dbGetQuery(con,
    "SELECT COUNT(*)::int n FROM alojamientos WHERE fuente_geocodigo='gobcan'")$n))

dbDisconnect(con)
cat("\n✓ P12 completado.\n")
