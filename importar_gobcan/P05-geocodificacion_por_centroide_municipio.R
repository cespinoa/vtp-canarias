#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P05-geocodificacion_por_centroide_municipio.R
# Fallback 3 (último): asigna el centroide del municipio a los registros que
# aún no tienen coordenadas pero tienen municipio_id.
# Los que no tengan municipio_id quedan definitivamente sin posición.
#
# Uso:
#   Rscript importar_gobcan/P05-geocodificacion_por_centroide_municipio.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P05 — Geocodificación por centroide de municipio\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P05_INICIO", "Geocodificación por centroide de municipio iniciada")

n_candidatos <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import
  WHERE latitud IS NULL
    AND municipio_id IS NOT NULL")$n

n_sin_municipio <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import
  WHERE latitud IS NULL
    AND municipio_id IS NULL")$n

cat("Candidatos con municipio_id :", n_candidatos, "\n")
cat("Sin municipio (irrecuperables):", n_sin_municipio, "\n\n")

if (n_candidatos > 0) {
  rescatados <- dbGetQuery(con, "
    WITH candidatos AS (
      SELECT id, municipio_id
      FROM staging_import
      WHERE latitud IS NULL
        AND municipio_id IS NOT NULL
    ),
    resultados AS (
      SELECT can.id,
             ST_X(cm.geom) AS lon,
             ST_Y(cm.geom) AS lat
      FROM candidatos can
      INNER JOIN centroides_municipio cm ON can.municipio_id = cm.municipio_id
    )
    UPDATE staging_import s
    SET longitud         = r.lon,
        latitud          = r.lat,
        fuente_geocodigo = 'centroide:municipio',
        estado           = 'bruto',
        direccion_match  = 'CENTROIDE MUNICIPIO'
    FROM resultados r WHERE s.id = r.id
    RETURNING s.id;")

  n_asignados <- nrow(rescatados)
  tasa        <- round(100 * n_asignados / n_candidatos, 1)
  cat("Asignados      :", n_asignados, "\n")
  cat("Tasa de éxito  :", tasa, "%\n")

  escribir_log("P05_GEO", paste0(
    "Asignados: ", n_asignados, "/", n_candidatos, " (", tasa, "%)"))
}

# Registros definitivamente sin posición (sin municipio_id)
dbExecute(con, "
  UPDATE staging_import
  SET estado = 'sin_posicion'
  WHERE latitud IS NULL")

n_sin_pos <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import WHERE estado = 'sin_posicion'")$n
cat("Sin posición (definitivo):", n_sin_pos, "\n")

# --- RESUMEN GLOBAL ---
cat("\n========================================\n")
cat("RESUMEN GLOBAL DE GEOCODIFICACIÓN\n")
cat("========================================\n")
print(dbGetQuery(con, "
  SELECT COALESCE(fuente_geocodigo, 'sin_posicion') AS fuente,
         COUNT(*)::int AS n,
         ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
  FROM staging_import
  GROUP BY fuente ORDER BY n DESC"))

escribir_log("P05_FIN", paste0(
  "Sin posición definitiva: ", n_sin_pos, " registros"))

dbDisconnect(con)
cat("\n✓ P05 completado.\n")
