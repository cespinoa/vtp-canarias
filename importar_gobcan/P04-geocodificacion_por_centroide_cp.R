#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P04-geocodificacion_por_centroide_cp.R
# Fallback 2: asigna el centroide del código postal (restringido al municipio)
# a los registros que aún no tienen coordenadas pero tienen CP y municipio_id.
#
# Uso:
#   Rscript importar_gobcan/P04-geocodificacion_por_centroide_cp.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P04 — Geocodificación por centroide de CP\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P04_INICIO", "Geocodificación por centroide de CP iniciada")

n_candidatos <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import
  WHERE latitud IS NULL
    AND cp IS NOT NULL
    AND municipio_id IS NOT NULL")$n

cat("Candidatos con CP y municipio:", n_candidatos, "\n\n")

if (n_candidatos > 0) {
  rescatados <- dbGetQuery(con, "
    WITH candidatos AS (
      SELECT id, cp, municipio_id
      FROM staging_import
      WHERE latitud IS NULL
        AND cp IS NOT NULL
        AND municipio_id IS NOT NULL
    ),
    matches AS (
      SELECT can.id,
             ST_X(c.geom) AS lon,
             ST_Y(c.geom) AS lat,
             c.cod_postal
      FROM candidatos can
      INNER JOIN centroides_cp c
        ON LPAD(TRIM(can.cp::text), 5, '0') = LPAD(TRIM(c.cod_postal::text), 5, '0')
       AND can.municipio_id = c.municipio_id
    )
    UPDATE staging_import s
    SET longitud         = m.lon,
        latitud          = m.lat,
        fuente_geocodigo = 'centroide:cp_municipio',
        estado           = 'bruto',
        direccion_match  = 'CENTROIDE CP: ' || m.cod_postal
    FROM matches m WHERE s.id = m.id
    RETURNING s.id;")

  n_asignados <- nrow(rescatados)
  tasa        <- round(100 * n_asignados / n_candidatos, 1)

  cat("Asignados      :", n_asignados, "\n")
  cat("Tasa de éxito  :", tasa, "%\n")
  cat("Sin centroide  :", n_candidatos - n_asignados, "(pasan a P05)\n")

  escribir_log("P04_FIN", paste0(
    "Asignados: ", n_asignados, "/", n_candidatos,
    " (", tasa, "%). Sin centroide: ", n_candidatos - n_asignados))
} else {
  cat("Sin candidatos. Nada que procesar.\n")
  escribir_log("P04_FIN", "Sin candidatos")
}

cat("\nFuentes actuales:\n")
print(dbGetQuery(con, "
  SELECT COALESCE(fuente_geocodigo, '(sin coordenadas)') AS fuente,
         COUNT(*)::int AS n
  FROM staging_import
  GROUP BY fuente ORDER BY n DESC"))

dbDisconnect(con)
cat("\n✓ P04 completado.\n")
