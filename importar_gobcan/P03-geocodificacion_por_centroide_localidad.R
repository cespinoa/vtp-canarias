#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P03-geocodificacion_por_centroide_localidad.R
# Fallback 1: asigna el centroide de la localidad a los registros que aún no
# tienen coordenadas pero tienen localidad_id. Estrategia: lotes de 1000.
#
# Uso:
#   Rscript importar_gobcan/P03-geocodificacion_por_centroide_localidad.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P03 — Geocodificación por centroide de localidad\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P03_INICIO", "Geocodificación por centroide de localidad iniciada")

# Candidatos: sin coordenadas, con localidad_id, no ya geocodificados
n_candidatos <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import
  WHERE latitud IS NULL
    AND localidad_id IS NOT NULL
    AND estado != 'bruto'")$n

cat("Candidatos con localidad_id:", n_candidatos, "\n\n")

BATCH_SIZE       <- 1000
total_asignados  <- 0
n_lote           <- 0

repeat {
  lote <- dbGetQuery(con, sprintf(
    "SELECT id FROM staging_import
     WHERE latitud IS NULL
       AND localidad_id IS NOT NULL
       AND estado != 'bruto'
     LIMIT %d", BATCH_SIZE))

  if (nrow(lote) == 0) break

  n_lote     <- n_lote + 1
  ids_string <- paste(lote$id, collapse = ",")
  procesados <- min((n_lote - 1) * BATCH_SIZE + nrow(lote), n_candidatos)
  cat(sprintf("Lote %d | procesados %d/%d | ", n_lote, procesados, n_candidatos))

  query_centroide <- paste0("
    WITH resultados AS (
      SELECT s.id,
             ST_X(c.geom) AS lon,
             ST_Y(c.geom) AS lat
      FROM staging_import s
      JOIN centroides_localidad c ON s.localidad_id = c.localidad_id
      WHERE s.id IN (", ids_string, ")
    )
    UPDATE staging_import s
    SET longitud         = r.lon,
        latitud          = r.lat,
        fuente_geocodigo = 'centroide:localidad',
        estado           = 'bruto',
        direccion_match  = 'CENTROIDE LOCALIDAD'
    FROM resultados r WHERE s.id = r.id
    RETURNING s.id;")

  rescatados_ids <- dbGetQuery(con, query_centroide)$id
  n_exitos       <- length(rescatados_ids)
  total_asignados <- total_asignados + n_exitos
  cat(sprintf("asignados: %d (acumulado: %d)\n", n_exitos, total_asignados))
  Sys.sleep(0.05)

  # Anti-bucle: los que tienen localidad_id pero no centroide en la tabla
  # pasan a geocod_cp_pendiente para que P04 los intente por CP.
  ids_fallidos <- setdiff(lote$id, rescatados_ids)
  if (length(ids_fallidos) > 0) {
    dbExecute(con, sprintf(
      "UPDATE staging_import SET estado = 'geocod_cp_pendiente'
       WHERE id IN (%s)", paste(ids_fallidos, collapse = ",")))
  }
}

# --- RESUMEN ---
tasa <- if (n_candidatos > 0) round(100 * total_asignados / n_candidatos, 1) else 0

cat("\n========================================\n")
cat("RESUMEN P03\n")
cat("  Candidatos     :", n_candidatos, "\n")
cat("  Asignados      :", total_asignados, "\n")
cat("  Tasa de éxito  :", tasa, "%\n")
cat("  Sin centroide  :", n_candidatos - total_asignados, "(pasan a P04)\n")
cat("========================================\n")

cat("\nFuentes actuales:\n")
print(dbGetQuery(con, "
  SELECT COALESCE(fuente_geocodigo, '(sin coordenadas)') AS fuente,
         COUNT(*)::int AS n
  FROM staging_import
  GROUP BY fuente ORDER BY n DESC"))

escribir_log("P03_FIN", paste0(
  "Asignados: ", total_asignados, "/", n_candidatos,
  " (", tasa, "%). Sin centroide: ", n_candidatos - total_asignados))

dbDisconnect(con)
cat("\n✓ P03 completado.\n")
