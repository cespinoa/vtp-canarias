#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P08-asignacion_municipio_localidad_por_geo.R
# Asigna localidad_id y municipio_id definitivos usando la posición geográfica:
#   1. Asignación directa: ST_Intersects con polígonos de localidades.
#   2. Rescate por proximidad: localidad más cercana dentro del municipio
#      para puntos que caen en huecos entre polígonos de localidades.
#
# Uso:
#   Rscript importar_gobcan/P08-asignacionde_municipio_localidad_por_geo.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P08 — Asignación municipio/localidad por geo\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P08_INICIO", "Asignación geográfica de municipio y localidad iniciada")

dbExecute(con, "SET work_mem = '128MB';")
dbExecute(con, "SET synchronous_commit = OFF;")

# --- 1. ASIGNACIÓN DIRECTA por intersección con polígonos de localidades ---
cat("Paso 1: asignación directa (ST_Intersects)... ")
n_directa <- dbExecute(con, "
  WITH cruce AS (
    SELECT DISTINCT ON (s.id)
      s.id,
      l.id          AS loc_id,
      l.municipio_id AS muni_id
    FROM staging_import s
    JOIN localidades l ON ST_Intersects(s.geom, l.geom)
    WHERE s.geom IS NOT NULL
      AND s.estado != 'finalizado_geo'
  )
  UPDATE staging_import s
  SET localidad_id     = c.loc_id,
      municipio_id     = c.muni_id,
      metodo_localidad = 'directa',
      estado           = 'finalizado_geo'
  FROM cruce c WHERE s.id = c.id;")
cat(n_directa, "registros asignados.\n")

# --- 2. RESCATE POR PROXIMIDAD ---
# Puntos que caen en huecos entre polígonos de localidades.
# Se busca la localidad más cercana dentro del municipio ya asignado.
huerfanos <- dbGetQuery(con, "
  SELECT id FROM staging_import
  WHERE geom IS NOT NULL
    AND estado != 'finalizado_geo'")

if (nrow(huerfanos) > 0) {
  cat("Paso 2: proximidad para", nrow(huerfanos), "huérfanos...\n")

  BATCH_SIZE <- 500
  ids        <- huerfanos$id
  num_lotes  <- ceiling(length(ids) / BATCH_SIZE)

  for (i in seq_len(num_lotes)) {
    lote_ids   <- ids[((i - 1) * BATCH_SIZE + 1):min(i * BATCH_SIZE, length(ids))]
    ids_string <- paste(lote_ids, collapse = ",")

    dbExecute(con, sprintf("
      WITH buscador AS (
        SELECT DISTINCT ON (s.id)
          s.id,
          l.id AS loc_id,
          ST_Distance(s.geom::geography, l.geom::geography) AS dist
        FROM staging_import s
        CROSS JOIN LATERAL (
          SELECT id, geom
          FROM localidades
          WHERE municipio_id = s.municipio_id
          ORDER BY s.geom <-> geom
          LIMIT 1
        ) l
        WHERE s.id IN (%s)
      )
      UPDATE staging_import s
      SET localidad_id     = b.loc_id,
          metodo_localidad = 'proximidad',
          audit_nota       = COALESCE(audit_nota, '') ||
                             ' | DIST_LOC: ' || ROUND(b.dist::numeric, 0) || 'm',
          estado           = 'finalizado_geo'
      FROM buscador b WHERE s.id = b.id;", ids_string))

    cat(sprintf("  Lote %d/%d completado.\n", i, num_lotes))
    Sys.sleep(0.05)
  }
  cat("✓ Rescate por proximidad finalizado.\n")
} else {
  cat("Paso 2: sin huérfanos — todos asignados en paso 1.\n")
}

dbExecute(con, "ANALYZE staging_import;")

# --- 3. AUDITORÍA FINAL ---
cat("\n========================================\n")
cat("RESUMEN P08\n")
cat("========================================\n")

print(dbGetQuery(con, "
  SELECT estado, COUNT(*)::int AS n
  FROM staging_import
  GROUP BY estado ORDER BY n DESC;"))

cat("\nMétodo de asignación de localidad:\n")
print(dbGetQuery(con, "
  SELECT COALESCE(metodo_localidad, '(sin asignar)') AS metodo,
         COUNT(*)::int AS n
  FROM staging_import
  GROUP BY metodo ORDER BY n DESC;"))

n_sin_localidad <- dbGetQuery(con,
  "SELECT COUNT(*)::int AS n FROM staging_import
   WHERE localidad_id IS NULL AND estado = 'finalizado_geo'")$n
if (n_sin_localidad > 0)
  cat("\nAVISO:", n_sin_localidad,
      "registros finalizado_geo sin localidad_id (municipio sin localidades en la tabla).\n")

escribir_log("P08_FIN", paste0(
  "Directa: ", n_directa,
  ". Proximidad: ", nrow(huerfanos),
  ". Sin posicion: 157"))

dbDisconnect(con)
cat("\n✓ P08 completado.\n")
