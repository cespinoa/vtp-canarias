#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P02-geocodificacion_filtrada_callejero.R
# Geocodifica por similitud fuzzy contra el callejero de portales, restringiendo
# el match al mismo municipio y código postal del registro (filtro territorial).
# Umbral de similitud: > 0.45. Estrategia: lotes de 500.
#
# Uso:
#   Rscript importar_gobcan/P02-geocodificacion_filtrada_callejero.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P02 — Geocodificación por callejero\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P02_INICIO", "Geocodificación por callejero iniciada")

# Ajustes de rendimiento
dbExecute(con, "SET work_mem = '64MB';")
dbExecute(con, "SET synchronous_commit = OFF;")

# --- 1. RESET SELECTIVO ---
# Devuelve a geocodificacion_pendiente todo lo que no provenga de gobcan,
# para que este script y los siguientes puedan actuar sobre ello.
n_reset <- dbExecute(con, "
  UPDATE staging_import
  SET estado           = 'geocodificacion_pendiente',
      fuente_geocodigo = NULL,
      latitud          = NULL,
      longitud         = NULL,
      direccion_match  = NULL,
      distancia_fuzzy  = NULL
  WHERE fuente_geocodigo IS NULL
     OR fuente_geocodigo != 'gobcan';")

cat("Reset selectivo:", n_reset, "registros devueltos a geocodificacion_pendiente.\n")
cat("(Los registros con coordenadas del GobCan se conservan intactos.)\n\n")
escribir_log("P02_RESET", paste(n_reset, "registros en geocodificacion_pendiente"))

# Candidatos disponibles (cp + municipio_id informados)
n_candidatos <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import
  WHERE estado = 'geocodificacion_pendiente'
    AND cp IS NOT NULL
    AND municipio_id IS NOT NULL")$n

cat("Candidatos para callejero (con CP y municipio):", n_candidatos, "\n\n")

# --- 2. GEOCODIFICACIÓN POR LOTES ---
BATCH_SIZE       <- 500
total_rescatados <- 0
n_lote           <- 0

repeat {
  lote_ids_df <- dbGetQuery(con, sprintf(
    "SELECT id, municipio_id FROM staging_import
     WHERE estado = 'geocodificacion_pendiente'
       AND cp IS NOT NULL
       AND municipio_id IS NOT NULL
     LIMIT %d", BATCH_SIZE))

  if (nrow(lote_ids_df) == 0) break

  n_lote      <- n_lote + 1
  ids_string  <- paste(lote_ids_df$id, collapse = ",")
  procesados  <- (n_lote - 1) * BATCH_SIZE + nrow(lote_ids_df)
  cat(sprintf("Lote %d | procesados %d/%d | ", n_lote, procesados, n_candidatos))

  query_turbo <- paste0("
    WITH lote_data AS (
      SELECT id, direccion, cp, municipio_id
      FROM staging_import
      WHERE id IN (", ids_string, ")
    ),
    resultados AS (
      SELECT DISTINCT ON (ld.id)
        ld.id,
        ST_X(cp_p.geom)  AS lon,
        ST_Y(cp_p.geom)  AS lat,
        cp_p.nombre_via  AS via_encontrada,
        cp_p.num_norm    AS num_encontrado,
        similarity(cp_p.nombre_via, ld.direccion) AS score
      FROM lote_data ld
      CROSS JOIN LATERAL (
        SELECT geom, nombre_via, num_norm
        FROM callejero_portales
        WHERE cod_postal   = ld.cp
          AND municipio_id = ld.municipio_id
          AND similarity(nombre_via, ld.direccion) > 0.45
        ORDER BY
          (ld.direccion ~ ('\\y' || num_norm || '\\y')) DESC,
          nombre_via <-> ld.direccion
        LIMIT 1
      ) cp_p
    )
    UPDATE staging_import s
    SET longitud        = r.lon,
        latitud         = r.lat,
        direccion_match = r.via_encontrada || ' ' || r.num_encontrado,
        distancia_fuzzy = r.score,
        fuente_geocodigo = 'callejero_fuzzy:cp_portal',
        estado          = 'bruto'
    FROM resultados r WHERE s.id = r.id
    RETURNING s.id;")

  rescatados_lote  <- length(dbGetQuery(con, query_turbo)$id)
  total_rescatados <- total_rescatados + rescatados_lote
  cat(sprintf("geocodificados en lote: %d (acumulado: %d)\n",
              rescatados_lote, total_rescatados))
  Sys.sleep(0.05)

  # Marcar los que no encontraron match para que no vuelvan a entrar en este bucle
  dbExecute(con, paste0("
    UPDATE staging_import SET estado = 'geocod_muni_pendiente'
    WHERE id IN (", ids_string, ") AND estado = 'geocodificacion_pendiente'"))
}

# --- 3. RESUMEN FINAL ---
tasa <- if (n_candidatos > 0) round(100 * total_rescatados / n_candidatos, 1) else 0

cat("\n========================================\n")
cat("RESUMEN P02\n")
cat("  Candidatos          :", n_candidatos, "\n")
cat("  Geocodificados      :", total_rescatados, "\n")
cat("  Tasa de éxito       :", tasa, "%\n")
cat("  Pendientes (otros)  :", n_candidatos - total_rescatados, "\n")
cat("========================================\n")

cat("\nEstado de staging_import:\n")
print(dbGetQuery(con, "
  SELECT estado, COUNT(*)::int AS n
  FROM staging_import
  GROUP BY estado ORDER BY n DESC"))

cat("\nFuente geocodificación:\n")
print(dbGetQuery(con, "
  SELECT COALESCE(fuente_geocodigo, '(sin coordenadas)') AS fuente,
         COUNT(*)::int AS n
  FROM staging_import
  GROUP BY fuente ORDER BY n DESC"))

escribir_log("P02_FIN", paste0(
  "Geocodificados: ", total_rescatados, "/", n_candidatos,
  " (", tasa, "%). Pendientes: ", n_candidatos - total_rescatados))

dbDisconnect(con)
cat("\n✓ P02 completado.\n")
