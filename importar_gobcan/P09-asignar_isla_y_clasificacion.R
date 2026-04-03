#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P09-asignar_isla_y_clasificacion.R
# Asigna los IDs de clasificación jerárquica (isla, modalidad, tipología,
# clasificación) y el microdestino turístico a cada registro de staging_import.
#
# Pasos:
#   0. Reparar tipología/clasificación cuando vienen vacíos o _U del CSV.
#   1. Mapear IDs: isla → modalidad → tipología → clasificación.
#   2. Asignar tipo_oferta (AR/VV).
#   3. Cruce espacial por isla con destinos_turisticos (microdestino).
#
# Uso:
#   Rscript importar_gobcan/P09-asignar_isla_y_clasificacion.R
# ==============================================================================

library(DBI)
source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P09 — Asignación isla, clasificación y microdestino\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P09_INICIO", "Asignación de jerarquías y microdestinos iniciada")

# --- PASO 0: REPARACIÓN DE TEXTOS ---
cat("Paso 0: reparando tipologías y clasificaciones vacías o _U...\n")

dbExecute(con, "
  UPDATE staging_import
  SET tipologia_texto = CASE
    WHEN origen_dato = 'AT' THEN 'Sin tipología'
    WHEN origen_dato = 'VV' THEN 'Vivienda Vacacional'
  END
  WHERE tipologia_texto IS NULL OR tipologia_texto = '_U'")

dbExecute(con, "
  UPDATE staging_import
  SET clasificacion_texto = CASE
    WHEN tipologia_texto IN ('Apartamento', 'Hotel', 'Hotel Urbano')
      THEN 'Sin categoría'
    WHEN tipologia_texto IN ('Casa Emblematica', 'Hotel Emblematico', 'Villa',
                             'Vivienda Turistica', 'Vivienda Vacacional',
                             'Vivienda turística')
      THEN 'Categoría única'
    ELSE clasificacion_texto
  END
  WHERE clasificacion_texto IS NULL OR clasificacion_texto = '_U'")

escribir_log("P09_PASO0", "Textos reparados")

# --- PASO 1: MAPEO DE IDS JERÁRQUICO ---
cat("Paso 1: mapeando IDs (isla → modalidad → tipología → clasificación)...\n")

dbExecute(con, "
  UPDATE staging_import s
  SET isla_id = m.isla_id
  FROM municipios m WHERE s.municipio_id = m.id")

dbExecute(con, "
  UPDATE staging_import s
  SET modalidad_id = mo.id
  FROM modalidades mo
  WHERE TRIM(UPPER(s.modalidad_texto)) = TRIM(UPPER(mo.nombre))")

dbExecute(con, "
  UPDATE staging_import s
  SET tipologia_id = t.id
  FROM tipologias t
  WHERE s.modalidad_id = t.modalidad_id
    AND REPLACE(REPLACE(TRIM(UPPER(s.tipologia_texto)), 'Í', 'I'), 'Ó', 'O') =
        REPLACE(REPLACE(TRIM(UPPER(t.nombre)),          'Í', 'I'), 'Ó', 'O')")

dbExecute(con, "
  UPDATE staging_import s
  SET clasificacion_id = c.id
  FROM clasificaciones c
  WHERE s.tipologia_id = c.tipologia_id
    AND TRIM(UPPER(unaccent(s.clasificacion_texto))) =
        TRIM(UPPER(unaccent(c.nombre)))")

# --- PASO 2: TIPO DE OFERTA ---
dbExecute(con, "
  UPDATE staging_import
  SET tipo_oferta = CASE WHEN origen_dato = 'AT' THEN 'AR' ELSE 'VV' END")

escribir_log("P09_PASO1", "IDs mapeados y tipo_oferta asignado")

# --- PASO 3: MICRODESTINOS ---
cat("Paso 3: cruce espacial con destinos_turisticos por isla...\n")

islas <- dbGetQuery(con,
  "SELECT DISTINCT isla_id FROM staging_import WHERE isla_id IS NOT NULL")$isla_id

for (id in islas) {
  n <- dbExecute(con, sprintf("
    UPDATE staging_import s
    SET en_area_turistica      = d.turistica,
        geocode_area_turistica = d.geocode
    FROM destinos_turisticos d
    WHERE s.isla_id = %s
      AND ST_Intersects(s.geom, d.geometria)", id))
  cat(sprintf("  Isla %s: %d registros con microdestino.\n", id, n))
  Sys.sleep(0.5)
}

escribir_log("P09_PASO3", "Cruce espacial con microdestinos finalizado")

# --- RESUMEN FINAL ---
cat("\n========================================\n")
cat("RESUMEN P09\n")
cat("========================================\n")

res <- dbGetQuery(con, "
  SELECT
    COUNT(*)::int                                        AS total,
    COUNT(isla_id)::int                                  AS con_isla,
    COUNT(modalidad_id)::int                             AS con_modalidad,
    COUNT(tipologia_id)::int                             AS con_tipologia,
    COUNT(clasificacion_id)::int                         AS con_clasificacion,
    COUNT(*) FILTER (WHERE en_area_turistica)::int       AS en_microdestino
  FROM staging_import")

cat("Total registros    :", res$total,           "\n")
cat("Con isla_id        :", res$con_isla,         "\n")
cat("Con modalidad_id   :", res$con_modalidad,    "\n")
cat("Con tipologia_id   :", res$con_tipologia,    "\n")
cat("Con clasificacion_id:", res$con_clasificacion, "\n")
cat("En área turística  :", res$en_microdestino,  "\n")

# Huérfanos por nivel
huerfanos <- res$total - res$con_clasificacion
if (huerfanos > 0) {
  cat("\nAVISO:", huerfanos, "registros sin clasificacion_id.\n")
  cat("Detalle por nivel:\n")
  print(dbGetQuery(con, "
    SELECT
      CASE
        WHEN modalidad_id  IS NULL THEN 'sin modalidad'
        WHEN tipologia_id  IS NULL THEN 'sin tipologia'
        WHEN clasificacion_id IS NULL THEN 'sin clasificacion'
      END AS nivel_fallo,
      COUNT(*)::int AS n
    FROM staging_import
    WHERE clasificacion_id IS NULL
    GROUP BY 1 ORDER BY n DESC"))
  escribir_log("P09_AVISO",
    paste(huerfanos, "registros sin clasificacion_id — revisar nuevas tipologías"))
} else {
  cat("\nOK: todos los registros tienen clasificacion_id.\n")
  escribir_log("P09_CHECK", "Todos los registros normalizados (0 huérfanos)")
}

dbDisconnect(con)
cat("\n✓ P09 completado.\n")
