#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P11-migrar_a_alojamientos.R
# Migra los registros de staging_import a la tabla alojamientos con resolución
# de duplicados (DISTINCT ON), gestión de altas y bajas.
#
# fecha_proceso: fecha del conjunto de ficheros procesados (YYYY-MM-DD).
#   - Registros nuevos:       fecha_alta = fecha_proceso
#   - Registros reactivados:  fecha_baja = NULL
#   - Registros desaparecidos: fecha_baja = fecha_proceso
#
# Uso:
#   Rscript importar_gobcan/P11-migrar_a_alojamientos.R
#   Rscript importar_gobcan/P11-migrar_a_alojamientos.R 2025-12-31
# ==============================================================================

library(DBI)
source("importar_gobcan/helper.R")
con <- conecta_db()

comunicar <- function(tipo, msg) {
  escribir_log(tipo, msg)
  cat(paste0("[", tipo, "] ", msg, "\n"))
}

cat("========================================\n")
cat("P11 — Migración staging → alojamientos\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

# --- 1. DETERMINAR FECHA DE PROCESO ---
args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
  fecha_proceso <- args[1]
  comunicar("INFO", paste("Fecha de proceso (parámetro):", fecha_proceso))
} else {
  candidatos <- Sys.glob("importar_gobcan/historico/vv-????-??-??.csv")
  if (length(candidatos) == 0)
    stop("No se encontraron ficheros vv-*.csv en importar_gobcan/historico/")
  ultimo      <- tail(sort(candidatos), 1)
  fecha_proceso <- sub(".*vv-(.+)\\.csv$", "\\1", ultimo)
  comunicar("INFO", paste("Fecha de proceso (más reciente):", fecha_proceso))
}

# --- 2. MIGRACIÓN: INSERT + ON CONFLICT ---
comunicar("INFO", "Ejecutando INSERT con resolución de duplicados...")

query_migracion <- sprintf("
WITH conteo_duplicados AS (
  SELECT establecimiento_id, (COUNT(*) - 1) AS eliminados
  FROM staging_import
  WHERE estado = 'finalizado_geo'
  GROUP BY establecimiento_id
)
INSERT INTO alojamientos (
  establecimiento_id, nombre_comercial,
  isla_id, municipio_id, localidad_id,
  modalidad_id, tipologia_id, clasificacion_id, tipo_oferta,
  plazas, unidades_explotacion, plazas_estimadas,
  muni_original_gobcan, muni_detectado_geo,
  direccion_original, direccion_match, distancia_fuzzy,
  fuente_geocodigo, metodo_localidad, geo_erronea_gobcan,
  en_area_turistica, geocode_area_turistica,
  modalidad_original, tipologia_original, clasificacion_original,
  geom, audit_resultado, audit_nota, fecha_alta
)
SELECT DISTINCT ON (s.establecimiento_id)
  s.establecimiento_id, s.nombre_comercial,
  s.isla_id, s.municipio_id, s.localidad_id,
  s.modalidad_id, s.tipologia_id, s.clasificacion_id, s.tipo_oferta,
  s.plazas, s.unidades_explotacion, s.plazas_estimadas,
  s.muni_nombre, s.muni_detectado_geo,
  s.direccion, s.direccion_match, s.distancia_fuzzy,
  s.fuente_geocodigo, s.metodo_localidad, s.geo_erronea_gobcan,
  s.en_area_turistica, s.geocode_area_turistica,
  s.modalidad_texto, s.tipologia_texto, s.clasificacion_texto,
  s.geom, s.audit_resultado,
  CONCAT(s.audit_nota, ' | duplicados_eliminados:', c.eliminados),
  '%s'::date
FROM staging_import s
JOIN conteo_duplicados c ON s.establecimiento_id = c.establecimiento_id
WHERE s.estado = 'finalizado_geo'
ORDER BY s.establecimiento_id, s.id DESC
ON CONFLICT (establecimiento_id) DO UPDATE SET
  nombre_comercial     = EXCLUDED.nombre_comercial,
  plazas               = EXCLUDED.plazas,
  unidades_explotacion = EXCLUDED.unidades_explotacion,
  geom                 = EXCLUDED.geom,
  audit_resultado      = EXCLUDED.audit_resultado,
  audit_nota           = EXCLUDED.audit_nota,
  fecha_baja           = NULL,
  fecha_sistema        = CURRENT_TIMESTAMP;", fecha_proceso)

tryCatch({
  n_afectados <- dbExecute(con, query_migracion)
  comunicar("SUCCESS", paste("Filas afectadas en alojamientos:", n_afectados))
}, error = function(e) {
  comunicar("ERROR", paste("Fallo crítico en la migración:", e$message))
  dbDisconnect(con)
  stop(e$message)
})

# --- 3. BAJAS: establecimientos que ya no aparecen en el staging ---
n_bajas <- dbExecute(con, sprintf("
  UPDATE alojamientos a
  SET fecha_baja = '%s'::date
  WHERE a.fecha_baja IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM staging_import s
      WHERE s.establecimiento_id = a.establecimiento_id
    );", fecha_proceso))

comunicar("INFO", paste("Bajas registradas (desaparecidos del registro):", n_bajas))

# --- 4. RESUMEN ---
total   <- dbGetQuery(con, "SELECT COUNT(*)::int AS n FROM alojamientos")$n
activos <- dbGetQuery(con, "SELECT COUNT(*)::int AS n FROM alojamientos WHERE fecha_baja IS NULL")$n
bajas   <- total - activos

incidencia <- dbGetQuery(con, "
  SELECT COALESCE(SUM(
    CAST(SUBSTRING(audit_nota FROM 'duplicados_eliminados:([0-9]+)') AS INTEGER)
  ), 0)::int AS total_descartados
  FROM alojamientos")

cat("\n========================================\n")
cat("RESUMEN P11\n")
cat("========================================\n")
cat("Total en alojamientos :", total,   "\n")
cat("Activos (sin baja)    :", activos, "\n")
cat("Con fecha_baja        :", bajas,   "\n")
cat("Duplicados descartados:", incidencia$total_descartados, "\n")
cat("Fecha de proceso      :", fecha_proceso, "\n")

comunicar("INFO", paste0(
  "Total: ", total,
  " | Activos: ", activos,
  " | Bajas: ", bajas,
  " | Duplicados descartados: ", incidencia$total_descartados))

dbDisconnect(con)
cat("\n✓ P11 completado.\n")
