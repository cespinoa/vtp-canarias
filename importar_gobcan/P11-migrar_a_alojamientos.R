library(DBI)
source("importar_gobcan/helper.R")

con <- conecta_db()

comunicar <- function(tipo, msg) {
  escribir_log(tipo, msg)
  cat(paste0("[", tipo, "] ", msg, "\n"))
}

comunicar("INFO", "Iniciando volcado final: staging_import -> alojamientos (con trazabilidad de duplicados)")

# 1. SQL con CTE para contar descartes y realizar el INSERT
query_migracion <- "
WITH conteo_duplicados AS (
    -- Contamos cuántos registros hay por ID menos el que vamos a conservar
    SELECT establecimiento_id, (COUNT(*) - 1) as eliminados
    FROM public.staging_import
    WHERE estado = 'finalizado_geo'
    GROUP BY establecimiento_id
)
INSERT INTO public.alojamientos (
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
    -- Concatenamos el conteo de duplicados eliminados en la nota de auditoría
    CONCAT(s.audit_nota, ' | duplicados_eliminados:', c.eliminados),
    CURRENT_DATE
FROM public.staging_import s
JOIN conteo_duplicados c ON s.establecimiento_id = c.establecimiento_id
WHERE s.estado = 'finalizado_geo'
ORDER BY s.establecimiento_id, s.id DESC
ON CONFLICT (establecimiento_id) 
DO UPDATE SET
    nombre_comercial = EXCLUDED.nombre_comercial,
    plazas = EXCLUDED.plazas,
    geom = EXCLUDED.geom,
    audit_resultado = EXCLUDED.audit_resultado,
    audit_nota = EXCLUDED.audit_nota, -- Actualizamos la nota con el nuevo conteo si cambia
    fecha_sistema = CURRENT_TIMESTAMP;"

comunicar("INFO", "Ejecutando INSERT con resolución de conflictos y conteo de descartes...")

tryCatch({
    res <- dbExecute(con, query_migracion)
    comunicar("SUCCESS", paste("Proceso finalizado. Filas afectadas en tabla alojamientos:", res))
    
    # 2. Informe rápido de la incidencia de duplicados para el Log
    incidencia <- dbGetQuery(con, "
        SELECT SUM(CAST(SUBSTRING(audit_nota FROM 'duplicados_eliminados:([0-9]+)') AS INTEGER)) as total_descartados
        FROM public.alojamientos")
    
    comunicar("INFO", paste("Total de registros duplicados omitidos en esta migración:", incidencia$total_descartados))

}, error = function(e) {
    comunicar("ERROR", paste("Fallo crítico en la migración:", e$message))
})

total_final <- dbGetQuery(con, "SELECT COUNT(*) FROM public.alojamientos")$count
comunicar("INFO", paste("Total registros actuales en tabla ALOJAMIENTOS:", total_final))

dbDisconnect(con)
comunicar("INFO", "Conexión cerrada.")
