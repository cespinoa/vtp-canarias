# =========================================================================
# SANEAMIENTO GEOGRÁFICO AUTOMÁTICO (Rescate de Náufragos y Bañistas)
# =========================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()


cat("\nIniciando saneamiento de coordenadas disparatadas y bañistas...\n")

# 1. LIMPIEZA DE "ASTRONAUTAS" (Fuera del Bounding Box de Canarias)
# Anula coordenadas imposibles y deja nota en la auditoría
dbExecute(con, "
    UPDATE staging_import
    SET 
        audit_nota = 'COORDS_FUERA_RANGO: ' || longitud || ', ' || latitud,
        fuente_geocodigo = NULL,
        longitud = NULL,
        latitud = NULL,
        geom = NULL
    WHERE fuente_geocodigo = 'gobcan'
      AND ((longitud NOT BETWEEN -19 AND -13) OR (latitud NOT BETWEEN 27 AND 30));
")

# 2. RESCATE DE "BAÑISTAS" (Puntos en el mar a menos de 1km de su municipio)
# Esto salvará al registro de Adeje y similares de cualquier fuente
dbExecute(con, "
    WITH rescate AS (
        SELECT 
            s.id,
            cl.geom as nueva_geom,
            cl.nombre_loca as destino,
            ST_Distance(s.geom::geography, cl.geom::geography) as dist
        FROM staging_import s
        CROSS JOIN LATERAL (
            SELECT geom, nombre_loca
            FROM public.centroides_localidad
            WHERE municipio_id = s.municipio_id
            ORDER BY s.geom <-> geom
            LIMIT 1
        ) cl
        WHERE s.audit_resultado = 'FUERA_DE_TIERRA (MAR)'
          AND ST_Distance(s.geom::geography, cl.geom::geography) < 1000
    )
    UPDATE staging_import s
    SET 
        geom = r.nueva_geom,
        latitud = ST_Y(r.nueva_geom),
        longitud = ST_X(r.nueva_geom),
        fuente_geocodigo = 'centroide:rescate_mar',
        audit_nota = COALESCE(audit_nota, '') || ' | RESCATE_BAÑISTA: ' || ROUND(r.dist::numeric, 0) || 'm a ' || r.destino,
        audit_resultado = 'OK_COHERENTE'
    FROM rescate r
    WHERE s.id = r.id;
")

# 3. FALLBACK PARA NÁUFRAGOS LEJANOS
# Si después de lo anterior sigue habiendo MAR, los mandamos al centroide del municipio
dbExecute(con, "
    UPDATE staging_import s
    SET 
        geom = cm.geom,
        latitud = ST_Y(cm.geom),
        longitud = ST_X(cm.geom),
        fuente_geocodigo = 'centroide:municipio_forzado',
        audit_resultado = 'OK_COHERENTE'
    FROM public.centroides_municipio cm
    WHERE s.municipio_id = cm.id
      AND s.audit_resultado = 'FUERA_DE_TIERRA (MAR)';
")

# Reporte al Log
resumen_rescate <- dbGetQuery(con, "
    SELECT fuente_geocodigo, COUNT(*) as total 
    FROM staging_import 
    WHERE fuente_geocodigo LIKE 'centroide:%_rescate%' 
    GROUP BY 1
")
escribir_log("SANEAMIENTO_GEO", paste(capture.output(print(resumen_rescate)), collapse="\n"))

cat("✓ Saneamiento geográfico completado.\n")
