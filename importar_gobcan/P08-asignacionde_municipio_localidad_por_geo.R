source("importar_gobcan/helper.R")
con <- conecta_db()

# 1. OPTIMIZACIÓN (Ahora en llamadas separadas para evitar el error)
dbExecute(con, "SET work_mem = '128MB';")
dbExecute(con, "SET synchronous_commit = OFF;")

cat("--- INICIANDO RESOLUCIÓN GEOGRÁFICA FINAL (Directa + Proximidad) ---\n")

# 2. ASEGURAR GEOMETRÍAS
dbExecute(con, "
    UPDATE staging_import 
    SET geom = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326)
    WHERE longitud IS NOT NULL AND latitud IS NOT NULL AND geom IS NULL;")

# 3. PASO 1: ASIGNACIÓN DIRECTA
cat("Ejecutando asignación DIRECTA... ")
res_directa <- dbExecute(con, "
    WITH cruce AS (
        SELECT s.id, l.id as loc_id, l.municipio_id as muni_id
        FROM staging_import s
        JOIN public.localidades l ON ST_Intersects(s.geom, l.geom)
        WHERE s.geom IS NOT NULL AND s.estado != 'finalizado_geo'
    )
    UPDATE staging_import s
    SET 
        localidad_id = c.loc_id,
        municipio_id = c.muni_id,
        metodo_localidad = 'directa',
        estado = 'finalizado_geo'
    FROM cruce c
    WHERE s.id = c.id;")
cat(res_directa, "registros OK.\n")

# 4. PASO 2: RESCATE DE HUÉRFANOS POR PROXIMIDAD
huerfanos_ids_df <- dbGetQuery(con, "
    SELECT id FROM staging_import 
    WHERE geom IS NOT NULL AND estado != 'finalizado_geo'")

if(nrow(huerfanos_ids_df) > 0) {
    huerfanos_ids <- huerfanos_ids_df$id
    cat("Rescatando", length(huerfanos_ids), "huérfanos por PROXIMIDAD MUNICIPAL...\n")
    
    BATCH_SIZE <- 500
    num_lotes <- ceiling(length(huerfanos_ids) / BATCH_SIZE)
    
    for(i in 1:num_lotes) {
        inicio <- (i-1) * BATCH_SIZE + 1
        fin <- min(i * BATCH_SIZE, length(huerfanos_ids))
        lote_ids <- huerfanos_ids[inicio:fin]
        
        query_proximidad <- sprintf("
            WITH buscador AS (
                SELECT DISTINCT ON (s.id)
                    s.id,
                    l.id as loc_id,
                    ST_Distance(s.geom::geography, l.geom::geography) as dist
                FROM staging_import s
                CROSS JOIN LATERAL (
                    SELECT id, geom
                    FROM public.localidades
                    WHERE municipio_id = s.municipio_id
                    ORDER BY s.geom <-> geom
                    LIMIT 1
                ) l
                WHERE s.id IN (%s)
            )
            UPDATE staging_import s
            SET 
                localidad_id = b.loc_id,
                metodo_localidad = 'proximidad',
                audit_nota = COALESCE(audit_nota, '') || ' | DIST_LOC: ' || ROUND(b.dist::numeric, 0) || 'm',
                estado = 'finalizado_geo'
            FROM buscador b
            WHERE s.id = b.id;", paste(lote_ids, collapse = ","))
        
        dbExecute(con, query_proximidad)
        cat(sprintf("\rLote %d/%d completado", i, num_lotes))
        Sys.sleep(0.05)
    }
    cat("\n✓ Rescate finalizado.\n")
}

# 5. AUDITORÍA FINAL
cat("\n--- RESULTADOS FINALES DE ASIGNACIÓN ---\n")
auditoria <- dbGetQuery(con, "
    SELECT 
        metodo_localidad, 
        COUNT(*) as total
    FROM staging_import 
    WHERE estado = 'finalizado_geo'
    GROUP BY 1")
print(auditoria)

dbExecute(con, "ANALYZE staging_import;")
dbDisconnect(con)
cat("\n✓ Conexión cerrada con estilo.\n")
