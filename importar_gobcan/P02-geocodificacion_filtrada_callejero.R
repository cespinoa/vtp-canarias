source("importar_gobcan/helper.R")
con <- conecta_db()

# Ajustes de rendimiento
dbExecute(con, "SET work_mem = '64MB';")
dbExecute(con, "SET synchronous_commit = OFF;")

cat("Iniciando Rescate Turbo con FILTRO DE SEGURIDAD MUNICIPAL...\n")

# 1. RESET SELECTIVO
dbExecute(con, "
    UPDATE staging_import 
    SET estado = 'geocodificacion_pendiente', 
        fuente_geocodigo = NULL, 
        latitud = NULL, 
        longitud = NULL,
        direccion_match = NULL,
        distancia_fuzzy = NULL
    WHERE fuente_geocodigo IS NULL 
       OR fuente_geocodigo != 'gobcan';")

cat("✓ Reset selectivo completado. Respetados los registros originales de GobCan.\n")

BATCH_SIZE <- 500  
total_rescatados <- 0

repeat {
  # 2. SELECT ampliado: Necesitamos traer el municipio_id para el filtro de seguridad
  lote_ids_df <- dbGetQuery(con, sprintf(
    "SELECT id, municipio_id FROM staging_import 
     WHERE estado = 'geocodificacion_pendiente' 
       AND cp IS NOT NULL 
       AND municipio_id IS NOT NULL
       AND (fuente_geocodigo IS NULL OR fuente_geocodigo != 'gobcan')
     LIMIT %d", BATCH_SIZE))
  
  if (nrow(lote_ids_df) == 0) break
  
  ids_string <- paste(lote_ids_df$id, collapse = ",")
  cat("Procesando lote de", nrow(lote_ids_df), "... ")
  
  # 3. QUERY MAESTRA: Ahora con filtro de integridad territorial
  query_turbo <- paste0("
    WITH lote_data AS (
      -- Traemos municipio_id para asegurar que el match no salte de municipio
      SELECT id, direccion, cp, municipio_id 
      FROM staging_import 
      WHERE id IN (", ids_string, ")
    ),
    resultados AS (
      SELECT DISTINCT ON (ld.id)
        ld.id, 
        ST_X(cp_p.geom) as lon, 
        ST_Y(cp_p.geom) as lat,
        cp_p.nombre_via as via_encontrada,
        cp_p.num_norm as num_encontrado,
        similarity(cp_p.nombre_via, ld.direccion) as score
      FROM lote_data ld
      CROSS JOIN LATERAL (
        SELECT geom, nombre_via, num_norm 
        FROM callejero_portales
        WHERE cod_postal = ld.cp
          -- CAMBIO CLAVE: El portal debe pertenecer al mismo municipio que el registro
          AND municipio_id = ld.municipio_id 
          AND similarity(nombre_via, ld.direccion) > 0.45
        ORDER BY 
          -- Prioridad: Que el número de portal esté en el texto
          (ld.direccion ~ ('\\y' || num_norm || '\\y')) DESC,
          nombre_via <-> ld.direccion
        LIMIT 1
      ) cp_p
    )
    UPDATE staging_import s
    SET longitud = r.lon, 
        latitud = r.lat, 
        direccion_match = r.via_encontrada || ' ' || r.num_encontrado,
        distancia_fuzzy = r.score, 
        fuente_geocodigo = 'callejero_fuzzy:cp_portal', 
        estado = 'bruto'
    FROM resultados r WHERE s.id = r.id
    RETURNING s.id;")
  
  # Ejecución y conteo
  rescatados_lote <- dbGetQuery(con, query_turbo)$id |> length()
  total_rescatados <- total_rescatados + rescatados_lote
  
  # 4. Marcado de "Plan B" para evitar bucles (los que no encontraron match con este filtro)
  dbExecute(con, paste0("
    UPDATE staging_import SET estado = 'geocod_muni_pendiente' 
    WHERE id IN (", ids_string, ") AND estado = 'geocodificacion_pendiente'"))

  cat("Rescatados:", rescatados_lote, "\n")
}

# Verificación final
cat("\n--- ESTADO FINAL POST-CORRECCIÓN ---\n")
print(dbGetQuery(con, "SELECT fuente_geocodigo, COUNT(*) FROM staging_import GROUP BY 1"))

dbDisconnect(con)
