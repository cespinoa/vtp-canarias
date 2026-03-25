source("importar_gobcan/helper.R")
con <- conecta_db()

cat("Iniciando Fase 02-D: Rescate Final por Centroide (Estado: Todos los pendientes)...\n")

BATCH_SIZE <- 1000 
total_rescatados <- 0

repeat {
  # SELECCIÓN AMPLIADA: 
  # Buscamos cualquier registro sin coordenadas que tenga localidad_id
  # independientemente de si viene de Fase A, B o C.
  lote <- dbGetQuery(con, sprintf(
    "SELECT id, localidad_id FROM staging_import 
     WHERE (latitud IS NULL OR latitud = 0) 
       AND localidad_id IS NOT NULL 
       AND estado != 'bruto'
     LIMIT %d", BATCH_SIZE))
  
  if (nrow(lote) == 0) break
  
  ids_lote <- lote$id
  ids_string <- paste(ids_lote, collapse = ",")
  cat("Procesando lote de", length(ids_lote), "... ")
  
  query_centroide <- paste0("
    WITH resultados AS (
      SELECT 
        s.id, 
        ST_X(c.geom) as lon, 
        ST_Y(c.geom) as lat
      FROM staging_import s
      JOIN centroides_localidad c ON s.localidad_id::text = c.localidad_id::text
      WHERE s.id IN (", ids_string, ")
    )
    UPDATE staging_import s
    SET longitud = r.lon, 
        latitud = r.lat, 
        fuente_geocodigo = 'centroide:localidad', 
        estado = 'bruto',
        direccion_match = 'CENTROIDE LOCALIDAD RESCATADO'
    FROM resultados r WHERE s.id = r.id
    RETURNING s.id;")
  
  rescatados_ids <- dbGetQuery(con, query_centroide)$id
  n_exitos <- length(rescatados_ids)
  total_rescatados <- total_rescatados + n_exitos
  
  # CONTROL ANTI-BUCLE:
  # Si tenían localidad_id pero no se encontró centroide, los cerramos.
  ids_fallidos <- setdiff(ids_lote, rescatados_ids)
  if(length(ids_fallidos) > 0) {
    dbExecute(con, sprintf(
      "UPDATE staging_import SET estado = 'final_sin_posicion' WHERE id IN (%s)", 
      paste(ids_fallidos, collapse = ",")))
  }

  cat("Asignados:", n_exitos, "\n")
}

# Cerramos también los que no tienen localidad_id para limpiar la tabla
dbExecute(con, "UPDATE staging_import SET estado = 'final_sin_posicion' WHERE (latitud IS NULL OR latitud = 0)")

cat("\n--- BALANCE FINAL ABSOLUTO ---\n")
print(dbGetQuery(con, "SELECT fuente_geocodigo, estado, COUNT(*) FROM staging_import GROUP BY 1, 2 ORDER BY 3 DESC"))

dbDisconnect(con)
