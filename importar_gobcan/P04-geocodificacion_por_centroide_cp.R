source("importar_gobcan/helper.R")
con <- conecta_db()

cat("Iniciando Fase 02-E (REVISADA): Rescate por CP + Municipio...\n")

# No filtramos por estado, solo por ausencia de coordenadas y presencia de CP
query_lote_inicial <- "
    SELECT id, cp, municipio_id 
    FROM staging_import 
    WHERE (latitud IS NULL OR latitud = 0) 
      AND cp IS NOT NULL 
      AND municipio_id IS NOT NULL"

lote_total <- dbGetQuery(con, query_lote_inicial)
cat("Registros candidatos detectados:", nrow(lote_total), "\n")

if (nrow(lote_total) > 0) {
  # Procesamos en un solo bloque o por lotes si prefieres, 
  # pero al ser ~7k registros, un solo UPDATE con WITH es muy eficiente.
  
  query_resurrecion <- "
    WITH candidatos AS (
        SELECT id, cp, municipio_id
        FROM staging_import
        WHERE (latitud IS NULL OR latitud = 0) 
          AND cp IS NOT NULL 
          AND municipio_id IS NOT NULL
    ),
    matches AS (
        SELECT 
            can.id,
            ST_X(c.geom) as lon,
            ST_Y(c.geom) as lat,
            c.cod_postal,
            c.municipio_id as m_id
        FROM candidatos can
        INNER JOIN centroides_cp c ON 
            LPAD(TRIM(can.cp::text), 5, '0') = LPAD(TRIM(c.cod_postal::text), 5, '0')
            AND can.municipio_id = c.municipio_id
    )
    UPDATE staging_import s
    SET longitud = m.lon,
        latitud = m.lat,
        fuente_geocodigo = 'centroide:cp_municipio',
        estado = 'bruto',
        direccion_match = 'RESCATE CP+MUNI: ' || m.cod_postal
    FROM matches m
    WHERE s.id = m.id
    RETURNING s.id;"

  rescatados <- dbGetQuery(con, query_resurrecion)
  cat("✓ Registros geolocalizados con éxito:", nrow(rescatados), "\n")
}

# Ahora marcamos como 'final_sin_posicion_cp' solo a los que, 
# teniendo CP, siguen sin tener latitud después de este intento.
dbExecute(con, "
    UPDATE staging_import 
    SET estado = 'final_sin_posicion_cp' 
    WHERE (latitud IS NULL OR latitud = 0) 
      AND cp IS NOT NULL")

cat("\n--- BALANCE TRAS RESCATE REAL ---\n")
print(dbGetQuery(con, "
    SELECT fuente_geocodigo, estado, COUNT(*) 
    FROM staging_import 
    GROUP BY 1, 2 
    ORDER BY 3 DESC"))

dbDisconnect(con)
