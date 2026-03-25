source("importar_gobcan/helper.R")
con <- conecta_db()

cat("Iniciando Fase 02-G: Aplicando Fallback Municipal desde Tabla Maestra...\n")

# Usamos directamente tu tabla de centroides_municipio
query_fallback <- "
    WITH candidatos AS (
        -- Seleccionamos lo que aún no tiene coordenadas
        SELECT id, municipio_id
        FROM staging_import
        WHERE (latitud IS NULL OR latitud = 0)
          AND municipio_id IS NOT NULL
    ),
    resultados AS (
        SELECT 
            can.id,
            ST_X(cm.geom) as lon,
            ST_Y(cm.geom) as lat
        FROM candidatos can
        -- Cruce directo por ID de municipio
        INNER JOIN centroides_municipio cm ON can.municipio_id = cm.municipio_id
    )
    UPDATE staging_import s
    SET longitud = r.lon,
        latitud = r.lat,
        fuente_geocodigo = 'centroide:municipio_final',
        estado = 'bruto',
        direccion_match = 'FALLBACK MUNICIPAL: CP/DIR NO ENCONTRADA'
    FROM resultados r
    WHERE s.id = r.id
    RETURNING s.id;"

rescatados <- dbGetQuery(con, query_fallback)
n_final <- nrow(rescatados)

cat("✓ Registros rescatados mediante centroide municipal:", n_final, "\n")

# Marcamos los que ni con estas se han salvado (probablemente no tienen municipio_id)
dbExecute(con, "
    UPDATE staging_import 
    SET estado = 'final_error_sin_datos' 
    WHERE (latitud IS NULL OR latitud = 0)")

cat("\n--- ESTADO GLOBAL DE LA CARGA ---\n")
balance <- dbGetQuery(con, "
    SELECT 
        COALESCE(fuente_geocodigo, 'SIN COORDENADAS') as fuente, 
        COUNT(*) as total,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
    FROM staging_import 
    GROUP BY 1 
    ORDER BY 2 DESC")
print(balance)

dbDisconnect(con)
