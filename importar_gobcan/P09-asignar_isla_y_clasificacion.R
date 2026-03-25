library(DBI)
library(knitr)
source("importar_gobcan/helper.R")

con <- conecta_db()

escribir_log("INFO", "Iniciando proceso de normalización de jerarquías (Isla/Modalidad/Tipología/Clasificación)")

# --- PASO 0: REPARACIÓN DE TEXTOS SEGÚN LÓGICA DE NEGOCIO ---
escribir_log("INFO", "Reparando tipologías y clasificaciones vacías...")

# 0.1 Reparar Tipologías
dbExecute(con, "
  UPDATE staging_import 
  SET tipologia_texto = CASE 
    WHEN origen_dato = 'AT' THEN 'Sin tipología'
    WHEN origen_dato = 'VV' THEN 'Vivienda Vacacional'
  END
  WHERE tipologia_texto IS NULL OR tipologia_texto = '_U'")

# 0.2 Reparar Clasificaciones (Normalizando texto antes de cruzar IDs)
dbExecute(con, "
  UPDATE staging_import 
  SET clasificacion_texto = CASE 
    WHEN tipologia_texto IN ('Apartamento', 'Hotel', 'Hotel Urbano') THEN 'Sin categoría'
    WHEN tipologia_texto IN ('Casa Emblematica', 'Hotel Emblematico', 'Villa', 'Vivienda Turistica', 'Vivienda Vacacional', 'Vivienda turística') THEN 'Categoría única'
    ELSE clasificacion_texto
  END
  WHERE clasificacion_texto IS NULL OR clasificacion_texto = '_U'")

escribir_log("SUCCESS", "Textos reparados. Iniciando mapeo de IDs con protección de tildes.")

# --- PASO 1: MAPEO DE IDS (JERÁRQUICO) ---

# 1.1 Isla ID
dbExecute(con, "UPDATE staging_import s SET isla_id = m.isla_id FROM municipios m WHERE s.municipio_id = m.id")

# 1.2 Modalidad ID
dbExecute(con, "
  UPDATE staging_import s SET modalidad_id = mo.id 
  FROM modalidades mo 
  WHERE TRIM(UPPER(s.modalidad_texto)) = TRIM(UPPER(mo.nombre))")

# 1.3 Tipología ID (PROTEGIDO CONTRA TILDES)
# Usamos REPLACE para las tildes más comunes en las tipologías
dbExecute(con, "
  UPDATE staging_import s SET tipologia_id = t.id 
  FROM tipologias t 
  WHERE s.modalidad_id = t.modalidad_id 
  AND REPLACE(REPLACE(TRIM(UPPER(s.tipologia_texto)), 'Í', 'I'), 'Ó', 'O') = 
      REPLACE(REPLACE(TRIM(UPPER(t.nombre)), 'Í', 'I'), 'Ó', 'O')")

# 1.4 Clasificación ID (PROTEGIDO CONTRA TILDES)
dbExecute(con, "
  UPDATE staging_import s SET clasificacion_id = c.id 
  FROM clasificaciones c 
  WHERE s.tipologia_id = c.tipologia_id 
  AND REPLACE(TRIM(UPPER(s.clasificacion_texto)), 'Í', 'I') = 
      REPLACE(TRIM(UPPER(c.nombre)), 'Í', 'I')")

# --- PASO 2: TIPO DE OFERTA (AR / VV) ---
dbExecute(con, "
  UPDATE staging_import 
  SET tipo_oferta = CASE WHEN origen_dato = 'AT' THEN 'AR' ELSE 'VV' END")

escribir_log("INFO", "Proceso completado. Verificando integridad...")

# Comprobación final
check <- dbGetQuery(con, "SELECT COUNT(*) as huerfanos FROM staging_import WHERE clasificacion_id IS NULL")
if(check$huerfanos == 0) {
  escribir_log("SUCCESS", "¡Perfecto! Todos los registros han sido normalizados (0 huérfanos).")
} else {
  escribir_log("WARNING", paste("Aún quedan", check$huerfanos, "registros sin ID. Revisa posibles nuevas tipologías."))
}


# --- PASO 3: MICRODESTINOS (OPTIMIZADO PARA RASPBERRY PI) ---
escribir_log("INFO", "Iniciando cruce espacial por bloques de isla...")

# Obtenemos las islas presentes para iterar
islas <- dbGetQuery(con, "SELECT DISTINCT isla_id FROM staging_import WHERE isla_id IS NOT NULL")$isla_id

for(id in islas) {
  escribir_log("INFO", paste("Procesando isla ID:", id))
  
  dbExecute(con, sprintf("
    UPDATE staging_import s
    SET 
      en_area_turistica = d.turistica,
      geocode_area_turistica = d.geocode
    FROM destinos_turisticos d
    WHERE s.isla_id = %s 
      AND ST_Intersects(s.geom, d.geometria)", id))
  
  # Liberar un poco el hilo de escritura de la SD
  Sys.sleep(0.5) 
}

escribir_log("SUCCESS", "Cruce espacial finalizado.")
cat ("Cruce espacial finalizado. \n")


dbDisconnect(con)
