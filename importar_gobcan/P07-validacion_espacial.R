# -------------------------------------------------------------------------
# SCRIPT 05: AUDITORÍA DE DIAGNÓSTICO (SIN MODIFICACIÓN DE COORDENADAS)
# -------------------------------------------------------------------------
source("importar_gobcan/helper.R")
con <- conecta_db()

cat("Iniciando Auditoría Pasiva de Municipios...\n")

# --- 1. PREPARACIÓN DE COLUMNAS DE AUDITORÍA ---
# Añadimos columnas para ver el 'match' geográfico real
dbExecute(con, "ALTER TABLE staging_import ADD COLUMN IF NOT EXISTS muni_detectado_geo TEXT;")
dbExecute(con, "ALTER TABLE staging_import ADD COLUMN IF NOT EXISTS audit_resultado TEXT;")

# --- 2. DETECCIÓN GEOGRÁFICA REAL ---
# Miramos en qué polígono municipal cae CADA punto actualmente
cat("Calculando intersecciones geográficas reales...\n")
dbExecute(con, "
    UPDATE staging_import s
    SET muni_detectado_geo = m.nombre
    FROM public.municipios m
    WHERE ST_Intersects(
        ST_SetSRID(ST_MakePoint(s.longitud, s.latitud), 4326), 
        m.geom
    )
    AND s.longitud IS NOT NULL;
")

# --- 3. CLASIFICACIÓN DE HALLAZGOS (Diagnóstico) ---
cat("Clasificando coherencia entre datos y mapa...\n")
dbExecute(con, "
    UPDATE staging_import s
    SET audit_resultado = CASE 
        WHEN s.longitud IS NULL THEN 'SIN_GEOMETRIA'
        WHEN m_map.muni_real IS NULL THEN 'MUNICIPIO_ORIGEN_DESCONOCIDO'
        WHEN s.muni_detectado_geo IS NULL THEN 'FUERA_DE_TIERRA (MAR)'
        WHEN LOWER(unaccent(s.muni_detectado_geo)) = LOWER(unaccent(m_map.muni_real)) THEN 'OK_COHERENTE'
        ELSE 'ERROR_DISCREPANCIA_MUNICIPAL'
    END
    FROM public.mapa_municipios m_map
    WHERE s.muni_nombre = m_map.muni_nombre;
")

# --- 4. INFORME DE SITUACIÓN PARA ANÁLISIS ---
cat("\n--- INFORME GLOBAL DE COHERENCIA ---\n")
resumen_audit <- dbGetQuery(con, "
    SELECT audit_resultado, COUNT(*) as total 
    FROM staging_import 
    GROUP BY 1 
    ORDER BY 2 DESC;
")
print(resumen_audit)

cat("\n--- DETALLE DE DISCREPANCIAS POR FUENTE ---\n")
discrepancias <- dbGetQuery(con, "
    SELECT fuente_geocodigo, audit_resultado, COUNT(*) as total
    FROM staging_import
    WHERE audit_resultado IN ('ERROR_DISCREPANCIA_MUNICIPAL', 'FUERA_DE_TIERRA (MAR)')
    GROUP BY 1, 2
    ORDER BY 3 DESC;
")
print(discrepancias)

cat("\n--- TOP 10 MUNICIPIOS CON CONFLICTOS (Papel vs Mapa) ---\n")
# Esto nos dirá si un municipio específico está 'robando' puntos de otro
conflictos_muni <- dbGetQuery(con, "
    SELECT muni_nombre as muni_en_papel, muni_detectado_geo as muni_en_mapa, COUNT(*) as total
    FROM staging_import
    WHERE audit_resultado = 'ERROR_DISCREPANCIA_MUNICIPAL'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10;
")
print(conflictos_muni)



dbDisconnect(con)
cat("\n✓ Auditoría finalizada. Revisa las columnas 'muni_detectado_geo' y 'audit_resultado'.\n")
