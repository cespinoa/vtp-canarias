#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P07-validacion_espacial.R
# Auditoría pasiva: clasifica cada registro según la coherencia entre el
# municipio declarado en el CSV y el municipio donde cae geográficamente.
# NO modifica coordenadas — solo escribe audit_resultado y muni_detectado_geo.
#
# Resultados posibles:
#   SIN_GEOMETRIA           — sin coordenadas (sin_posicion desde P05)
#   SIN_MUNICIPIO_ORIGEN    — muni_nombre no reconocido en mapa_municipios
#   FUERA_DE_TIERRA         — coordenadas en el mar (no debería quedar ninguno tras P06)
#   OK                      — municipio declarado coincide con el geográfico
#   DISCREPANCIA            — municipio declarado ≠ municipio geográfico
#
# Uso:
#   Rscript importar_gobcan/P07-validacion_espacial.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P07 — Validación espacial (auditoría pasiva)\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P07_INICIO", "Validación espacial iniciada")

# Resetear auditoría para que sea reproducible
dbExecute(con, "UPDATE staging_import SET audit_resultado = NULL, muni_detectado_geo = NULL;")

# --- 1. CLASIFICAR SIN_GEOMETRIA ---
# Se hace antes del JOIN para que los registros sin muni_nombre
# (convertidos a NULL en P01) queden también clasificados.
n_sin_geo <- dbExecute(con, "
  UPDATE staging_import
  SET audit_resultado = 'SIN_GEOMETRIA'
  WHERE longitud IS NULL;")
cat("Sin geometría:", n_sin_geo, "\n")

# --- 2. DETECTAR MUNICIPIO GEOGRÁFICO REAL ---
# Qué polígono municipal contiene cada punto.
cat("Calculando intersecciones geográficas...\n")
n_detectados <- dbExecute(con, "
  UPDATE staging_import s
  SET muni_detectado_geo = m.nombre
  FROM municipios m
  WHERE ST_Intersects(s.geom, m.geom)
    AND s.longitud IS NOT NULL;")
cat("Municipio geográfico detectado:", n_detectados, "\n\n")

# --- 3. CLASIFICAR EL RESTO ---
# JOIN con mapa_municipios para comparar municipio declarado vs geográfico.
n_clasificados <- dbExecute(con, "
  UPDATE staging_import s
  SET audit_resultado = CASE
      WHEN s.muni_detectado_geo IS NULL
        THEN 'FUERA_DE_TIERRA'
      WHEN LOWER(unaccent(s.muni_detectado_geo)) = LOWER(unaccent(m_map.muni_real))
        THEN 'OK'
      ELSE 'DISCREPANCIA'
    END
  FROM mapa_municipios m_map
  WHERE s.muni_nombre = m_map.muni_nombre
    AND s.audit_resultado IS NULL;")

# Los que tienen coordenadas pero muni_nombre no está en mapa_municipios
n_sin_origen <- dbExecute(con, "
  UPDATE staging_import
  SET audit_resultado = 'SIN_MUNICIPIO_ORIGEN'
  WHERE audit_resultado IS NULL
    AND longitud IS NOT NULL;")

cat("Clasificados por comparación geográfica:", n_clasificados, "\n")
cat("Sin municipio de origen reconocido    :", n_sin_origen, "\n\n")

# --- 4. INFORME ---
cat("========================================\n")
cat("RESULTADO DE AUDITORÍA\n")
cat("========================================\n")
resumen <- dbGetQuery(con, "
  SELECT audit_resultado,
         COUNT(*)::int AS total,
         ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
  FROM staging_import
  GROUP BY audit_resultado
  ORDER BY total DESC;")
print(resumen)

cat("\nDiscrepancias por fuente geocodificación:\n")
print(dbGetQuery(con, "
  SELECT fuente_geocodigo, audit_resultado, COUNT(*)::int AS total
  FROM staging_import
  WHERE audit_resultado IN ('DISCREPANCIA', 'FUERA_DE_TIERRA')
  GROUP BY fuente_geocodigo, audit_resultado
  ORDER BY total DESC;"))

cat("\nTop 10 pares municipio declarado → municipio geográfico (discrepancias):\n")
print(dbGetQuery(con, "
  SELECT muni_nombre AS declarado, muni_detectado_geo AS geografico,
         COUNT(*)::int AS total
  FROM staging_import
  WHERE audit_resultado = 'DISCREPANCIA'
  GROUP BY muni_nombre, muni_detectado_geo
  ORDER BY total DESC
  LIMIT 10;"))

escribir_log("P07_FIN", paste(
  capture.output(print(resumen)), collapse = " | "))

dbDisconnect(con)
cat("\n✓ P07 completado.\n")
