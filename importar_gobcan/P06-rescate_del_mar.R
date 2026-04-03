#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P06-rescate_del_mar.R
# 1. Genera la geometría (geom) de staging_import a partir de longitud/latitud.
# 2. Detecta registros cuya coordenada cae fuera de cualquier polígono municipal
#    (es decir, en el mar o en tierra ajena a Canarias).
# 3. Si el punto está a menos de 1 km de un centroide de localidad del municipio
#    asignado: mueve la coordenada a ese centroide.
# 4. Fallback: si sigue fuera, mueve al centroide del municipio.
#
# Uso:
#   Rscript importar_gobcan/P06-rescate_del_mar.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P06 — Rescate de coordenadas en el mar\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P06_INICIO", "Rescate del mar iniciado")

# --- 1. GENERAR GEOMETRÍA DESDE LONGITUD/LATITUD ---
cat("Generando geometría (geom) desde longitud/latitud...\n")
n_geom <- dbExecute(con, "
  UPDATE staging_import
  SET geom = ST_SetSRID(ST_MakePoint(longitud, latitud), 4326)
  WHERE longitud IS NOT NULL AND latitud IS NOT NULL;")
cat("  Geometrías generadas:", n_geom, "\n\n")

# --- 2. DETECTAR PUNTOS EN EL MAR ---
# Un punto está "en el mar" si no cae dentro de ningún polígono municipal.
# Solo se comprueban registros con municipio_id asignado (los demás ya están
# gestionados como sin_posicion por P05).
n_mar <- dbGetQuery(con, "
  SELECT COUNT(*)::int AS n FROM staging_import s
  WHERE s.geom IS NOT NULL
    AND s.municipio_id IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM municipios m WHERE ST_Within(s.geom, m.geom)
    );")$n

cat("Registros con coordenadas fuera de polígonos municipales:", n_mar, "\n\n")

if (n_mar > 0) {
  # --- 3. RESCATE CERCANO: centroide de localidad a menos de 1 km ---
  n_rescate_localidad <- dbExecute(con, "
    WITH en_mar AS (
      SELECT s.id, s.geom, s.municipio_id
      FROM staging_import s
      WHERE s.geom IS NOT NULL
        AND s.municipio_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM municipios m WHERE ST_Within(s.geom, m.geom)
        )
    ),
    rescate AS (
      SELECT
        em.id,
        cl.geom                                                     AS nueva_geom,
        cl.nombre_loca                                              AS destino,
        ST_Distance(em.geom::geography, cl.geom::geography)        AS dist
      FROM en_mar em
      CROSS JOIN LATERAL (
        SELECT geom, nombre_loca
        FROM centroides_localidad
        WHERE municipio_id = em.municipio_id
        ORDER BY em.geom <-> geom
        LIMIT 1
      ) cl
      WHERE ST_Distance(em.geom::geography, cl.geom::geography) < 1000
    )
    UPDATE staging_import s
    SET geom             = r.nueva_geom,
        latitud          = ST_Y(r.nueva_geom),
        longitud         = ST_X(r.nueva_geom),
        fuente_geocodigo = 'centroide:rescate_mar',
        audit_nota       = COALESCE(audit_nota, '') ||
                           ' | RESCATE_MAR: ' || ROUND(r.dist::numeric, 0) ||
                           'm → ' || r.destino
    FROM rescate r WHERE s.id = r.id;")

  cat("Rescatados por localidad cercana (<1 km):", n_rescate_localidad, "\n")

  # --- 4. FALLBACK: centroide de municipio para los que siguen en el mar ---
  n_rescate_municipio <- dbExecute(con, "
    WITH en_mar AS (
      SELECT s.id, s.geom, s.municipio_id
      FROM staging_import s
      WHERE s.geom IS NOT NULL
        AND s.municipio_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM municipios m WHERE ST_Within(s.geom, m.geom)
        )
    )
    UPDATE staging_import s
    SET geom             = cm.geom,
        latitud          = ST_Y(cm.geom),
        longitud         = ST_X(cm.geom),
        fuente_geocodigo = 'centroide:municipio_forzado',
        audit_nota       = COALESCE(audit_nota, '') || ' | RESCATE_MAR_FALLBACK'
    FROM en_mar em
    JOIN centroides_municipio cm ON em.municipio_id = cm.municipio_id
    WHERE s.id = em.id;")

  cat("Rescatados por centroide de municipio (>1 km):", n_rescate_municipio, "\n")

  escribir_log("P06_RESCATE", paste0(
    "En el mar: ", n_mar,
    ". Rescatados por localidad: ", n_rescate_localidad,
    ". Por municipio: ", n_rescate_municipio))
} else {
  cat("Ningún punto en el mar. Nada que rescatar.\n")
  escribir_log("P06_RESCATE", "Ningún punto en el mar detectado")
}

# --- 5. RESUMEN ---
cat("\nFuentes actuales:\n")
print(dbGetQuery(con, "
  SELECT COALESCE(fuente_geocodigo, 'sin_posicion') AS fuente,
         COUNT(*)::int AS n
  FROM staging_import
  GROUP BY fuente ORDER BY n DESC"))

escribir_log("P06_FIN", paste("geom generadas:", n_geom, "| en el mar detectados:", n_mar))

dbDisconnect(con)
cat("\n✓ P06 completado.\n")
