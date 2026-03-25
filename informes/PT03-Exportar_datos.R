# ==============================================================================
# SCRIPT: PT03-exportar_datos.R
# Objetivo: Reconstrucción de MV y exportación de JSONs estratégicos
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)
library(glue)
library(jsonlite)

# --- 1. CONFIGURACIÓN Y CONEXIÓN ---
source("importar_gobcan/helper.R")
con <- conecta_db()

# RECUPERACIÓN AUTOMÁTICA DE LA FECHA
# Le preguntamos a base_snapshots qué fecha acabamos de procesar
fecha_raw <- dbGetQuery(con, "SELECT DISTINCT fecha_calculo FROM base_snapshots LIMIT 1")$fecha_calculo

if (length(fecha_raw) == 0) {
    stop("ERROR: No se han encontrado datos en base_snapshots. ¿Ha fallado el proceso de captura?")
}

# Formateamos para SQL (con comillas simples)
fecha_sql <- shQuote(paste0(as.character(fecha_raw), " 00:00:00"))
cat("Iniciando exportación para la fecha:", as.character(fecha_raw), "\n")

# --- 2. GENERACIÓN DE LA MATERIALIZED VIEW ---

cat("\nReconstruyendo Vista Materializada desde tablas maestras...")

# 1. Eliminamos la vista anterior
dbExecute(con, "DROP MATERIALIZED VIEW IF EXISTS mv_full_snapshots_dashboard CASCADE;")

# 2. Obtener campos marcados para la MV
res_campos <- dbGetQuery(con, "SELECT id_campo FROM diccionario_de_datos WHERE en_mv = TRUE")
campos_dinamicos <- paste0("s.", res_campos$id_campo, collapse = ", ")

# Niveles y tablas de geometría
niveles <- list(
  list(ambito = "canarias", tabla_geo = "canarias",   join = "1=1"),
  list(ambito = "isla",     tabla_geo = "islas",      join = "s.isla_id = g.id"),
  list(ambito = "municipio", tabla_geo = "municipios", join = "s.municipio_id = g.id")
)

sql_parts <- lapply(niveles, function(n) {
  glue::glue("
    SELECT
        s.id,
        {campos_dinamicos},
        g.geom::geometry(MultiPolygon, 4326) as geom_martin
    FROM full_snapshots s
    JOIN {n$tabla_geo} g ON {n$join}
    WHERE s.fecha_calculo = {fecha_sql}
      AND s.ambito = '{n$ambito}'
  ")
})

sql_mv <- paste0(
    "CREATE MATERIALIZED VIEW mv_full_snapshots_dashboard AS ",
    paste(sql_parts, collapse = " UNION ALL ")
)

dbExecute(con, sql_mv)

# 3. Índices y Optimización
dbExecute(con, "CREATE INDEX idx_mv_geom ON mv_full_snapshots_dashboard USING gist (geom_martin);")
dbExecute(con, "CREATE INDEX idx_mv_ambito ON mv_full_snapshots_dashboard (ambito);")
dbExecute(con, "CLUSTER mv_full_snapshots_dashboard USING idx_mv_geom;")
dbExecute(con, "ANALYZE mv_full_snapshots_dashboard;")

# --- 3. GENERACIÓN DE JSONs ---

ruta_general <- "/home/carlos/visor/web/sites/default/files/visor/"

# A. DASHBOARD (Última foto)
cat("\nGenerando datos_dashboard.json...")
data_dashboard <- dbGetQuery(con, glue::glue("
    SELECT * FROM full_snapshots 
    WHERE fecha_calculo = {fecha_sql} 
    AND ambito IN ('canarias', 'isla', 'municipio')
"))
write_json(data_dashboard, paste0(ruta_general, "datos_dashboard.json"), simplifyVector = TRUE)

# B. SERIES (Histórico Comparable)
cat("\nGenerando series.json...")
campos_fijos <- c("ambito", "isla_id", "municipio_id", "localidad_id", "fecha_calculo", "etiqueta")

get_campos <- function(columna_check) {
  dbGetQuery(con, glue::glue("SELECT id_campo FROM diccionario_de_datos WHERE {columna_check} = TRUE"))$id_campo
}

campos_series <- unique(c(campos_fijos, get_campos("comparable")))
data_series <- dbGetQuery(con, glue::glue("
    SELECT {paste(campos_series, collapse=', ')} 
    FROM full_snapshots 
    WHERE ambito IN ('canarias', 'isla', 'municipio')
    ORDER BY fecha_calculo ASC
"))
write_json(data_series, paste0(ruta_general, "series.json"), simplifyVector = TRUE)

# C. LOCALIDADES (Proceso actual)
cat("\nGenerando localidades.json...")
campos_localidad <- unique(c(campos_fijos, get_campos("en_localidades")))
data_localidades <- dbGetQuery(con, glue::glue("
    SELECT {paste(campos_localidad, collapse=', ')} 
    FROM full_snapshots 
    WHERE fecha_calculo = {fecha_sql}
    AND ambito = 'localidad'
"))
write_json(data_localidades, paste0(ruta_general, "localidades.json"), simplifyVector = TRUE)

cat("\n¡Proceso finalizado con éxito!\n")
