#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: PT03-Exportar_datos.R
# Reconstruye la vista materializada mv_full_snapshots_dashboard y exporta
# los tres JSONs que consume el visor Drupal.
#
# La fecha se obtiene de base_snapshots (escrita por PT01), o puede
# proporcionarse como argumento para re-exportar un snapshot histórico.
#
# Uso:
#   Rscript informes/PT03-Exportar_datos.R
#   Rscript informes/PT03-Exportar_datos.R 2025-12-31
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)
library(glue)
library(jsonlite)

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("PT03 — Exportación de datos\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

# --- FECHA DE PROCESO ---
args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
  fecha_input <- args[1]
  if (is.na(as.Date(fecha_input, format = "%Y-%m-%d")))
    stop("Fecha no válida. Use el formato YYYY-MM-DD")
  cat("Fecha de proceso (parámetro):", fecha_input, "\n")
} else {
  fecha_raw <- dbGetQuery(con,
    "SELECT DISTINCT fecha_calculo FROM base_snapshots LIMIT 1")$fecha_calculo
  if (length(fecha_raw) == 0)
    stop("base_snapshots vacío. Ejecute PT01 primero.")
  fecha_input <- as.character(fecha_raw)
  cat("Fecha de proceso (base_snapshots):", fecha_input, "\n")
}

fecha_sql <- shQuote(paste0(fecha_input, " 00:00:00"))

escribir_log("PT03_INICIO", paste("fecha_proceso:", fecha_input))

# --- 1. VISTA MATERIALIZADA ---
cat("\nReconstruyendo mv_full_snapshots_dashboard...\n")

invisible(dbExecute(con, "DROP MATERIALIZED VIEW IF EXISTS mv_full_snapshots_dashboard CASCADE;"))

res_campos     <- dbGetQuery(con, "SELECT id_campo FROM diccionario_de_datos WHERE en_mv = TRUE")
campos_dinamicos <- paste0("s.", res_campos$id_campo, collapse = ", ")

niveles <- list(
  list(ambito = "canarias",  tabla_geo = "canarias",   join = "1=1"),
  list(ambito = "isla",      tabla_geo = "islas",      join = "s.isla_id = g.id"),
  list(ambito = "municipio", tabla_geo = "municipios", join = "s.municipio_id = g.id")
)

sql_parts <- lapply(niveles, function(n) {
  glue::glue("
    SELECT
        s.id,
        {campos_dinamicos},
        g.geom::geometry(MultiPolygon, 4326) AS geom_martin
    FROM full_snapshots s
    JOIN {n$tabla_geo} g ON {n$join}
    WHERE s.fecha_calculo = {fecha_sql}
      AND s.ambito = '{n$ambito}'
  ")
})

sql_mv <- paste0(
  "CREATE MATERIALIZED VIEW mv_full_snapshots_dashboard AS ",
  paste(sql_parts, collapse = " UNION ALL "))

invisible(dbExecute(con, sql_mv))

invisible(dbExecute(con, "CREATE INDEX idx_mv_geom   ON mv_full_snapshots_dashboard USING gist (geom_martin);"))
invisible(dbExecute(con, "CREATE INDEX idx_mv_ambito ON mv_full_snapshots_dashboard (ambito);"))
invisible(dbExecute(con, "CLUSTER mv_full_snapshots_dashboard USING idx_mv_geom;"))
invisible(dbExecute(con, "ANALYZE mv_full_snapshots_dashboard;"))
cat("Vista materializada creada con índices.\n")

# --- 2. JSONs ---
ruta_general <- "/home/carlos/visor/web/sites/default/files/visor/"
ruta_backup  <- "/home/carlos/visor/web/sites/default/files/visor/backup/"
dir.create(ruta_backup, recursive = TRUE, showWarnings = FALSE)

# Backup de los JSONs anteriores antes de sobreescribir
for (f in c("datos_dashboard.json", "series.json", "localidades.json")) {
  origen <- paste0(ruta_general, f)
  if (file.exists(origen))
    file.copy(origen, paste0(ruta_backup, f), overwrite = TRUE)
}
cat("Backup de JSONs anteriores guardado en", ruta_backup, "\n")

get_campos <- function(columna_check) {
  dbGetQuery(con, glue::glue(
    "SELECT id_campo FROM diccionario_de_datos WHERE {columna_check} = TRUE"))$id_campo
}

campos_fijos <- c("ambito", "isla_id", "municipio_id", "localidad_id", "fecha_calculo", "etiqueta")

cat("\nGenerando datos_dashboard.json...")
data_dashboard <- dbGetQuery(con, glue::glue("
    SELECT * FROM full_snapshots
    WHERE fecha_calculo = {fecha_sql}
      AND ambito IN ('canarias', 'isla', 'municipio')"))
write_json(data_dashboard, paste0(ruta_general, "datos_dashboard.json"), simplifyVector = TRUE)
cat(" OK (", nrow(data_dashboard), "filas)\n")

cat("Generando series.json...")
campos_series <- unique(c(campos_fijos, get_campos("comparable")))
data_series <- dbGetQuery(con, glue::glue("
    SELECT {paste(campos_series, collapse=', ')}
    FROM full_snapshots
    WHERE ambito IN ('canarias', 'isla', 'municipio')
    ORDER BY fecha_calculo ASC"))
write_json(data_series, paste0(ruta_general, "series.json"), simplifyVector = TRUE)
cat(" OK (", nrow(data_series), "filas)\n")

cat("Generando localidades.json...")
campos_localidad <- unique(c(campos_fijos, get_campos("en_localidades")))
data_localidades <- dbGetQuery(con, glue::glue("
    SELECT {paste(campos_localidad, collapse=', ')}
    FROM full_snapshots
    WHERE fecha_calculo = {fecha_sql}
      AND ambito = 'localidad'"))
write_json(data_localidades, paste0(ruta_general, "localidades.json"), simplifyVector = TRUE)
cat(" OK (", nrow(data_localidades), "filas)\n")

escribir_log("PT03_FIN", paste(
  "fecha_proceso:", fecha_input,
  "| dashboard:", nrow(data_dashboard),
  "| series:", nrow(data_series),
  "| localidades:", nrow(data_localidades)))

dbDisconnect(con)

cat("\nReiniciando Martin...\n")
system("docker restart martin-canarias-production", ignore.stdout = TRUE, ignore.stderr = TRUE)
cat("Martin reiniciado.\n")

cat("\n✓ PT03 completado.\n")
