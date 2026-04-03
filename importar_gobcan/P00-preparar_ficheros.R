#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P00-preparar_ficheros.R
# Lee los CSVs de alojamientos del historico, corrige el typo de cabecera de
# hoteleros, fusiona ap + ht en at, limpia saltos de línea y deja los ficheros
# listos en importar_gobcan/tmp/ para que P01 los consuma.
#
# Uso:
#   Rscript importar_gobcan/P00-preparar_ficheros.R
#   Rscript importar_gobcan/P00-preparar_ficheros.R 2025-12-31
#
# Si no se indica fecha_proceso se usa el conjunto más reciente disponible
# en importar_gobcan/historico/ (el que tenga la fecha mayor en vv-*.csv).
# ==============================================================================

source("importar_gobcan/helper.R")

dir_historico <- "importar_gobcan/historico"
dir_tmp       <- "importar_gobcan/tmp"
dir.create(dir_tmp, recursive = TRUE, showWarnings = FALSE)

# --- 1. DETERMINAR FECHA DE PROCESO ---
args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
  fecha_proceso <- args[1]
  cat("Fecha de proceso (parámetro):", fecha_proceso, "\n")
} else {
  candidatos_vv <- Sys.glob(file.path(dir_historico, "vv-????-??-??.csv"))
  if (length(candidatos_vv) == 0)
    stop("No se encontraron ficheros vv-*.csv en ", dir_historico)
  ultimo_vv   <- tail(sort(candidatos_vv), 1)
  fecha_proceso <- sub(".*vv-(.+)\\.csv$", "\\1", ultimo_vv)
  cat("Fecha de proceso (más reciente):", fecha_proceso, "\n")
}

# --- 2. LOCALIZAR FICHEROS ---
ruta_vv <- file.path(dir_historico, paste0("vv-", fecha_proceso, ".csv"))
ruta_ap <- file.path(dir_historico, paste0("ap-", fecha_proceso, ".csv"))
ruta_ht <- file.path(dir_historico, paste0("ht-", fecha_proceso, ".csv"))

for (ruta in c(ruta_vv, ruta_ap, ruta_ht)) {
  if (!file.exists(ruta)) stop("Fichero no encontrado: ", ruta)
}

cat("Fuentes:\n")
cat("  VV:", ruta_vv, "\n")
cat("  AP:", ruta_ap, "\n")
cat("  HT:", ruta_ht, "\n\n")

# --- 3. FUNCIÓN DE LECTURA ---
leer_csv_gobcan <- function(ruta) {
  read.csv(ruta, header = TRUE, sep = ";", quote = "\"", fill = TRUE,
           stringsAsFactors = FALSE, check.names = FALSE, encoding = "UTF-8")
}

limpiar_saltos <- function(df) {
  df[] <- lapply(df, function(x) if (is.character(x)) gsub("[\r\n]", " ", x) else x)
  df
}

# --- 4. PROCESAR VV ---
cat("Procesando VV...\n")
data_vv <- leer_csv_gobcan(ruta_vv)
data_vv  <- limpiar_saltos(data_vv)
write.csv(data_vv, file.path(dir_tmp, "vv.csv"), row.names = FALSE)
cat("  ✓", nrow(data_vv), "registros →", file.path(dir_tmp, "vv.csv"), "\n")
escribir_log("P00_VV", paste("VV procesado:", nrow(data_vv), "registros, fecha", fecha_proceso))

# --- 5. PROCESAR AT (ap + ht fusionados) ---
cat("Procesando AT (ap + ht)...\n")
data_ap <- leer_csv_gobcan(ruta_ap)
data_ht <- leer_csv_gobcan(ruta_ht)

# Corrección del typo en la cabecera de hoteleros
if ("direcion_municipio_nombre" %in% names(data_ht)) {
  names(data_ht)[names(data_ht) == "direcion_municipio_nombre"] <- "direccion_municipio_nombre"
  cat("  Typo corregido: direcion_municipio_nombre → direccion_municipio_nombre\n")
}

cols_comunes <- intersect(names(data_ap), names(data_ht))
data_at <- rbind(data_ap[, cols_comunes], data_ht[, cols_comunes])
data_at  <- limpiar_saltos(data_at)

write.csv(data_at, file.path(dir_tmp, "at.csv"), row.names = FALSE)
cat("  ✓", nrow(data_ap), "AP +", nrow(data_ht), "HT =", nrow(data_at),
    "registros →", file.path(dir_tmp, "at.csv"), "\n")
escribir_log("P00_AT", paste("AT fusionado:", nrow(data_at), "registros, fecha", fecha_proceso))

# --- 6. ESCRIBIR FICHERO DE FECHA ---
writeLines(fecha_proceso, file.path(dir_tmp, "fecha_proceso.txt"))

cat("\nP00 completado. Ficheros listos en", dir_tmp, "\n")
cat("Fecha grabada en", file.path(dir_tmp, "fecha_proceso.txt"), "\n")
