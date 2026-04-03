#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P01-ingesta_controlada.R
# Lee los ficheros tmp/vv.csv y tmp/at.csv generados por P00, los carga en
# staging_import y aplica las limpiezas iniciales: nulos, coordenadas fuera
# de rango, coordenadas cero y vinculación con tablas maestras.
#
# Uso:
#   Rscript importar_gobcan/P01-ingesta_controlada.R
# ==============================================================================

library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

mapear_y_cargar <- function(ruta_fichero, tipo_alojamiento) {
  cat("\nProcesando:", tipo_alojamiento, "...\n")
  raw <- read.csv(ruta_fichero, stringsAsFactors = FALSE, check.names = FALSE)

  lon_num <- as.numeric(raw[["longitud"]])
  lat_num <- as.numeric(raw[["latitud"]])

  plazas_csv <- as.numeric(raw[["plazas"]])

  d_ind <- if ("numero_dormitorios_individuales_explotacion" %in% names(raw))
              as.numeric(raw[["numero_dormitorios_individuales_explotacion"]]) else 0
  d_dob <- if ("numero_dormitorios_dobles_explotacion" %in% names(raw))
              as.numeric(raw[["numero_dormitorios_dobles_explotacion"]]) else 0

  # Cascada de confianza para plazas:
  #   1. Si plazas_csv > 0: usar ese valor.
  #   2. Si no, calcular por dormitorios: indiv + (dobles × 2).
  #   3. Si el cálculo da 0 o NA: mínimo de 2.
  df_final_plazas <- sapply(1:nrow(raw), function(i) {
    p <- plazas_csv[i]
    if (!is.na(p) && p > 0) {
      return(list(valor = p, estimada = FALSE))
    } else {
      calc  <- (if (is.na(d_ind[i])) 0 else d_ind[i]) +
               (if (is.na(d_dob[i])) 0 else d_dob[i] * 2)
      final <- if (calc > 0) calc else 2
      return(list(valor = final, estimada = TRUE))
    }
  })

  plazas_pobladas <- unlist(df_final_plazas["valor", ])
  es_estimada     <- unlist(df_final_plazas["estimada", ])

  df <- data.frame(
    origen_dato          = tipo_alojamiento,
    establecimiento_id   = raw[["establecimiento_id"]],
    nombre_comercial     = raw[["establecimiento_nombre_comercial"]],
    modalidad_texto      = raw[["establecimiento_modalidad"]],
    tipologia_texto      = raw[["establecimiento_tipologia"]],
    clasificacion_texto  = raw[["establecimiento_clasificacion"]],
    direccion            = raw[["direccion"]],
    isla_nombre          = raw[["direccion_isla_nombre"]],
    muni_nombre          = raw[["direccion_municipio_nombre"]],
    loca_nombre          = raw[["direccion_localidad_nombre"]],
    cp                   = raw[["direccion_codigo_postal"]],
    dormitorios_indiv    = d_ind,
    dormitorios_dobles   = d_dob,
    unidades_explotacion = if ("unidades_explotacion" %in% names(raw))
                             as.numeric(raw[["unidades_explotacion"]]) else 0,
    plazas               = plazas_pobladas,
    plazas_estimadas     = es_estimada,
    longitud             = lon_num,
    latitud              = lat_num,
    fuente_geocodigo     = ifelse(!is.na(lon_num) & !is.na(lat_num) & lon_num != 0, "gobcan", NA),
    dir_norm             = sapply(raw[["direccion"]], normalizar_geo),
    muni_norm            = sapply(raw[["direccion_municipio_nombre"]], normalizar_geo),
    loca_norm            = sapply(raw[["direccion_localidad_nombre"]], normalizar_geo),
    estado               = "bruto",
    stringsAsFactors     = FALSE
  )

  dbWriteTable(con, "staging_import", df, append = TRUE, row.names = FALSE)
  cat("  ✓", nrow(df), "registros cargados.\n")
  escribir_log(paste("INGESTA", tipo_alojamiento), paste("Cargados", nrow(df)))
}

# --- 0. VERIFICAR FECHA DE P00 ---
ruta_fecha <- "importar_gobcan/tmp/fecha_proceso.txt"
if (!file.exists(ruta_fecha))
  stop("No se encontró importar_gobcan/tmp/fecha_proceso.txt. Ejecute P00 primero.")

fecha_p00 <- trimws(readLines(ruta_fecha, warn = FALSE))
cat("========================================\n")
cat("FECHA DE PROCESO (de P00):", fecha_p00, "\n")
cat("========================================\n\n")
escribir_log("P01_FECHA", paste("Procesando fecha de P00:", fecha_p00))

# --- 1. LIMPIEZA PREVENTIVA ---
cat("Vaciando staging_import...\n")
dbExecute(con, "TRUNCATE TABLE staging_import")
escribir_log("DB_TRUNCATE", "Tabla staging_import vaciada para nueva carga")

# --- 2. CARGA ---
tryCatch(mapear_y_cargar("importar_gobcan/tmp/vv.csv", "VV"),
         error = function(e) cat("Error en VV:", e$message, "\n"))

tryCatch(mapear_y_cargar("importar_gobcan/tmp/at.csv", "AT"),
         error = function(e) cat("Error en AT:", e$message, "\n"))

# --- 3. VALORES _U → NULL ---
cat("\nConvirtiendo _U a NULL...\n")
dbExecute(con, "
  UPDATE public.staging_import
  SET
    muni_nombre = CASE WHEN muni_nombre = '_U' THEN NULL ELSE muni_nombre END,
    loca_nombre = CASE WHEN loca_nombre = '_U' THEN NULL ELSE loca_nombre END,
    direccion   = CASE WHEN direccion   = '_U' THEN NULL ELSE direccion   END,
    cp          = CASE WHEN cp IN ('_U', '0')  THEN NULL ELSE cp          END,
    muni_norm   = CASE WHEN muni_norm   = '_u'  THEN NULL ELSE muni_norm  END,
    loca_norm   = CASE WHEN loca_norm   = '_u'  THEN NULL ELSE loca_norm  END
  WHERE muni_nombre = '_U' OR loca_nombre = '_U'
     OR direccion   = '_U' OR cp IN ('_U', '0');
")

# --- 4. COORDENADAS FUERA DEL BOUNDING BOX DE CANARIAS ---
# Longitud: −19 a −13 | Latitud: 27 a 30
cat("Anulando coordenadas fuera de rango...\n")
dbExecute(con, "
  UPDATE public.staging_import
  SET
    audit_nota       = 'COORDS_FUERA_RANGO: ' || longitud || ', ' || latitud,
    fuente_geocodigo = NULL,
    longitud         = NULL,
    latitud          = NULL
  WHERE fuente_geocodigo = 'gobcan'
    AND ((longitud NOT BETWEEN -19 AND -13) OR (latitud NOT BETWEEN 27 AND 30));
")

res_fuera <- dbGetQuery(con,
  "SELECT COUNT(*)::int AS n FROM staging_import WHERE audit_nota LIKE 'COORDS_FUERA_RANGO%'")
cat(" ", res_fuera$n, "registros con coordenadas fuera de rango anulados.\n")
escribir_log("LIMPIEZA_GEO", paste(res_fuera$n, "registros con coordenadas fuera de rango anulados"))

# --- 5. COORDENADAS CERO → NULL ---
cat("Convirtiendo coordenadas 0 a NULL...\n")
dbExecute(con, "
  UPDATE public.staging_import
  SET
    latitud  = CASE WHEN latitud  = 0 THEN NULL ELSE latitud  END,
    longitud = CASE WHEN longitud = 0 THEN NULL ELSE longitud END;
")

# --- 6. VINCULACIÓN CON TABLAS MAESTRAS ---
cat("Asignando municipio_id y localidad_id...\n")

dbExecute(con, "
  UPDATE public.staging_import si
  SET municipio_id = m.id
  FROM public.municipios m
  WHERE si.muni_norm = m.muni_norm
    AND si.municipio_id IS NULL;
")

dbExecute(con, "
  UPDATE public.staging_import si
  SET localidad_id = l.id
  FROM public.localidades l
  WHERE si.municipio_id = l.municipio_id
    AND si.loca_norm    = l.loca_norm
    AND si.localidad_id IS NULL;
")

# --- 7. AUDITORÍA FINAL ---
res_audit <- dbGetQuery(con, "
  SELECT
    COUNT(*)                                                            AS total,
    COUNT(municipio_id)                                                 AS con_muni,
    COUNT(localidad_id)                                                 AS con_loca,
    COUNT(*) FILTER (WHERE latitud IS NULL)                             AS sin_coords,
    COUNT(*) FILTER (WHERE loca_norm IS NOT NULL AND loca_norm != ''
                       AND localidad_id IS NULL)                        AS errores_loca
  FROM public.staging_import
")

msg_audit <- paste0(
  "\n[AUDITORÍA P01 — ", Sys.time(), "]\n",
  "Total registros    : ", res_audit$total,      "\n",
  "Municipios vinc.   : ", res_audit$con_muni,   "\n",
  "Localidades vinc.  : ", res_audit$con_loca,   "\n",
  "Sin coordenadas    : ", res_audit$sin_coords,  "\n",
  "Localidades huérf. : ", res_audit$errores_loca, "\n",
  "------------------------------------------\n"
)
cat(msg_audit)
escribir_log("AUDITORIA", msg_audit)

if (res_audit$errores_loca > 0) {
  aviso <- paste("AVISO: hay", res_audit$errores_loca,
                 "registros con loca_norm informado pero sin localidad_id.")
  cat(aviso, "\n")
  escribir_log("AVISO", aviso)
}

# --- 8. RESUMEN FINAL ---
resumen     <- dbGetQuery(con, "SELECT origen_dato, COUNT(*) n FROM staging_import GROUP BY origen_dato")
log_resumen <- paste(apply(resumen, 1, function(x) paste(x[1], x[2], sep = ": ")), collapse = ", ")
escribir_log("RESUMEN_INGESTA", log_resumen)

dbDisconnect(con)
cat("\n✓ P01 completado. Resumen:", log_resumen, "\n")
