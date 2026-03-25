# -------------------------------------------------------------------------
# SCRIPT: 01-A-ingesta_controlada.R
# -------------------------------------------------------------------------
source("importar_gobcan/helper.R")
con <- conecta_db()

dbExecute(con, "ALTER TABLE staging_import ADD COLUMN IF NOT EXISTS muni_norm TEXT;")
dbExecute(con, "ALTER TABLE staging_import ADD COLUMN IF NOT EXISTS loca_norm TEXT;")
dbExecute(con, "ALTER TABLE staging_import ADD COLUMN IF NOT EXISTS dir_norm TEXT;")
dbExecute(con, "ALTER TABLE staging_import ADD COLUMN IF NOT EXISTS audit_nota TEXT;")

mapear_y_cargar <- function(ruta_fichero, tipo_alojamiento) {
  cat("\nProcesando:", tipo_alojamiento, "...\n")
  raw <- read.csv(ruta_fichero, stringsAsFactors = FALSE, check.names = FALSE)
  
  lon_num <- as.numeric(raw[["longitud"]])
  lat_num <- as.numeric(raw[["latitud"]])

  # 1. Intentamos leer las plazas del CSV
  plazas_csv <- as.numeric(raw[["plazas"]])
  
  # 2. Preparamos los dormitorios por si acaso (solo para el cálculo de VV)
  d_ind <- if("numero_dormitorios_individuales_explotacion" %in% names(raw)) 
              as.numeric(raw[["numero_dormitorios_individuales_explotacion"]]) else 0
  d_dob <- if("numero_dormitorios_dobles_explotacion" %in% names(raw)) 
              as.numeric(raw[["numero_dormitorios_dobles_explotacion"]]) else 0
  
  # 3. LÓGICA DE DECISIÓN (Cascada de confianza)
  # - Si plazas_csv existe y es > 0: USAMOS ESE VALOR.
  # - Si no, calculamos por dormitorios: indiv + (dobles * 2).
  # - Si el cálculo da 0 o es NA: Ponemos el MÍNIMO de 2.
  
  df_final_plazas <- sapply(1:nrow(raw), function(i) {
    p <- plazas_csv[i]
    if (!is.na(p) && p > 0) {
      return(list(valor = p, estimada = FALSE))
    } else {
      # Entra el rescate
      calc <- (if(is.na(d_ind[i])) 0 else d_ind[i]) + (if(is.na(d_dob[i])) 0 else d_dob[i] * 2)
      final <- if(calc > 0) calc else 2
      return(list(valor = final, estimada = TRUE))
    }
  })

  # Extraemos los resultados de la lista
  plazas_pobladas <- unlist(df_final_plazas["valor", ])
  es_estimada     <- unlist(df_final_plazas["estimada", ])

  # 4. CONSTRUCCIÓN DEL DATA.FRAME
  df <- data.frame(
    origen_dato         = tipo_alojamiento,
    establecimiento_id  = raw[["establecimiento_id"]],
    nombre_comercial    = raw[["establecimiento_nombre_comercial"]],
    modalidad_texto     = raw[["establecimiento_modalidad"]],
    tipologia_texto     = raw[["establecimiento_tipologia"]],
    clasificacion_texto = raw[["establecimiento_clasificacion"]],
    direccion           = raw[["direccion"]],
    isla_nombre         = raw[["direccion_isla_nombre"]],
    muni_nombre         = raw[["direccion_municipio_nombre"]],
    loca_nombre         = raw[["direccion_localidad_nombre"]],
    cp                  = raw[["direccion_codigo_postal"]],
    
    dormitorios_indiv   = d_ind,
    dormitorios_dobles  = d_dob,
    unidades_explotacion = if("unidades_explotacion" %in% names(raw)) as.numeric(raw[["unidades_explotacion"]]) else 0,
    
    plazas              = plazas_pobladas,
    plazas_estimadas    = es_estimada,
    
    longitud            = lon_num,
    latitud             = lat_num,
    fuente_geocodigo    = ifelse(!is.na(lon_num) & !is.na(lat_num) & lon_num != 0, 'gobcan', NA),
    
    dir_norm            = sapply(raw[["direccion"]], normalizar_geo),
    muni_norm           = sapply(raw[["direccion_municipio_nombre"]], normalizar_geo),
    loca_norm           = sapply(raw[["direccion_localidad_nombre"]], normalizar_geo),
    
    estado              = 'bruto',
    stringsAsFactors    = FALSE
  )

  dbWriteTable(con, "staging_import", df, append = TRUE, row.names = FALSE)
  cat("✓", tipo_alojamiento, "cargado. Plazas originales respetadas.")
  escribir_log(paste("INGESTA", tipo_alojamiento), paste("Cargados", nrow(df)))

}

# 1. Limpieza preventiva
cat("Vaciando tabla de staging para carga limpia...\n")
dbExecute(con, "TRUNCATE TABLE staging_import")
escribir_log("DB_TRUNCATE", "Tabla staging_import vaciada para nueva carga")

# 2. Cargas individuales
tryCatch({
  mapear_y_cargar("importar_gobcan/tmp/vv.csv", "VV")
}, error = function(e) cat("Error en VV:", e$message, "\n"))

tryCatch({
  mapear_y_cargar("importar_gobcan/tmp/at.csv", "AT")
}, error = function(e) cat("Error en AT:", e$message, "\n"))




# --- Script 01: Ingesta y Preparación ---

# ... (Carga del CSV y normalización inicial de muni_norm, loca_norm, etc.) ...

cat("Modificando _U a null ...\n")
dbExecute(con, "
    UPDATE public.staging_import 
    SET 
        muni_nombre = CASE WHEN muni_nombre = '_U' THEN NULL ELSE muni_nombre END,
        loca_nombre = CASE WHEN loca_nombre = '_U' THEN NULL ELSE loca_nombre END,
        direccion   = CASE WHEN direccion   = '_U' THEN NULL ELSE direccion   END,
        cp          = CASE WHEN cp = '_U' OR cp = '0' THEN NULL ELSE cp END,
        muni_norm   = CASE WHEN muni_norm = '_u' THEN NULL ELSE muni_norm END,
        loca_norm   = CASE WHEN loca_norm = '_u' THEN NULL ELSE loca_norm END
    WHERE 
        muni_nombre = '_U' OR 
        loca_nombre = '_U' OR 
        direccion   = '_U' OR 
        cp IN ('_U', '0');
")



# --- LIMPIEZA DE COORDENADAS DISPARATADAS (Fuera de Canarias) ---
cat("Detectando y anulando coordenadas fuera del rango de Canarias...\n")

# Bounding Box aproximado de Canarias: 
# Longitud entre -19 y -13 | Latitud entre 27 y 30
dbExecute(con, "
    UPDATE public.staging_import
    SET 
        audit_nota = CASE 
            WHEN (longitud NOT BETWEEN -19 AND -13) OR (latitud NOT BETWEEN 27 AND 30) 
            THEN 'COORDS_FUERA_RANGO: ' || longitud || ', ' || latitud
            ELSE audit_nota 
        END,
        fuente_geocodigo = CASE 
            WHEN (longitud NOT BETWEEN -19 AND -13) OR (latitud NOT BETWEEN 27 AND 30) 
            THEN NULL 
            ELSE fuente_geocodigo 
        END,
        longitud = CASE 
            WHEN (longitud NOT BETWEEN -19 AND -13) OR (latitud NOT BETWEEN 27 AND 30) 
            THEN NULL 
            ELSE longitud 
        END,
        latitud = CASE 
            WHEN (longitud NOT BETWEEN -19 AND -13) OR (latitud NOT BETWEEN 27 AND 30) 
            THEN NULL 
            ELSE latitud 
        END
    WHERE fuente_geocodigo = 'gobcan';
")

# --- AUDITORÍA DE LIMPIEZA PARA EL LOG ---
res_limpieza <- dbGetQuery(con, "
    SELECT COUNT(*) as descartados 
    FROM public.staging_import 
    WHERE audit_nota LIKE 'COORDS_FUERA_RANGO%'
")

msg_limpieza <- paste("LIMPIEZA_GEO:", res_limpieza$descartados, "registros con coordenadas imposibles anulados.")
cat(msg_limpieza, "\n")
escribir_log("LIMPIEZA", msg_limpieza)



# --- VINCULACIÓN DE IDs ---
cat("Asignando IDs de municipio y localidad...\n")

# Municipio_id
dbExecute(con, "
    UPDATE public.staging_import si
    SET municipio_id = m.id
    FROM public.municipios m
    WHERE si.muni_norm = m.muni_norm 
      AND si.municipio_id IS NULL;
")

# Localidad_id (usando la nueva columna loca_norm que acabamos de crear arriba)
dbExecute(con, "
    UPDATE public.staging_import si
    SET localidad_id = l.id
    FROM public.localidades l
    WHERE si.municipio_id = l.municipio_id 
      AND si.loca_norm = l.loca_norm 
      AND si.localidad_id IS NULL;
")

# --- AUDITORÍA PARA EL LOG ---
res_audit <- dbGetQuery(con, "
    SELECT 
        COUNT(*) as total,
        COUNT(municipio_id) as con_muni,
        COUNT(localidad_id) as con_loca,
        COUNT(*) FILTER (WHERE loca_norm IS NOT NULL AND loca_norm != '' AND localidad_id IS NULL) as errores_loca
    FROM public.staging_import
")

msg_audit <- paste0(
    "\n[AUDITORÍA INGESTA ", Sys.time(), "]\n",
    "Total registros: ", res_audit$total, "\n",
    "Municipios vinculados: ", res_audit$con_muni, "\n",
    "Localidades vinculadas: ", res_audit$con_loca, "\n",
    "Localidades huérfanas: ", res_audit$errores_loca, "\n",
    "------------------------------------------\n"
)

cat(msg_audit)
escribir_log("AUDITORIA", msg_audit)



# Crear el mensaje de log
msg_log <- paste0(
    "\n--- Resumen de Vinculación de IDs ---\n",
    "Total registros en staging: ", res_audit$total, "\n",
    "Municipios vinculados: ", res_audit$con_municipio, " (Errores: ", res_audit$errores_muni, ")\n",
    "Localidades vinculadas: ", res_audit$con_localidad, " (Errores: ", res_audit$errores_loca, ")\n",
    "-------------------------------------\n"
)

# Imprimir en consola
cat(msg_log)

cat("Normalizando coordenadas: convirtiendo ceros y vacíos en NULL...\n")

# Ejecutamos la limpieza masiva
dbExecute(con, "
    UPDATE public.staging_import
    SET 
        latitud = CASE 
            WHEN latitud = 0 THEN NULL 
            WHEN CAST(latitud AS TEXT) IN ('', ' ', 'NaN') THEN NULL 
            ELSE latitud 
        END,
        longitud = CASE 
            WHEN longitud = 0 THEN NULL 
            WHEN CAST(longitud AS TEXT) IN ('', ' ', 'NaN') THEN NULL 
            ELSE longitud 
        END;
")

# Verificación para el log
pendientes <- dbGetQuery(con, "SELECT COUNT(*) as total FROM staging_import WHERE latitud IS NULL")
escribir_log("INFO", paste("Coordenadas normalizadas. Registros pendientes de geo:", pendientes$total))

cat("✓ Coordenadas listas (NULL para registros sin geolocalizar).\n")

# Escribir en el archivo de log (usando append para no borrar lo anterior)
# Sustituye 'ruta_log' por tu variable de ruta del archivo .log
escribir_log("AUDITORIA", msg_log)

if(res_audit$errores_loca > 0 || res_audit$errores_muni > 0) {
    aviso <- "!!! ATENCIÓN: Existen nombres normalizados que no han cruzado con los maestros.\n"
    cat(aviso)
    escribir_log("AVISO", aviso)
}


# 3. Resumen final en Log
resumen <- dbGetQuery(con, "SELECT origen_dato, COUNT(*) FROM staging_import GROUP BY origen_dato")
log_resumen <- paste(apply(resumen, 1, function(x) paste(x[1], x[2], sep=": ")), collapse=", ")
escribir_log("RESUMEN_INGESTA", log_resumen)

dbDisconnect(con)
cat("\n✓ Ingesta terminada. Resumen:", log_resumen, "\n")
