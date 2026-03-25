# ==============================================================================
# SCRIPT: calcular_full_snapshots.R
# MOTOR COMPLETO: Funciones + Motor Dinámico + Segmentación por Tipo
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

# --- 1. CONFIGURACIÓN Y CONEXIÓN ---
source("importar_gobcan/helper.R")
con <- conecta_db()

args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
    fecha_input <- args[1]
    # Validación técnica
    if (is.na(as.Date(fecha_input))) {
        stop("ERROR: La fecha proporcionada no es válida. Use el formato YYYY-MM-DD")
    }
} else {
    fecha_input <- as.character(Sys.Date())
}

# VARIABLE 1: Para las funciones de R (clase Date)
# Esta es la que usará capturar() -> format(fecha_proceso, "%Y")
fecha_proceso <- as.Date(fecha_input)

# VARIABLE 2: Para las consultas SQL (clase Character)
# Esta es la que usaremos en paste0() para el SQL
fecha_sql <- paste0(as.character(fecha_proceso), " 00:00:00")

cat("Procesando para R (Date):", as.character(fecha_proceso), "\n")
cat("Procesando para SQL (Timestamp):", fecha_sql, "\n")

cat("--- INICIANDO CAPTURA DE DATOS BASE ---", as.character(fecha_proceso), "\n")

# --- 2. FUNCIONES DE EXTRACCIÓN ---

get_hogares_limitado <- function(ambito_val, i_id) {
  if (!ambito_val %in% c("canarias", "isla")) return(list(miembros = NA_real_, year = NA))
  query <- if(ambito_val == "canarias") {
    "SELECT miembros, year FROM hogares WHERE ambito = 'canarias' ORDER BY year DESC LIMIT 1"
  } else {
    "SELECT miembros, year FROM hogares WHERE ambito = 'isla' AND isla_id = $1 ORDER BY year DESC LIMIT 1"
  }
  res <- if(ambito_val == "canarias") dbGetQuery(con, query) else dbGetQuery(con, query, params = list(as.integer(i_id)))
  if(nrow(res) > 0) list(miembros = res$miembros, year = res$year) else list(miembros = NA_real_, year = NA)
}

get_oferta_dinamica <- function(tabla, ambito_val, i_id, m_id, l_id, f_corte, tipo_oferta) {
  config <- if (ambito_val == "canarias") list(sql = "1=1", par = list()) else 
            if (ambito_val == "isla") list(sql = "isla_id = $1", par = list(as.integer(i_id))) else 
            if (ambito_val == "municipio") list(sql = "municipio_id = $1", par = list(as.integer(m_id))) else 
            list(sql = "localidad_id = $1", par = list(as.integer(l_id)))
  
  idx_f <- paste0("$", length(config$par) + 1)
  params <- c(config$par, as.character(f_corte))
  
  query <- paste0("SELECT en_area_turistica, COUNT(*) as n, SUM(plazas)::numeric as p FROM ", tabla, 
                  " WHERE ", config$sql, " AND fecha_alta <= ", idx_f, 
                  " AND (fecha_baja IS NULL OR fecha_baja > ", idx_f, ") AND tipo_oferta = '", tipo_oferta, "' GROUP BY en_area_turistica")
  res <- dbGetQuery(con, query, params = params)
  list(est_t = sum(res$n[res$en_area_turistica == TRUE], na.rm=T), est_r = sum(res$n[res$en_area_turistica == FALSE], na.rm=T),
       pla_t = sum(res$p[res$en_area_turistica == TRUE], na.rm=T), pla_r = sum(res$p[res$en_area_turistica == FALSE], na.rm=T))
}

get_datos_maestros <- function(ambito_val, i_id, m_id, f_proceso) {
  if (ambito_val == "localidad") return(list(pob=0, pob_y=NA, ptr=0, ptr_y=NA, ptv=0, ptv_p=NA))
  y <- as.numeric(format(f_proceso, "%Y")); m <- as.numeric(format(f_proceso, "%m"))
  filtros <- if(ambito_val=="canarias") "" else if(ambito_val=="isla") "AND isla_id=$2" else "AND municipio_id=$2"
  p_base <- if(ambito_val=="canarias") list(ambito_val, y) else list(ambito_val, if(ambito_val=="isla") as.integer(i_id) else as.integer(m_id), y)
  
  pob <- dbGetQuery(con, paste0("SELECT valor, year FROM poblacion WHERE ambito=$1 ", filtros, " AND year<=$", length(p_base), " ORDER BY year DESC LIMIT 1"), params=p_base)
  ptr <- dbGetQuery(con, paste0("SELECT pte_reglada, year FROM pte_reglada WHERE ambito=$1 ", filtros, " AND year<=$", length(p_base), " ORDER BY year DESC LIMIT 1"), params=p_base)
  ptv <- dbGetQuery(con, paste0("SELECT ptev, year, mes FROM pte_vacacional WHERE ambito=$1 ", filtros, " AND (year<$", length(p_base), " OR (year=$", length(p_base), " AND mes<=", m, ")) ORDER BY year DESC, mes DESC LIMIT 1"), params=p_base)
  
  list(pob=if(nrow(pob)>0) pob$valor else 0, pob_y=if(nrow(pob)>0) pob$year else NA,
       ptr=if(nrow(ptr)>0) ptr$pte_reglada else 0, ptr_y=if(nrow(ptr)>0) ptr$year else NA,
       ptv=if(nrow(ptv)>0) ptv$ptev else 0, ptv_p=if(nrow(ptv)>0) paste0(ptv$year,"-",sprintf("%02d", ptv$mes)) else NA)
}

get_ext_viviendas <- function(ambito_val, i_id, m_id) {
  if (ambito_val == "localidad") return(list(sup=NA, tot=0, vac=0, esp=0, hab=0))
  filtros <- if(ambito_val=="canarias") "" else if(ambito_val=="isla") "AND isla_id=$2" else "AND municipio_id=$2"
  p <- if(ambito_val=="canarias") list(ambito_val) else list(ambito_val, if(ambito_val=="isla") as.integer(i_id) else as.integer(m_id))
  sup <- dbGetQuery(con, paste0("SELECT superficie FROM superficies WHERE ambito=$1 ", filtros), params=p)
  viv <- dbGetQuery(con, paste0("SELECT total, vacias, esporadicas, habituales FROM viviendas_municipios WHERE ambito=$1 ", filtros), params=p)
  list(sup=if(nrow(sup)>0) sup$superficie/100 else NA, tot=if(nrow(viv)>0) viv$total else 0, vac=if(nrow(viv)>0) viv$vacias else 0, esp=if(nrow(viv)>0) viv$esporadicas else 0, hab=if(nrow(viv)>0) viv$habituales else 0)
}

capturar <- function(ambito, i_id, m_id, l_id, f_p, nom) {
  o_vv <- get_oferta_dinamica("alojamientos", ambito, i_id, m_id, l_id, f_p, "VV")
  o_ar <- get_oferta_dinamica("alojamientos", ambito, i_id, m_id, l_id, f_p, "AR")
  m_dat <- get_datos_maestros(ambito, i_id, m_id, f_p)
  ext <- get_ext_viviendas(ambito, i_id, m_id)
  hog <- get_hogares_limitado(ambito, i_id)
  
  data.frame(
    ambito=ambito, isla_id=as.integer(i_id), municipio_id=as.integer(m_id), localidad_id=as.integer(l_id),
    etiqueta=nom, fecha_calculo=f_p, superficie_km2=ext$sup, poblacion=m_dat$pob, poblacion_year=m_dat$pob_y,
    viviendas_total=ext$tot, viviendas_vacias=ext$vac, viviendas_esporadicas=ext$esp, viviendas_habituales=ext$hab,
    pte_r=m_dat$ptr, pte_r_year=m_dat$ptr_y, pte_v=m_dat$ptv, pte_v_periodo=m_dat$ptv_p,
    personas_por_hogar=hog$miembros, personas_por_hogar_year=hog$year,
    uds_vv_turisticas=o_vv$est_t, uds_vv_residenciales=o_vv$est_r, plazas_vv_turisticas=o_vv$pla_t, plazas_vv_residenciales=o_vv$pla_r,
    plazas_at_turisticas=o_ar$pla_t, plazas_at_residenciales=o_ar$pla_r
  )
}

# --- 3. PROCESAMIENTO ---

dbExecute(con, "TRUNCATE TABLE base_snapshots RESTART IDENTITY")
lista_base <- list()

cat("1/4 Canarias...\n")
d_can <- capturar("canarias", NA, NA, NA, fecha_proceso, "Canarias")
d_can$tipo_municipio <- "General"
d_can$tipo_isla <- "General"
d_can$etiqueta_ambito_superior <- NA
lista_base[[1]] <- d_can

cat("2/4 Islas...\n")
islas_ref <- dbGetQuery(con, "SELECT id, nombre, tipo_isla FROM islas")
for(i in 1:nrow(islas_ref)) {
  d <- capturar("isla", islas_ref$id[i], NA, NA, fecha_proceso, islas_ref$nombre[i])
  d$tipo_municipio <- "General"
  d$tipo_isla <- islas_ref$tipo_isla[i]
  d$etiqueta_ambito_superior <- "Canarias"
  lista_base[[length(lista_base)+1]] <- d
}

cat("3/4 Municipios...\n")
muni_ref <- dbGetQuery(con, "SELECT m.id, m.isla_id, m.nombre, m.tipo_municipio, i.nombre as nombre_isla FROM municipios m JOIN islas i ON m.isla_id = i.id")
for(i in 1:nrow(muni_ref)) {
  d <- capturar("municipio", muni_ref$isla_id[i], muni_ref$id[i], NA, fecha_proceso, muni_ref$nombre[i])
  d$tipo_municipio <- muni_ref$tipo_municipio[i]
  d$tipo_isla <- "General"
  d$etiqueta_ambito_superior <- muni_ref$nombre_isla[i]
  lista_base[[length(lista_base)+1]] <- d
}

cat("4/4 Localidades...\n")
loc_ref <- dbGetQuery(con, "SELECT l.id as l_id, l.nombre, m.id as m_id, m.isla_id as i_id, m.tipo_municipio, m.nombre as nombre_muni FROM localidades l JOIN municipios m ON l.municipio_id = m.id")
for(i in 1:nrow(loc_ref)) {
  d <- capturar("localidad", loc_ref$i_id[i], loc_ref$m_id[i], loc_ref$l_id[i], fecha_proceso, loc_ref$nombre[i])
  d$tipo_municipio <- loc_ref$tipo_municipio[i]
  d$tipo_isla <- "General"
  d$etiqueta_ambito_superior <- loc_ref$nombre_muni[i]
  lista_base[[length(lista_base)+1]] <- d
}

df_base <- bind_rows(lista_base)
dbWriteTable(con, "base_snapshots", df_base, append = TRUE, row.names = FALSE)
cat("¡Captura Base OK!\n")
