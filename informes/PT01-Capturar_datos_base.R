#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: PT01-Capturar_datos_base.R
# Captura datos brutos para base_snapshots: Canarias, islas, municipios y
# localidades. La fecha de referencia se usa para:
#   - Filtrar alojamientos: fecha_alta <= fecha AND (fecha_baja IS NULL OR > fecha)
#   - Seleccionar el dato más reciente <= fecha en poblacion, pte_reglada, pte_vacacional
#
# La fecha se determina así:
#   - Si se proporciona como argumento → se usa esa (permite rehacer histórico)
#   - Si no → se toma MAX(fecha_alta) de alojamientos
#
# Si ya existen registros en full_snapshots para esa fecha, se eliminan antes
# de comenzar el proceso (deduplicación anticipada).
#
# Uso:
#   Rscript informes/PT01-Capturar_datos_base.R
#   Rscript informes/PT01-Capturar_datos_base.R 2025-12-31
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("PT01 — Captura de datos base\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

# --- FECHA DE PROCESO ---
args <- commandArgs(trailingOnly = TRUE)

if (length(args) > 0) {
  fecha_input <- args[1]
  if (is.na(as.Date(fecha_input, format = "%Y-%m-%d")))
    stop("Fecha no válida. Use el formato YYYY-MM-DD")
  cat("Fecha de proceso (parámetro)   :", fecha_input, "\n")
} else {
  fecha_input <- dbGetQuery(con,
    "SELECT MAX(fecha_alta)::text AS f FROM alojamientos")$f
  if (is.null(fecha_input) || is.na(fecha_input))
    stop("No se encontró ningún registro en alojamientos")
  cat("Fecha de proceso (MAX fecha_alta):", fecha_input, "\n")
}

fecha_proceso <- as.Date(fecha_input)
fecha_sql     <- paste0(as.character(fecha_proceso), " 00:00:00")

# --- DEDUPLICACIÓN ANTICIPADA ---
n_previos <- dbGetQuery(con, paste0(
  "SELECT COUNT(*)::int AS n FROM full_snapshots WHERE fecha_calculo = '", fecha_sql, "'"))$n

if (n_previos > 0) {
  cat("Eliminando", n_previos, "registros previos en full_snapshots para", fecha_input, "...\n")
  dbExecute(con, paste0(
    "DELETE FROM full_snapshots WHERE fecha_calculo = '", fecha_sql, "'"))
  cat("OK — registros previos eliminados.\n")
} else {
  cat("Sin registros previos en full_snapshots para esta fecha.\n")
}

cat("\n")
escribir_log("PT01_INICIO", paste("fecha_proceso:", fecha_input))

# --- FUNCIONES DE EXTRACCIÓN ---

get_hogares_limitado <- function(ambito_val, i_id, m_id = NA) {
  res <- switch(ambito_val,
    canarias  = dbGetQuery(con,
      "SELECT miembros, year FROM hogares WHERE ambito = 'canarias' ORDER BY year DESC LIMIT 1"),
    isla      = dbGetQuery(con,
      "SELECT miembros, year FROM hogares WHERE ambito = 'isla' AND isla_id = $1 ORDER BY year DESC LIMIT 1",
      params = list(as.integer(i_id))),
    municipio = dbGetQuery(con,
      "SELECT miembros, year FROM hogares WHERE ambito = 'municipio' AND municipio_id = $1 ORDER BY year DESC LIMIT 1",
      params = list(as.integer(m_id))),
    return(list(miembros = NA_real_, year = NA))  # localidad u otros
  )
  if (nrow(res) > 0) list(miembros = res$miembros, year = res$year) else
    list(miembros = NA_real_, year = NA)
}

get_oferta_dinamica <- function(tabla, ambito_val, i_id, m_id, l_id, f_corte, tipo_oferta) {
  config <- if (ambito_val == "canarias") list(sql = "1=1", par = list()) else
            if (ambito_val == "isla")     list(sql = "isla_id = $1",       par = list(as.integer(i_id))) else
            if (ambito_val == "municipio") list(sql = "municipio_id = $1", par = list(as.integer(m_id))) else
            list(sql = "localidad_id = $1", par = list(as.integer(l_id)))

  idx_f  <- paste0("$", length(config$par) + 1)
  params <- c(config$par, as.character(f_corte))

  query <- paste0(
    "SELECT en_area_turistica, COUNT(*) as n, SUM(plazas)::numeric as p ",
    "FROM ", tabla,
    " WHERE ", config$sql,
    " AND fecha_alta <= ", idx_f,
    " AND (fecha_baja IS NULL OR fecha_baja > ", idx_f, ")",
    " AND tipo_oferta = '", tipo_oferta, "'",
    " GROUP BY en_area_turistica")

  res <- dbGetQuery(con, query, params = params)
  list(
    est_t = sum(res$n[res$en_area_turistica == TRUE],  na.rm = TRUE),
    est_r = sum(res$n[res$en_area_turistica == FALSE], na.rm = TRUE),
    pla_t = sum(res$p[res$en_area_turistica == TRUE],  na.rm = TRUE),
    pla_r = sum(res$p[res$en_area_turistica == FALSE], na.rm = TRUE)
  )
}

get_datos_maestros <- function(ambito_val, i_id, m_id, f_proceso) {
  if (ambito_val == "localidad") return(list(pob = 0, pob_y = NA, ptr = 0, ptr_y = NA, ptv = 0, ptv_p = NA))

  y <- as.numeric(format(f_proceso, "%Y"))
  m <- as.numeric(format(f_proceso, "%m"))

  filtros <- if (ambito_val == "canarias") "" else
             if (ambito_val == "isla")     "AND isla_id=$2" else "AND municipio_id=$2"
  p_base  <- if (ambito_val == "canarias") list(ambito_val, y) else
             list(ambito_val, if (ambito_val == "isla") as.integer(i_id) else as.integer(m_id), y)

  pob <- dbGetQuery(con, paste0(
    "SELECT valor, year FROM poblacion WHERE ambito=$1 ", filtros,
    " AND year<=$", length(p_base), " ORDER BY year DESC LIMIT 1"), params = p_base)
  ptr <- dbGetQuery(con, paste0(
    "SELECT pte_reglada, year FROM pte_reglada WHERE ambito=$1 ", filtros,
    " AND year<=$", length(p_base), " ORDER BY year DESC LIMIT 1"), params = p_base)
  ptv <- dbGetQuery(con, paste0(
    "SELECT AVG(ptev) AS ptev, COUNT(*) AS n_meses,",
    "  MIN(year || '-' || LPAD(mes::text, 2, '0')) AS periodo_desde,",
    "  MAX(year || '-' || LPAD(mes::text, 2, '0')) AS periodo_hasta",
    " FROM pte_vacacional WHERE ambito=$1 ", filtros,
    " AND make_date(year, mes, 1) <= make_date($", length(p_base), ", ", m, ", 1)",
    " AND make_date(year, mes, 1) >  make_date($", length(p_base), ", ", m, ", 1) - INTERVAL '12 months'"),
    params = p_base)

  list(
    pob   = if (nrow(pob) > 0) pob$valor        else 0,
    pob_y = if (nrow(pob) > 0) pob$year         else NA,
    ptr   = if (nrow(ptr) > 0) ptr$pte_reglada  else 0,
    ptr_y = if (nrow(ptr) > 0) ptr$year         else NA,
    ptv   = if (!is.na(ptv$ptev)) ptv$ptev       else 0,
    ptv_p = if (!is.na(ptv$ptev)) paste0(ptv$periodo_desde, "/", ptv$periodo_hasta) else NA
  )
}

get_serpavi <- function(ambito_val, i_id, m_id, f_proceso) {
  if (ambito_val == "localidad") return(list(media = NA, anyo = NA, var10 = NA))

  y <- as.integer(format(f_proceso, "%Y"))

  # Precio medio ponderado por n_viviendas: el último año disponible <= año del snapshot
  # Para canarias/isla se agrega desde municipios (único nivel disponible en SERPAVI)
  filtro_ambito <- switch(ambito_val,
    canarias = "",
    isla     = "AND isla_id = $2",
    municipio = "AND municipio_id = $2"
  )
  params_base <- if (ambito_val == "canarias") list(y) else
    list(y, if (ambito_val == "isla") as.integer(i_id) else as.integer(m_id))

  sql_precio <- paste0("
    SELECT anyo,
           SUM(alq_m2_media * n_viviendas) / NULLIF(SUM(n_viviendas), 0) AS media
    FROM serpavi_alquiler
    WHERE alq_m2_media IS NOT NULL AND n_viviendas IS NOT NULL
      AND anyo <= $1 ", filtro_ambito, "
    GROUP BY anyo ORDER BY anyo DESC LIMIT 1")
  res <- dbGetQuery(con, sql_precio, params = params_base)

  if (nrow(res) == 0 || is.na(res$media)) return(list(media = NA, anyo = NA, var10 = NA))

  anyo_actual <- res$anyo
  precio_actual <- res$media
  anyo_base <- anyo_actual - 10

  params_base10 <- if (ambito_val == "canarias") list(anyo_base) else
    list(anyo_base, if (ambito_val == "isla") as.integer(i_id) else as.integer(m_id))

  sql_base10 <- paste0("
    SELECT SUM(alq_m2_media * n_viviendas) / NULLIF(SUM(n_viviendas), 0) AS media
    FROM serpavi_alquiler
    WHERE alq_m2_media IS NOT NULL AND n_viviendas IS NOT NULL
      AND anyo = $1 ", filtro_ambito)
  res10 <- dbGetQuery(con, sql_base10, params = params_base10)

  var10 <- if (nrow(res10) > 0 && !is.na(res10$media) && res10$media > 0)
    round((precio_actual - res10$media) / res10$media * 100, 2)
  else NA

  list(media = round(precio_actual, 2), anyo = anyo_actual, var10 = var10)
}

get_nucleos_censales <- function(ambito_val, i_id, m_id) {
  if (ambito_val == "localidad")
    return(list(yr = NA, tot = NA, n0 = NA, n1 = NA, n2 = NA, n3 = NA))

  sql_base <- "SELECT SUM(hogares_0) h0, SUM(hogares_1) h1, SUM(hogares_2) h2, SUM(hogares_3) h3, MAX(year) yr
               FROM nucleos_censales"
  res <- if (ambito_val == "canarias")
    dbGetQuery(con, sql_base)
  else if (ambito_val == "isla")
    dbGetQuery(con, paste(sql_base, "WHERE isla_id = $1"),     params = list(as.integer(i_id)))
  else
    dbGetQuery(con, paste(sql_base, "WHERE municipio_id = $1"), params = list(as.integer(m_id)))

  if (nrow(res) > 0 && !is.na(res$yr))
    list(yr  = res$yr,
         tot = res$h0 + res$h1 + res$h2 + res$h3,
         n0  = as.integer(res$h0), n1 = as.integer(res$h1),
         n2  = as.integer(res$h2), n3 = as.integer(res$h3))
  else
    list(yr = NA, tot = NA, n0 = NA, n1 = NA, n2 = NA, n3 = NA)
}

get_ext_viviendas <- function(ambito_val, i_id, m_id) {
  if (ambito_val == "localidad") return(list(sup = NA, tot = 0, vac = 0, esp = 0, hab = 0, yr = NA))

  filtros <- if (ambito_val == "canarias") "" else
             if (ambito_val == "isla")     "AND isla_id=$2" else "AND municipio_id=$2"
  p <- if (ambito_val == "canarias") list(ambito_val) else
       list(ambito_val, if (ambito_val == "isla") as.integer(i_id) else as.integer(m_id))

  sup <- dbGetQuery(con, paste0(
    "SELECT superficie FROM superficies WHERE ambito=$1 ", filtros), params = p)
  viv <- dbGetQuery(con, paste0(
    "SELECT total, vacias, esporadicas, habituales, year FROM viviendas_municipios WHERE ambito=$1 ", filtros), params = p)

  list(
    sup = if (nrow(sup) > 0) sup$superficie / 100 else NA,
    tot = if (nrow(viv) > 0) viv$total       else 0,
    vac = if (nrow(viv) > 0) viv$vacias      else 0,
    esp = if (nrow(viv) > 0) viv$esporadicas else 0,
    hab = if (nrow(viv) > 0) viv$habituales  else 0,
    yr  = if (nrow(viv) > 0) viv$year        else NA
  )
}

capturar <- function(ambito, i_id, m_id, l_id, f_p, nom) {
  o_vv  <- get_oferta_dinamica("alojamientos", ambito, i_id, m_id, l_id, f_p, "VV")
  o_ar  <- get_oferta_dinamica("alojamientos", ambito, i_id, m_id, l_id, f_p, "AR")
  m_dat <- get_datos_maestros(ambito, i_id, m_id, f_p)
  ext   <- get_ext_viviendas(ambito, i_id, m_id)
  hog   <- get_hogares_limitado(ambito, i_id, m_id)
  nuc   <- get_nucleos_censales(ambito, i_id, m_id)
  alq   <- get_serpavi(ambito, i_id, m_id, f_p)

  viv_efectivas <- if (ambito == "canarias") ext$hab + terminadas_post2021 else ext$hab
  tamanio_actual <- if (ambito != "localidad" && !is.na(viv_efectivas) && viv_efectivas > 0)
    round(m_dat$pob / viv_efectivas, 2) else NA_real_

  data.frame(
    ambito        = ambito,
    isla_id       = as.integer(i_id),
    municipio_id  = as.integer(m_id),
    localidad_id  = as.integer(l_id),
    etiqueta      = nom,
    fecha_calculo = f_p,
    superficie_km2        = ext$sup,
    poblacion             = m_dat$pob,
    poblacion_year        = m_dat$pob_y,
    viviendas_total       = ext$tot,
    viviendas_vacias      = ext$vac,
    viviendas_esporadicas = ext$esp,
    viviendas_habituales  = ext$hab,
    viviendas_year        = ext$yr,
    pte_r                 = m_dat$ptr,
    pte_r_year            = m_dat$ptr_y,
    pte_v                 = m_dat$ptv,
    pte_v_periodo         = m_dat$ptv_p,
    personas_por_hogar      = hog$miembros,
    personas_por_hogar_year = hog$year,
    uds_vv_turisticas      = o_vv$est_t,
    uds_vv_residenciales   = o_vv$est_r,
    plazas_vv_turisticas   = o_vv$pla_t,
    plazas_vv_residenciales = o_vv$pla_r,
    plazas_at_turisticas   = o_ar$pla_t,
    plazas_at_residenciales = o_ar$pla_r,
    year_nucleos_censales  = nuc$yr,
    hogares_total          = nuc$tot,
    hogares_0              = nuc$n0,
    hogares_1              = nuc$n1,
    hogares_2              = nuc$n2,
    hogares_3              = nuc$n3,
    alq_m2_media           = alq$media,
    alq_m2_year            = alq$anyo,
    alq_m2_variacion_10a   = alq$var10,
    tamanio_hogar_actual   = tamanio_actual
  )
}

# --- VIVIENDAS TERMINADAS POST-CENSO (solo Canarias ES70) ---
# Corrección sobre viviendas_habituales del Censo 2021 para el cálculo de tamanio_hogar_actual.
# Isla y municipio no tienen desglose disponible; usan viviendas_habituales del censo directamente.
terminadas_post2021 <- dbGetQuery(con, "
  SELECT COALESCE(SUM(viviendas_terminadas), 0) AS total
  FROM vivienda_iniciada_terminada_canarias
  WHERE territorio_codigo = 'ES70' AND tipo_periodo = 'anual'
    AND year > 2021 AND year <= $1",
  params = list(as.integer(format(fecha_proceso, "%Y"))))$total
cat(sprintf("Viviendas terminadas post-Censo 2021 (Canarias ES70): %d\n", as.integer(terminadas_post2021)))

# --- FACTORES DE CORRECCIÓN PTEv (FRONTUR-based) ---
# Para Canarias e islas: reemplaza pte_v por PTEv_real = PTEt(FRONTUR×EGT/365) - PTEr(reglado)
# Para municipios: escala pte_v proporcional al PTEv_real de su isla, conservando
# la distribución relativa del ISTAC y garantizando que la suma municipal = isla.
# El Hierro y La Gomera (sin FRONTUR) conservan pte_v del ISTAC sin corrección.

y_corte <- as.numeric(format(fecha_proceso, "%Y"))
m_corte <- as.numeric(format(fecha_proceso, "%m"))

pte_v_real_islas <- dbGetQuery(con, "
  WITH
  pte_t AS (
    SELECT f.year, f.isla_id, f.turistas * e.estancia_media / 365.0 AS pte_t
    FROM (SELECT year, isla_id, SUM(turistas) AS turistas
          FROM frontur_turistas WHERE ambito = 'isla' GROUP BY year, isla_id) f
    JOIN egt_estancia_media e ON e.isla_id = f.isla_id AND e.year = f.year
  ),
  pte_r AS (
    SELECT p.ejercicio AS year, p.isla_id, p.plazas * t.tasa / 100.0 AS pte_r
    FROM historico_plazas_regladas p
    JOIN historico_tasa_ocupacion_reglada t
      ON t.ejercicio = p.ejercicio AND t.ambito = p.ambito
      AND COALESCE(t.isla_id, 0) = COALESCE(p.isla_id, 0)
    WHERE p.ambito = 'isla'
  ),
  pte_v_real AS (
    SELECT t.year, t.isla_id, GREATEST(t.pte_t - r.pte_r, 0) AS pte_v_real
    FROM pte_t t JOIN pte_r r USING (year, isla_id)
  ),
  sistema AS (
    SELECT isla_id, AVG(ptev) AS pte_v_sistema
    FROM pte_vacacional
    WHERE ambito = 'isla'
      AND make_date(year, mes, 1) <= make_date($1, $2, 1)
      AND make_date(year, mes, 1) >  make_date($1, $2, 1) - INTERVAL '12 months'
    GROUP BY isla_id
  ),
  ultimo AS (
    SELECT isla_id, MAX(year) AS year FROM pte_v_real WHERE year <= $1 GROUP BY isla_id
  )
  SELECT s.isla_id, u.year AS year_ref, v.pte_v_real, s.pte_v_sistema,
         v.pte_v_real / NULLIF(s.pte_v_sistema, 0) AS factor
  FROM sistema s
  JOIN ultimo u USING (isla_id)
  JOIN pte_v_real v ON v.isla_id = s.isla_id AND v.year = u.year
", params = list(as.integer(y_corte), as.integer(m_corte)))

pte_v_real_can <- dbGetQuery(con, "
  WITH
  pte_t AS (
    SELECT f.year, f.turistas * e.estancia_media / 365.0 AS pte_t
    FROM (SELECT year, SUM(turistas) AS turistas
          FROM frontur_turistas WHERE ambito = 'canarias' GROUP BY year) f
    JOIN egt_estancia_media e ON e.ambito = 'canarias' AND e.year = f.year
  ),
  pte_r AS (
    SELECT p.ejercicio AS year, p.plazas * t.tasa / 100.0 AS pte_r
    FROM historico_plazas_regladas p
    JOIN historico_tasa_ocupacion_reglada t
      ON t.ejercicio = p.ejercicio AND t.ambito = p.ambito
      AND COALESCE(t.isla_id, 0) = 0
    WHERE p.ambito = 'canarias'
  )
  SELECT t.year, GREATEST(t.pte_t - r.pte_r, 0) AS pte_v_real
  FROM pte_t t JOIN pte_r r USING (year)
  WHERE t.year <= $1 ORDER BY t.year DESC LIMIT 1
", params = list(as.integer(y_corte)))

# Vectores de consulta rápida por isla_id
pte_v_real_vec  <- setNames(pte_v_real_islas$pte_v_real,  pte_v_real_islas$isla_id)
factor_vec      <- setNames(pte_v_real_islas$factor,       pte_v_real_islas$isla_id)

cat("Factores de corrección PTEv por isla:\n")
print(pte_v_real_islas[, c("isla_id", "year_ref", "pte_v_real", "pte_v_sistema", "factor")])
cat("\n")

# --- PROCESAMIENTO ---

invisible(dbExecute(con, "TRUNCATE TABLE base_snapshots RESTART IDENTITY"))
lista_base <- list()

cat("1/4 Canarias...\n")
d_can <- capturar("canarias", NA, NA, NA, fecha_proceso, "Canarias")
if (nrow(pte_v_real_can) > 0) d_can$pte_v <- pte_v_real_can$pte_v_real[1]
d_can$tipo_municipio <- "General"
d_can$tipo_isla      <- "General"
d_can$etiqueta_ambito_superior <- NA
lista_base[[1]] <- d_can

cat("2/4 Islas...\n")
islas_ref <- dbGetQuery(con, "SELECT id, nombre, tipo_isla FROM islas")
for (i in 1:nrow(islas_ref)) {
  d <- capturar("isla", islas_ref$id[i], NA, NA, fecha_proceso, islas_ref$nombre[i])
  iid <- as.character(islas_ref$id[i])
  if (!is.na(pte_v_real_vec[iid])) d$pte_v <- pte_v_real_vec[iid]
  d$tipo_municipio <- "General"
  d$tipo_isla      <- islas_ref$tipo_isla[i]
  d$etiqueta_ambito_superior <- "Canarias"
  lista_base[[length(lista_base) + 1]] <- d
}

cat("3/4 Municipios...\n")
muni_ref <- dbGetQuery(con, "
  SELECT m.id, m.isla_id, m.nombre, m.tipo_municipio, i.nombre AS nombre_isla
  FROM municipios m JOIN islas i ON m.isla_id = i.id")
for (i in 1:nrow(muni_ref)) {
  d <- capturar("municipio", muni_ref$isla_id[i], muni_ref$id[i], NA, fecha_proceso, muni_ref$nombre[i])
  iid <- as.character(muni_ref$isla_id[i])
  f <- factor_vec[iid]
  if (!is.na(f) && f > 0) d$pte_v <- d$pte_v * f
  d$tipo_municipio <- muni_ref$tipo_municipio[i]
  d$tipo_isla      <- "General"
  d$etiqueta_ambito_superior <- muni_ref$nombre_isla[i]
  lista_base[[length(lista_base) + 1]] <- d
}

cat("4/4 Localidades...\n")
loc_ref <- dbGetQuery(con, "
  SELECT l.id AS l_id, l.nombre, m.id AS m_id, m.isla_id AS i_id,
         m.tipo_municipio, m.nombre AS nombre_muni
  FROM localidades l JOIN municipios m ON l.municipio_id = m.id")
for (i in 1:nrow(loc_ref)) {
  d <- capturar("localidad", loc_ref$i_id[i], loc_ref$m_id[i], loc_ref$l_id[i], fecha_proceso, loc_ref$nombre[i])
  d$tipo_municipio <- loc_ref$tipo_municipio[i]
  d$tipo_isla      <- "General"
  d$etiqueta_ambito_superior <- loc_ref$nombre_muni[i]
  lista_base[[length(lista_base) + 1]] <- d
}

df_base <- bind_rows(lista_base)
dbWriteTable(con, "base_snapshots", df_base, append = TRUE, row.names = FALSE)

escribir_log("PT01_FIN", paste(
  "fecha_proceso:", fecha_input,
  "| filas:", nrow(df_base)))

dbDisconnect(con)
cat("\n✓ PT01 completado —", nrow(df_base), "filas en base_snapshots.\n")
