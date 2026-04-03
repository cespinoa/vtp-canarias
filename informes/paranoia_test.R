#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: paranoia_test.R
# Validación independiente: recalcula manualmente con SQL directo para
# Canarias, Lanzarote (isla_id=6) y Arrecife (municipio_id=28), y compara
# contra full_snapshots campo a campo.
#
# No usa ninguna función del pipeline. Las queries son independientes.
#
# Uso:
#   Rscript informes/paranoia_test.R
#   Rscript informes/paranoia_test.R 2025-12-31
# ==============================================================================

library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("PARANOIA TEST — Validación independiente\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

# --- FECHA ---
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  fecha <- args[1]
} else {
  fecha <- as.character(dbGetQuery(con,
    "SELECT MAX(fecha_calculo)::date AS f FROM full_snapshots")$f)
}
cat("Fecha de validación:", fecha, "\n\n")
y <- as.integer(format(as.Date(fecha), "%Y"))
m <- as.integer(format(as.Date(fecha), "%m"))

# ==============================================================================
# FUNCIÓN: imprimir tabla de comparación
# ==============================================================================
imprimir_tabla <- function(titulo, pares) {
  W <- 100
  cat("\n", strrep("=", W), "\n", sep = "")
  cat(sprintf("  %s\n", titulo))
  cat(strrep("-", W), "\n")
  cat(sprintf("  %-52s %18s %18s   %s\n", "CAMPO", "CALCULADO", "SNAPSHOT", "ESTADO"))
  cat(strrep("-", W), "\n")

  n_ok  <- 0L
  n_err <- 0L
  n_na  <- 0L

  for (p in pares) {
    campo <- p[[1]]
    calc  <- p[[2]]
    snap  <- p[[3]]

    # Normalizar NaN/Inf → NA
    if (!is.na(calc) && !is.finite(calc)) calc <- NA_real_
    if (!is.na(snap) && !is.finite(snap)) snap <- NA_real_

    if (is.na(calc) && is.na(snap)) {
      estado <- "  ----  "; n_na  <- n_na  + 1L
    } else if (is.na(calc) && !is.na(snap) && abs(snap) < 1) {
      # NA calculado → 0 en snapshot: es la coerción esperada (campos enteros NA → 0)
      estado <- "  NA→0  "; n_na  <- n_na  + 1L
    } else if (is.na(calc) || is.na(snap)) {
      estado <- "[ ERR ] "; n_err <- n_err + 1L
    } else if (abs(calc - snap) < 1.0) {
      estado <- "[ OK ]  "; n_ok  <- n_ok  + 1L
    } else {
      estado <- "[ ERR ] "; n_err <- n_err + 1L
    }

    c_str <- if (is.na(calc)) sprintf("%18s", "NA") else sprintf("%18.2f", calc)
    s_str <- if (is.na(snap)) sprintf("%18s", "NA") else sprintf("%18.2f", snap)
    cat(sprintf("  %-52s %s %s   %s\n", campo, c_str, s_str, estado))
  }

  cat(strrep("-", W), "\n")
  cat(sprintf("  Resultado: %d OK  |  %d NA/esperado  |  %d ERROR\n",
              n_ok, n_na, n_err))
  cat(strrep("=", W), "\n")
  invisible(n_err)
}

# ==============================================================================
# FUNCIÓN PRINCIPAL DE VERIFICACIÓN
# ==============================================================================
verificar <- function(ambito, isla_id, muni_id, titulo) {

  cat("\nExtraendo datos brutos para:", titulo, "...\n")

  # ---- Filtros ----------------------------------------------------------------
  fa <- switch(ambito,
    canarias  = "1=1",
    isla      = paste0("isla_id = ", isla_id),
    municipio = paste0("municipio_id = ", muni_id))

  fp <- switch(ambito,
    canarias  = "",
    isla      = paste0(" AND isla_id = ", isla_id, " AND municipio_id IS NULL"),
    municipio = paste0(" AND municipio_id = ", muni_id))

  fm <- switch(ambito,
    canarias  = "",
    isla      = paste0(" AND isla_id = ", isla_id),
    municipio = paste0(" AND municipio_id = ", muni_id))

  # ---- Alojamientos -----------------------------------------------------------
  vv <- dbGetQuery(con, paste0("
    SELECT
      COUNT(*)::int AS uds_vv_total,
      SUM(CASE WHEN en_area_turistica     THEN 1 ELSE 0 END)::int AS uds_t,
      SUM(CASE WHEN NOT en_area_turistica THEN 1 ELSE 0 END)::int AS uds_r,
      COALESCE(SUM(CASE WHEN en_area_turistica     THEN plazas END), 0)::int AS plz_t,
      COALESCE(SUM(CASE WHEN NOT en_area_turistica THEN plazas END), 0)::int AS plz_r
    FROM alojamientos
    WHERE ", fa, " AND tipo_oferta = 'VV'
      AND fecha_alta <= '", fecha, "'
      AND (fecha_baja IS NULL OR fecha_baja > '", fecha, "')"))

  at_ <- dbGetQuery(con, paste0("
    SELECT
      COALESCE(SUM(CASE WHEN en_area_turistica     THEN plazas END), 0)::int AS plz_t,
      COALESCE(SUM(CASE WHEN NOT en_area_turistica THEN plazas END), 0)::int AS plz_r
    FROM alojamientos
    WHERE ", fa, " AND tipo_oferta = 'AR'
      AND fecha_alta <= '", fecha, "'
      AND (fecha_baja IS NULL OR fecha_baja > '", fecha, "')"))

  # ---- PTE --------------------------------------------------------------------
  pter <- dbGetQuery(con, paste0("
    SELECT pte_reglada FROM pte_reglada
    WHERE ambito = '", ambito, "'", fp,
    " AND year <= ", y, " ORDER BY year DESC LIMIT 1"))

  ptev <- dbGetQuery(con, paste0("
    SELECT ptev FROM pte_vacacional
    WHERE ambito = '", ambito, "'", fp,
    " AND (year < ", y, " OR (year = ", y, " AND mes <= ", m, "))",
    " ORDER BY year DESC, mes DESC LIMIT 1"))

  # ---- Población --------------------------------------------------------------
  pob <- dbGetQuery(con, paste0("
    SELECT valor FROM poblacion
    WHERE ambito = '", ambito, "'", fm,
    " AND year <= ", y, " ORDER BY year DESC LIMIT 1"))

  # ---- Viviendas y superficie -------------------------------------------------
  viv <- dbGetQuery(con, paste0("
    SELECT total, vacias, esporadicas, habituales
    FROM viviendas_municipios WHERE ambito = '", ambito, "'", fm))

  sup <- dbGetQuery(con, paste0("
    SELECT superficie FROM superficies WHERE ambito = '", ambito, "'", fm))

  # ---- Hogares (canarias, isla y municipio) -----------------------------------
  hog <- switch(ambito,
    canarias  = dbGetQuery(con,
      "SELECT miembros FROM hogares WHERE ambito = 'canarias' ORDER BY year DESC LIMIT 1"),
    isla      = dbGetQuery(con, paste0(
      "SELECT miembros FROM hogares WHERE ambito = 'isla' AND isla_id = ", isla_id,
      " ORDER BY year DESC LIMIT 1")),
    municipio = dbGetQuery(con, paste0(
      "SELECT miembros FROM hogares WHERE ambito = 'municipio' AND municipio_id = ", muni_id,
      " ORDER BY year DESC LIMIT 1")),
    data.frame(miembros = NA_real_)
  )

  # ---- Valores base -----------------------------------------------------------
  uds_vv_t  <- vv$uds_t
  uds_vv_r  <- vv$uds_r
  plz_vv_t  <- vv$plz_t
  plz_vv_r  <- vv$plz_r
  plz_at_t  <- at_$plz_t
  plz_at_r  <- at_$plz_r
  pte_r_val <- if (nrow(pter) > 0) pter$pte_reglada      else 0
  pte_v_val <- if (nrow(ptev) > 0) ptev$ptev              else 0
  pob_val   <- if (nrow(pob) > 0)  pob$valor              else 0
  viv_tot   <- if (nrow(viv) > 0)  viv$total              else 0
  viv_vac   <- if (nrow(viv) > 0)  viv$vacias             else 0
  viv_esp   <- if (nrow(viv) > 0)  viv$esporadicas        else 0
  viv_hab   <- if (nrow(viv) > 0)  viv$habituales         else 0
  sup_km2   <- if (nrow(sup) > 0)  sup$superficie / 100   else NA_real_
  pphogar   <- if (nrow(hog) > 0 && !is.na(hog$miembros[1])) hog$miembros[1] else NA_real_

  # ---- Derivados (mismo orden que el diccionario) -----------------------------
  pte_total_c        <- pte_r_val + pte_v_val
  uds_vv_total_c     <- uds_vv_t + uds_vv_r
  plazas_vac_c       <- plz_vv_r + plz_vv_t
  plazas_reg_c       <- plz_at_r + plz_at_t
  plazas_tot_c       <- plazas_vac_c + plazas_reg_c
  plazas_s_res_c     <- plz_vv_r + plz_at_r
  plazas_s_tur_c     <- plz_vv_t + plz_at_t
  viv_disp_c         <- viv_hab - uds_vv_r
  viv_nec_c          <- pob_val / pphogar            # NA para municipios
  viv_disp_hab_c     <- viv_disp_c / viv_hab * 100
  deficit_viv_c      <- viv_disp_c - viv_nec_c      # NA si viv_nec es NA
  consumidores_c     <- pob_val + pte_v_val
  pob_total_c        <- pob_val + pte_total_c
  vhab_hab_c         <- viv_hab / pob_val * 100
  vhab_cons_c        <- viv_hab / consumidores_c * 100
  cobertura_c        <- viv_disp_c / viv_nec_c * 100 # NA si viv_nec es NA
  rit_r_c            <- pte_r_val / pob_val * 100
  rit_v_c            <- pte_v_val / pob_val * 100
  rit_c              <- pte_total_c / pob_val * 100
  rit_r_km2_c        <- pte_r_val / sup_km2
  rit_v_km2_c        <- pte_v_val / sup_km2
  rit_km2_c          <- pte_total_c / sup_km2
  res_km2_c          <- pob_val / sup_km2
  pres_hum_km2_c     <- pob_total_c / sup_km2
  vac_vhab_c         <- uds_vv_r / viv_hab * 100
  vac_vtot_c         <- uds_vv_total_c / viv_tot * 100
  uds_vv_hab_c       <- uds_vv_total_c / pob_val * 100
  uds_vv_res_hab_c   <- uds_vv_r / pob_val * 100
  deficit_oferta_c   <- (viv_nec_c - viv_disp_c) / viv_disp_c * 100
  uds_vv_res_porc_c  <- uds_vv_r / uds_vv_total_c * 100
  uds_vv_tur_porc_c  <- uds_vv_t / uds_vv_total_c * 100
  plz_vv_res_porc_c  <- plz_vv_r / plazas_vac_c * 100
  plz_vv_tur_porc_c  <- plz_vv_t / plazas_vac_c * 100
  plz_at_res_porc_c  <- plz_at_r / plazas_reg_c * 100
  plz_at_tur_porc_c  <- plz_at_t / plazas_reg_c * 100
  pte_v_porc_c       <- pte_v_val / pte_total_c * 100
  pte_r_porc_c       <- pte_r_val / pte_total_c * 100
  pte_v_totpob_c     <- pte_v_val / (pte_total_c + pob_val) * 100
  pte_r_totpob_c     <- pte_r_val / (pte_total_c + pob_val) * 100
  pob_totpob_c       <- pob_val / pob_total_c * 100
  rit_r_porc_c       <- rit_r_c / rit_c * 100
  rit_v_porc_c       <- rit_v_c / rit_c * 100
  viv_vac_tot_c      <- viv_vac / viv_tot * 100
  viv_esp_tot_c      <- viv_esp / viv_tot * 100
  viv_hab_tot_c      <- viv_hab / viv_tot * 100
  plz_vac_tot_c      <- plazas_vac_c / plazas_tot_c * 100
  plz_reg_tot_c      <- plazas_reg_c / plazas_tot_c * 100
  rit_km2_pres_c     <- rit_km2_c / pres_hum_km2_c * 100
  res_km2_pres_c     <- res_km2_c / pres_hum_km2_c * 100
  plz_at_res_res_c   <- plz_at_r / plazas_s_res_c * 100
  plz_vv_res_res_c   <- plz_vv_r / plazas_s_res_c * 100
  plz_at_tur_tur_c   <- plz_at_t / plazas_s_tur_c * 100
  plz_vv_tur_tur_c   <- plz_vv_t / plazas_s_tur_c * 100
  plz_res_tot_c      <- plazas_s_res_c / plazas_tot_c * 100
  plz_tur_tot_c      <- plazas_s_tur_c / plazas_tot_c * 100

  # ---- Snapshot ---------------------------------------------------------------
  sf <- switch(ambito,
    canarias  = "ambito = 'canarias'",
    isla      = paste0("ambito = 'isla' AND isla_id = ", isla_id),
    municipio = paste0("ambito = 'municipio' AND municipio_id = ", muni_id))

  sn <- dbGetQuery(con, paste0(
    "SELECT * FROM full_snapshots WHERE ", sf,
    " AND fecha_calculo = '", fecha, " 00:00:00' LIMIT 1"))

  if (nrow(sn) == 0) {
    cat("ERROR: No se encontró snapshot para", titulo, "\n")
    return(invisible(99L))
  }

  # ---- Pares de comparación ---------------------------------------------------
  pares <- list(
    # ── Datos base ──────────────────────────────────────────────────────────────
    list("uds_vv_turisticas",       uds_vv_t,       sn$uds_vv_turisticas),
    list("uds_vv_residenciales",    uds_vv_r,       sn$uds_vv_residenciales),
    list("plazas_vv_turisticas",    plz_vv_t,       sn$plazas_vv_turisticas),
    list("plazas_vv_residenciales", plz_vv_r,       sn$plazas_vv_residenciales),
    list("plazas_at_turisticas",    plz_at_t,       sn$plazas_at_turisticas),
    list("plazas_at_residenciales", plz_at_r,       sn$plazas_at_residenciales),
    list("pte_r",                   pte_r_val,      sn$pte_r),
    list("pte_v",                   pte_v_val,      sn$pte_v),
    list("poblacion",               pob_val,        sn$poblacion),
    list("viviendas_total",         viv_tot,        sn$viviendas_total),
    list("viviendas_vacias",        viv_vac,        sn$viviendas_vacias),
    list("viviendas_esporadicas",   viv_esp,        sn$viviendas_esporadicas),
    list("viviendas_habituales",    viv_hab,        sn$viviendas_habituales),
    list("superficie_km2",          sup_km2,        sn$superficie_km2),
    list("personas_por_hogar",      pphogar,        sn$personas_por_hogar),
    # ── Derivados ───────────────────────────────────────────────────────────────
    list("pte_total",               pte_total_c,    sn$pte_total),
    list("uds_vv_total",            uds_vv_total_c, sn$uds_vv_total),
    list("plazas_vacacionales",     plazas_vac_c,   sn$plazas_vacacionales),
    list("plazas_regladas",         plazas_reg_c,   sn$plazas_regladas),
    list("plazas_total",            plazas_tot_c,   sn$plazas_total),
    list("plazas_suelo_residencial", plazas_s_res_c, sn$plazas_suelo_residencial),
    list("plazas_suelo_turistico",  plazas_s_tur_c, sn$plazas_suelo_turistico),
    list("viviendas_disponibles",   viv_disp_c,     sn$viviendas_disponibles),
    list("viviendas_necesarias",    viv_nec_c,      sn$viviendas_necesarias),
    list("viviendas_disp_hab_%",    viv_disp_hab_c, sn$viviendas_disponibles_viviendas_habituales),
    list("deficit_viviendas",       deficit_viv_c,  sn$deficit_viviendas),
    list("consumidores_vivienda",   consumidores_c, sn$consumidores_vivienda),
    list("poblacion_total",         pob_total_c,    sn$poblacion_total),
    list("vhabituales_habitantes",  vhab_hab_c,     sn$vhabituales_habitantes),
    list("vhabituales_consumidores", vhab_cons_c,   sn$vhabituales_consumidores),
    list("cobertura_demanda_viviendas", cobertura_c, sn$cobertura_demanda_viviendas),
    list("rit_r",                   rit_r_c,        sn$rit_r),
    list("rit_v",                   rit_v_c,        sn$rit_v),
    list("rit",                     rit_c,          sn$rit),
    list("rit_r_km2",               rit_r_km2_c,    sn$rit_r_km2),
    list("rit_v_km2",               rit_v_km2_c,    sn$rit_v_km2),
    list("rit_km2",                 rit_km2_c,      sn$rit_km2),
    list("residentes_km2",          res_km2_c,      sn$residentes_km2),
    list("presion_humana_km2",      pres_hum_km2_c, sn$presion_humana_km2),
    list("vacacional_por_vhab",     vac_vhab_c,     sn$vacacional_por_viviendas_habituales),
    list("vacacional_por_vtotal",   vac_vtot_c,     sn$vacacional_por_viviendas_total),
    list("uds_vv_habitantes",       uds_vv_hab_c,   sn$uds_vv_habitantes),
    list("uds_vv_residenciales_hab", uds_vv_res_hab_c, sn$uds_vv_residenciales_habitantes),
    list("deficit_oferta_viviendas", deficit_oferta_c, sn$deficit_oferta_viviendas),
    list("uds_vv_residenciales_%",  uds_vv_res_porc_c, sn$uds_vv_residenciales_porc),
    list("uds_vv_turisticas_%",     uds_vv_tur_porc_c, sn$uds_vv_turisticas_porc),
    list("plazas_vv_residenciales_%", plz_vv_res_porc_c, sn$plazas_vv_residenciales_porc),
    list("plazas_vv_turisticas_%",  plz_vv_tur_porc_c, sn$plazas_vv_turisticas_porc),
    list("plazas_at_residenciales_%", plz_at_res_porc_c, sn$plazas_at_residenciales_porc),
    list("plazas_at_turisticas_%",  plz_at_tur_porc_c, sn$plazas_at_turisticas_porc),
    list("pte_v_porc",              pte_v_porc_c,   sn$pte_v_porc),
    list("pte_r_porc",              pte_r_porc_c,   sn$pte_r_porc),
    list("pte_v_total_poblacion_%", pte_v_totpob_c, sn$pte_v_total_poblacion_porc),
    list("pte_r_total_poblacion_%", pte_r_totpob_c, sn$pte_r_total_poblacion_porc),
    list("poblacion_total_%",       pob_totpob_c,   sn$poblacion_total_poblacion_porc),
    list("rit_r_porc",              rit_r_porc_c,   sn$rit_r_porc),
    list("rit_v_porc",              rit_v_porc_c,   sn$rit_v_porc),
    list("viviendas_vacias_%",      viv_vac_tot_c,  sn$viviendas_vacias_viviendas_total),
    list("viviendas_esporadicas_%", viv_esp_tot_c,  sn$viviendas_esporadicas_viviendas_total),
    list("viviendas_habituales_%",  viv_hab_tot_c,  sn$viviendas_habituales_viviendas_total),
    list("plazas_vacacionales_%",   plz_vac_tot_c,  sn$plazas_vacacionales_plazas_total_porc),
    list("plazas_regladas_%",       plz_reg_tot_c,  sn$plazas_regladas_plazas_total_porc),
    list("rit_km2/presion_%",       rit_km2_pres_c, sn$rit_km2_presion_humana_km2),
    list("residentes_km2/presion_%", res_km2_pres_c, sn$residentes_km2_presion_humana_km2),
    list("plz_at_res/suelo_res_%",  plz_at_res_res_c, sn$plazas_at_residenciales_oferta_en_residencial),
    list("plz_vv_res/suelo_res_%",  plz_vv_res_res_c, sn$plazas_vv_residenciales_oferta_en_residencial),
    list("plz_at_tur/suelo_tur_%",  plz_at_tur_tur_c, sn$plazas_at_turisticas_oferta_en_turistico),
    list("plz_vv_tur/suelo_tur_%",  plz_vv_tur_tur_c, sn$plazas_vv_turisticas_oferta_en_turistico),
    list("plazas_suelo_residencial_%", plz_res_tot_c, sn$plazas_suelo_residencial_porc),
    list("plazas_suelo_turistico_%", plz_tur_tot_c, sn$plazas_suelo_turistico_porc)
  )

  imprimir_tabla(titulo, pares)
}

# ==============================================================================
# EJECUCIÓN PARA LOS TRES ÁMBITOS
# ==============================================================================
errores_total <- 0L

errores_total <- errores_total + verificar("canarias",  NA, NA, "CANARIAS (total)")
errores_total <- errores_total + verificar("isla",       6, NA, "LANZAROTE (isla_id = 6)")
errores_total <- errores_total + verificar("municipio", NA, 28, "ARRECIFE (municipio_id = 28)")
errores_total <- errores_total + verificar("municipio", NA, 32, "TIAS (municipio_id = 32, con suelo turístico)")
errores_total <- errores_total + verificar("isla",       1, NA, "EL HIERRO (isla_id = 1, sin suelo turístico)")

cat("\n")
if (errores_total == 0L) {
  cat("RESULTADO GLOBAL: TODOS LOS CAMPOS VERIFICADOS CORRECTAMENTE.\n")
} else {
  cat(sprintf("RESULTADO GLOBAL: %d ERRORES DETECTADOS.\n", errores_total))
}

dbDisconnect(con)
cat("\n✓ paranoia_test completado.\n")
