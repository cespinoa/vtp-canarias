# --- TEST DE PARANOIA: RECALCULO MANUAL ---


library(tidyverse)
library(DBI)
library(RPostgres)
library(dotenv)
library(optparse) # Para gestionar el parámetro de fecha desde terminal
library(dplyr)

# --- 0. CONEXIÓN ---
source("importar_gobcan/helper.R")
con <- conecta_db()

# ==============================================================================
# SCRIPT DE AUDITORÍA DE "FUERZA BRUTA" (VALIDACIÓN EXTERNA)
# ==============================================================================
# Propósito: Calcular manualmente sin usar las funciones del sistema
# para verificar la integridad de la tabla 'totales'.







# ==============================================================================
# TEST PARANOIA GLOBAL COMPLETO: VV + AT + PTE + VIVIENDAS + POB (ISLA_ID = 6)
# ==============================================================================

# 1. EXTRACCIÓN BRUTA VV (Viviendas Vacacionales)
bruto_vv <- dbGetQuery(con, "
  SELECT 
    COUNT(*)::integer as total,
    SUM(CASE WHEN en_area_turistica = TRUE THEN 1 ELSE 0 END)::integer as v_t,
    SUM(CASE WHEN en_area_turistica = FALSE THEN 1 ELSE 0 END)::integer as v_r,
    SUM(CASE WHEN en_area_turistica = TRUE THEN plazas ELSE 0 END)::numeric as p_t,
    SUM(CASE WHEN en_area_turistica = FALSE THEN plazas ELSE 0 END)::numeric as p_r
  FROM alojamientos
  WHERE isla_id = 6 AND tipo_oferta = 'VV'
    AND fecha_alta <= '2025-11-24' AND (fecha_baja IS NULL OR fecha_baja > '2025-11-24')
")

# 2. EXTRACCIÓN BRUTA AT (Apartamentos Turísticos)
bruto_at <- dbGetQuery(con, "
  SELECT 
    SUM(CASE WHEN en_area_turistica = TRUE THEN plazas ELSE 0 END)::numeric as p_t,
    SUM(CASE WHEN en_area_turistica = FALSE THEN plazas ELSE 0 END)::numeric as p_r
  FROM alojamientos
  WHERE isla_id = 6 AND tipo_oferta = 'AR' 
    AND fecha_alta <= '2025-11-24' AND (fecha_baja IS NULL OR fecha_baja > '2025-11-24')
")

# 3. EXTRACCIÓN MAESTROS PTE
bruto_pter <- dbGetQuery(con, "SELECT pte_reglada FROM pte_reglada WHERE isla_id = 6 AND municipio_id IS NULL ORDER BY year DESC LIMIT 1")$pte_reglada
bruto_ptev <- dbGetQuery(con, "SELECT ptev FROM pte_vacacional WHERE isla_id = 6 AND municipio_id IS NULL ORDER BY year DESC, mes DESC LIMIT 1")$ptev

# 4. EXTRACCIÓN VIVIENDAS (viviendas_municipios)
bruto_viv <- dbGetQuery(con, "
  SELECT total, vacias, esporadicas, habituales 
  FROM viviendas_municipios 
  WHERE ambito = 'isla' AND isla_id = 6 
  LIMIT 1
")

# 5. EXTRACCIÓN POBLACIÓN (Última actualización)
bruto_pob <- dbGetQuery(con, "
  SELECT valor 
  FROM poblacion 
  WHERE ambito = 'isla' AND isla_id = 6 
  ORDER BY year DESC LIMIT 1
")$valor

# 6. EXTRACCIÓN CONSOLIDADA (Tabla Totales)
consolidado <- dbGetQuery(con, "
  SELECT 
    vv_total, vv_turisticas, vv_residenciales, 
    plazas_vv_turisticas, plazas_vv_residenciales,
    plazas_at_turisticas, plazas_at_residenciales,
    pte_r, pte_v,
    viviendas_total, viviendas_vacias, viviendas_esporadicas, viviendas_habituales,
    poblacion
  FROM snapshots WHERE ambito = 'isla' AND isla_id = 6 AND fecha_calculo = '2025-11-24'
")

# 7. CONSTRUCCIÓN DEL INFORME UNIFICADO
conceptos <- c(
  "VV TOTALES (Nº)", "VV TURÍSTICAS (Nº)", "VV RESIDENCIALES (Nº)", 
  "PLAZAS VV TURIST.", "PLAZAS VV RESID.",
  "PLAZAS AT TURIST.", "PLAZAS AT RESID.",
  "PTE REGLADA (PTE_R)", "PTE VACACIONAL (PTE_V)",
  "VIV. TOTALES (Censo)", "VIV. VACÍAS", "VIV. ESPORÁDICAS", "VIV. HABITUALES",
  "POBLACIÓN TOTAL"
)

origen <- c(
  bruto_vv$total, bruto_vv$v_t, bruto_vv$v_r, 
  bruto_vv$p_t, bruto_vv$p_r,
  bruto_at$p_t, bruto_at$p_r,
  bruto_pter, bruto_ptev,
  bruto_viv$total, bruto_viv$vacias, bruto_viv$esporadicas, bruto_viv$habituales,
  bruto_pob
)

destino <- c(
  consolidado$vv_total, consolidado$vv_turisticas, consolidado$vv_residenciales, 
  consolidado$plazas_vv_turisticas, consolidado$plazas_vv_residenciales,
  consolidado$plazas_at_turisticas, consolidado$plazas_at_residenciales,
  consolidado$pte_r, consolidado$pte_v,
  consolidado$viviendas_total, consolidado$viviendas_vacias, consolidado$viviendas_esporadicas, consolidado$viviendas_habituales,
  consolidado$poblacion
)


informe_final <- data.frame(Concepto = conceptos, Origen_SQL = origen, Totales_BD = destino) %>%
  mutate(Dif = Origen_SQL - Totales_BD, Status = if_else(abs(Dif) < 0.1, "[ OK ]", "[ ERROR ]"))

# 8. SALIDA POR PANTALLA (Formato Ancho)
cat("\n", str_dup("=", 115), "\n")
cat(sprintf("%-25s | %18s | %18s | %12s | %s\n", "CONCEPTO", "ORIGEN (BRUTO)", "CONSOLIDADO (BD)", "DIF.", "ESTADO"))
cat(str_dup("-", 115), "\n")

for(i in 1:nrow(informe_final)) {
  cat(sprintf("%-25s | %18.2f | %18.2f | %12.2f | %s\n", 
              informe_final$Concepto[i], informe_final$Origen_SQL[i], 
              informe_final$Totales_BD[i], informe_final$Dif[i], informe_final$Status[i]))
}
cat(str_dup("=", 115), "\n")
