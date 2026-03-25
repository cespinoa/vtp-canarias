library(DBI)
library(dplyr)
library(tidyr)
library(stringr)
library(knitr)

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("--- INFORME FINAL DE AUDITORÍA Y CALIDAD DEL CENSO (Tabla Alojamientos) ---\n\n")

# 0. AUDITORÍA DE DUPLICADOS (NUEVO)
cat("### 0. Gestión de Registros Duplicados\n")
dups_query <- dbGetQuery(con, "
  SELECT 
    COUNT(*) FILTER (WHERE audit_nota LIKE '%duplicados_eliminados:[1-9]%') as establecimientos_con_duplicados,
    SUM(CAST(SUBSTRING(audit_nota FROM 'duplicados_eliminados:([0-9]+)') AS INTEGER)) as total_registros_descartados
  FROM alojamientos")

cat(paste0("- Establecimientos que presentaban duplicidad en origen: ", dups_query$establecimientos_con_duplicados, "\n"))
cat(paste0("- Total de registros 'basura' eliminados durante la migración: ", dups_query$total_registros_descartados, "\n\n"))

# 1. CÁLCULO DE TOTALES Y MODALIDADES
resumen_censos <- dbGetQuery(con, "
  SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE modalidad_original = 'Hotelera') as hoteles,
    COUNT(*) FILTER (WHERE tipologia_original = 'Vivienda Vacacional') as vv,
    COUNT(*) FILTER (WHERE audit_nota LIKE '%COORDS_FUERA_RANGO%') as geo_erroneas
  FROM alojamientos")

total <- resumen_censos$total
hoteles <- resumen_censos$hoteles
viviendas_vacacionales <- resumen_censos$vv
extrahoteleros_resto <- total - (hoteles + viviendas_vacacionales)

cat(paste0("- Total de establecimientos únicos cargados: ", total, "\n"))
cat(paste0("- Establecimientos hoteleros: ", hoteles, "\n"))
cat(paste0("- Viviendas vacacionales: ", viviendas_vacacionales, "\n"))
cat(paste0("- Establecimientos extrahoteleros (excl. VV): ", extrahoteleros_resto, "\n"))

# 2. CALIDAD DE ORIGEN
cat("\n--- CALIDAD DE ORIGEN (GOBIERNO DE CANARIAS) ---\n")
cat(paste0("- Registros con coordenadas originales erróneas (corregidos): ", resumen_censos$geo_erroneas, "\n"))

# 1. ORIGEN DE LA GEOCODIFICACIÓN
cat("\n### 1. Origen de la Geocodificación (Trazabilidad Final)\n")
geo_origen <- dbGetQuery(con, "
  SELECT 
    COALESCE(fuente_geocodigo, 'No geocodificado') as fuente,
    COUNT(*) as registros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
  FROM alojamientos
  GROUP BY 1 ORDER BY 2 DESC")
print(kable(geo_origen, format = "markdown"))

# 2. DISTANCIA FUZZY
cat("\n### 2. Precisión de Cruce de Nombres (Fuzzy Distance)\n")
fuzzy_dist <- dbGetQuery(con, "
  SELECT 
    ROUND(distancia_fuzzy::numeric, 1) as dist_decimal,
    COUNT(*) as registros
  FROM alojamientos
  WHERE distancia_fuzzy IS NOT NULL
  GROUP BY 1 ORDER BY 1 ASC")
print(kable(fuzzy_dist, format = "markdown"))

cat("\n### 2.1. Muestra de validación de direcciones (Original vs Match)\n")
query_ejemplos <- "
SELECT 
    muni_detectado_geo as municipio,
    direccion_original as fuente, 
    direccion_match as encontrado, 
    distancia_fuzzy as dist
FROM alojamientos
WHERE distancia_fuzzy > 0 AND direccion_match IS NOT NULL
ORDER BY distancia_fuzzy ASC
LIMIT 10;"

ejemplos_fuzzy <- dbGetQuery(con, query_ejemplos)
print(kable(ejemplos_fuzzy, format = "markdown"))

# 3. MÉTODO DE LOCALIDAD Y DISTANCIAS
cat("\n### 3. Distribución por Núcleos y Diseminados\n")
loc_data <- dbGetQuery(con, "SELECT metodo_localidad, audit_nota FROM alojamientos")

loc_resumen <- loc_data %>%
  mutate(dist_m = as.numeric(str_extract(audit_nota, "(?<=DIST_LOC: )\\d+"))) %>%
  mutate(rango_proximidad = case_when(
    metodo_localidad == 'interseccion' ~ "En núcleo (0m)",
    is.na(dist_m) ~ "En núcleo (0m)",
    dist_m <= 100 ~ "Muy cerca (<100m)",
    dist_m <= 500 ~ "Cerca (100-500m)",
    TRUE ~ "Alejado (>500m)"
  )) %>%
  group_by(Metodo = metodo_localidad, Rango = rango_proximidad) %>%
  summarise(Registros = n(), .groups = 'drop')

print(kable(loc_resumen, format = "markdown"))

# 4. CAPACIDAD OPERATIVA
cat("\n### 4. Integridad de Plazas y Capacidad\n")
plazas_audit <- dbGetQuery(con, "
  SELECT 
    CASE WHEN plazas_estimadas = TRUE THEN 'Estimadas (Media Municipal)' ELSE 'Oficiales (Registro)' END as origen,
    COUNT(*) as registros,
    SUM(plazas) as total_plazas,
    ROUND(AVG(plazas), 1) as promedio
  FROM alojamientos
  GROUP BY plazas_estimadas")
print(kable(plazas_audit, format = "markdown"))

# 5. BALANCE DE TRANSFERENCIAS (Versión Corregida)
cat("\n### 5. Balance de Correcciones Municipales (Trazabilidad Geográfica)\n")

query_transferencias <- "
WITH transferencias AS (
    SELECT 
        TRIM(UPPER(muni_original_gobcan)) AS municipio_origen,
        TRIM(UPPER(muni_detectado_geo)) AS municipio_destino,
        COUNT(*) AS n_est,
        SUM(plazas) AS s_plz
    FROM alojamientos
    WHERE muni_original_gobcan <> muni_detectado_geo
    GROUP BY 1, 2
),
universo AS (
    SELECT municipio_origen AS muni FROM transferencias 
    UNION 
    SELECT municipio_destino AS muni FROM transferencias
),
ganadas AS (
    SELECT municipio_destino as muni, SUM(n_est) as est_g, SUM(s_plz) as plz_g 
    FROM transferencias GROUP BY 1
),
perdidas AS (
    SELECT municipio_origen as muni, SUM(n_est) as est_p, SUM(s_plz) as plz_p 
    FROM transferencias GROUP BY 1
)
SELECT 
    u.muni as Municipio,
    COALESCE(g.est_g, 0) as Recibidos,
    COALESCE(p.est_p, 0) as Cedidos,
    (COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)) as Saldo_Est,
    COALESCE(g.plz_g, 0) as Plazas_Rec,
    (COALESCE(g.plz_g, 0) - COALESCE(p.plz_p, 0)) as Saldo_Plz
FROM universo u
LEFT JOIN ganadas g ON u.muni = g.muni
LEFT JOIN perdidas p ON u.muni = p.muni
WHERE (COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)) <> 0
ORDER BY ABS(COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)) DESC; -- Repetimos la expresión para el ORDER BY
"

transfer_res <- dbGetQuery(con, query_transferencias)

if(nrow(transfer_res) > 0) {
    print(kable(transfer_res, format = "markdown", 
          col.names = c("Municipio", "Est. Recibidos", "Est. Cedidos", "Saldo Est.", "Plazas Recibidas", "Saldo Plazas")))
          write.csv(transfer_res, "balance_municipal_calidad.csv", row.names = FALSE)
          cat("\n✅ Informe finalizado. El balance municipal se ha exportado a CSV.\n")
} else {
    cat("No se detectaron discrepancias municipales entre el origen y la geolocalización.\n")
}

dbDisconnect(con)
cat("\n✅ Auditoría finalizada sin errores de sintaxis.\n")
