library(DBI)
library(dplyr)
library(tidyr)
library(stringr)
library(knitr) # Para formatear tablas limpias

source("importar_gobcan/helper.R")
con <- conecta_db()

cat("--- RESUMEN DEL CENSO TURÍSTICO ANALIZADO ---\n\n")

# 1. CÁLCULO DE TOTALES Y MODALIDADES
resumen_censos <- dbGetQuery(con, "
  SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE modalidad_texto = 'Hotelera') as hoteles,
    COUNT(*) FILTER (WHERE tipologia_texto = 'Vivienda Vacacional') as vv,
    COUNT(*) FILTER (WHERE audit_nota LIKE '%COORDS_FUERA_RANGO%') as geo_erroneas
  FROM staging_import")

total <- resumen_censos$total
hoteles <- resumen_censos$hoteles
viviendas_vacacionales <- resumen_censos$vv
# Extrahoteleros sin VV = Total - (Hoteles + VV)
extrahoteleros_resto <- total - (hoteles + viviendas_vacacionales)

cat(paste0("- Total de establecimientos: ", total, "\n"))
cat(paste0("- Establecimientos hoteleros: ", hoteles, "\n"))
cat(paste0("- Viviendas vacacionales: ", viviendas_vacacionales, "\n"))
cat(paste0("- Establecimientos extrahoteleros (excl. VV): ", extrahoteleros_resto, "\n"))

# 2. DATOS ORIGINALES CON GEOERRONÉA
cat("\n--- CALIDAD DE ORIGEN (GOBIERNO DE CANARIAS) ---\n")
cat(paste0("- Registros con coordenadas fuera de rango (geo_erronea_gobcan): ", resumen_censos$geo_erroneas, "\n"))
cat("  *(Nota: Registros con coordenadas originales en el mar, fuera del archipiélago o en el desierto sahariano)*\n\n")

cat("--- GENERANDO INFORME DE CALIDAD GEOGRÁFICA ---\n\n")

# 1. ORIGEN DE LA GEOLOCALIZACIÓN
cat("### 1. Origen de la Geocodificación\n")
geo_origen <- dbGetQuery(con, "
  SELECT 
    COALESCE(fuente_geocodigo, 'No geocodificado') as fuente,
    COUNT(*) as registros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as porcentaje
  FROM staging_import
  GROUP BY 1 ORDER BY 2 DESC")
print(kable(geo_origen, format = "markdown"))

# 2. DISTANCIA FUZZY (Con ejemplos)
cat("\n### 2. Precisión de Cruce de Nombres (Fuzzy Distance)\n")
fuzzy_dist <- dbGetQuery(con, "
  SELECT 
    ROUND(distancia_fuzzy::numeric, 1) as dist_decimal,
    COUNT(*) as registros
  FROM staging_import
  WHERE distancia_fuzzy IS NOT NULL
  GROUP BY 1 ORDER BY 1 ASC")
print(kable(fuzzy_dist, format = "markdown"))

cat("\n### 2.1. Ejemplos representativos de concordancia Fuzzy\n")

# Consultamos ejemplos variados. 
# Filtramos para evitar nulos y ordenamos para obtener diversidad de distancias.
query_ejemplos <- "
WITH ejemplos_ordenados AS (
    SELECT 
        muni_nombre as municipio,
        direccion, 
        direccion_match, 
        ROUND(distancia_fuzzy::numeric, 1) as dist,
        ROW_NUMBER() OVER(PARTITION BY ROUND(distancia_fuzzy::numeric, 0) ORDER BY distancia_fuzzy DESC) as rnk
    FROM public.staging_import
    WHERE distancia_fuzzy IS NOT NULL 
      AND distancia_fuzzy > 0
      AND direccion_match IS NOT NULL
)
SELECT 
    municipio,
    direccion as direccion_fuente, 
    direccion_match as direccion_encontrada, 
    dist as distancia_fuzzy
FROM ejemplos_ordenados
WHERE rnk <= 3
ORDER BY dist ASC, municipio ASC
LIMIT 15; -- Traemos una muestra representativa de los primeros niveles
"

ejemplos_fuzzy <- dbGetQuery(con, query_ejemplos)

print(kable(ejemplos_fuzzy, format = "markdown", 
            col.names = c("Municipio", "Dirección Original (GobCan)", "Dirección Match (Callejero)", "Distancia Fuzzy")))

# 3. MÉTODO DE LOCALIDAD Y DISTANCIAS DE PROXIMIDAD
cat("\n### 3. Asignación de Localidades y Diseminados\n")

# Extraemos los datos necesarios
loc_data <- dbGetQuery(con, "
  SELECT 
    CASE 
      WHEN metodo_localidad = 'proximidad' THEN 'Por proximidad (Diseminados/Rústico)'
      ELSE 'Directa (Intersección)'
    END as metodo_texto,
    audit_nota
  FROM staging_import 
  WHERE estado = 'finalizado_geo'")

# Procesamiento con R
loc_resumen <- loc_data %>%
  # Extraer el número tras DIST_LOC:
  mutate(dist_m = as.numeric(str_extract(audit_nota, "(?<=DIST_LOC: )\\d+"))) %>%
  # Clasificar según la distancia extraída
  mutate(rango_proximidad = case_when(
    metodo_texto == 'Directa (Intersección)' ~ "En núcleo (0m)",
    is.na(dist_m) ~ "En núcleo (0m)", # Por si acaso
    dist_m <= 100 ~ "Muy cerca (<100m)",
    dist_m <= 500 ~ "Cerca (100-500m)",
    TRUE ~ "Alejado (>500m)"
  )) %>%
  # Agrupar y contar
  group_by(metodo_texto, rango_proximidad) %>%
  summarise(registros = n(), .groups = 'drop') %>%
  rename(Metodo = metodo_texto, Rango = rango_proximidad)

print(kable(loc_resumen, format = "markdown"))

# 4. INTEGRIDAD DE CAPACIDAD OPERATIVA
cat("\n### 4. Integridad de Capacidad Operativa\n")

plazas_audit <- dbGetQuery(con, "
  SELECT 
    CASE 
      WHEN plazas_estimadas = TRUE THEN 'Estimadas (Cálculo por media municipal)'
      ELSE 'Originales (Declaradas en Registro)' 
    END as origen_dato,
    COUNT(*) as registros,
    SUM(COALESCE(plazas, 0)) as total_plazas,
    ROUND(AVG(COALESCE(plazas, 0)), 1) as promedio_plazas
  FROM staging_import
  GROUP BY plazas_estimadas
  ORDER BY plazas_estimadas ASC")

print(kable(plazas_audit, format = "markdown", col.names = c("Origen del Dato", "Nº Registros", "Total Plazas", "Media Plazas/Est.")))

# 5. REASIGNACIÓN DE MUNICIPIOS
cat("\n### 5. Balance de Transferencias Municipales (Corrección de Errores Originales)\n")

query_transferencias <- "
WITH transferencias AS (
    SELECT 
        TRIM(UPPER(muni_nombre)) AS municipio_origen,
        TRIM(UPPER(muni_detectado_geo)) AS municipio_destino,
        COUNT(*) AS num_establecimientos,
        SUM(COALESCE(plazas, 0)) AS suma_plazas
    FROM public.staging_import
    WHERE audit_resultado = 'ERROR_DISCREPANCIA_MUNICIPAL'
      AND fuente_geocodigo = 'gobcan'
    GROUP BY 1, 2
),
perdidas AS (
    SELECT municipio_origen AS muni, SUM(num_establecimientos) AS est_p, SUM(suma_plazas) AS plz_p FROM transferencias GROUP BY 1
),
ganadas AS (
    SELECT municipio_destino AS muni, SUM(num_establecimientos) AS est_g, SUM(suma_plazas) AS plz_g FROM transferencias GROUP BY 1
),
universo AS (
    SELECT municipio_origen AS muni FROM transferencias UNION SELECT municipio_destino AS muni FROM transferencias
)
SELECT 
    u.muni AS municipio,
    COALESCE(g.est_g, 0) AS est_recibidos, 
    COALESCE(p.est_p, 0) AS est_cedidos,
    (COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)) AS saldo_est,
    COALESCE(g.plz_g, 0) AS plz_recibidas, 
    COALESCE(p.plz_p, 0) AS plz_cedidas,
    (COALESCE(g.plz_g, 0) - COALESCE(p.plz_p, 0)) AS saldo_plz
FROM universo u
LEFT JOIN ganadas g ON u.muni = g.muni
LEFT JOIN perdidas p ON u.muni = p.muni
ORDER BY ABS(COALESCE(g.est_g, 0) - COALESCE(p.est_p, 0)) DESC;" # Usamos la expresión completa aquí

transfer_res <- dbGetQuery(con, query_transferencias)
print(kable(transfer_res, format = "markdown", 
            col.names = c("Municipio", "Est. Recibidos", "Est. Cedidos", "Saldo Est.", "Plazas Recibidas", "Plazas Cedidas", "Saldo Plazas")))


# Opcional: Guardar el balance municipal a CSV para abrirlo directamente en Calc/Excel
write.csv(transfer_res, "balance_municipal_calidad.csv", row.names = FALSE)
cat("\n✅ Informe finalizado. El balance municipal se ha exportado a CSV.\n")


dbDisconnect(con)
