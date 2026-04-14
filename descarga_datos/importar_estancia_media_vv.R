#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_estancia_media_vv.R
# Calcula el histórico anual de estancia media en viviendas vacacionales
# (VV) a partir de la tabla pte_vacacional y lo carga en
# historico_estancia_media_vv.
#
# Fuente primaria: pte_vacacional (ISTAC C00065A_000061)
# Cobertura: anual 2019–año más reciente en pte_vacacional. Canarias + 7 islas.
#
# Método de agregación: media ponderada por viviendas_reservadas.
#   La estancia_media mensual es la duración media de la estancia de las VV
#   ocupadas ese mes. Para obtener el dato anual se pondera cada mes por el
#   volumen de reservas (viviendas_reservadas), de modo que los meses con más
#   actividad turística tienen mayor peso.
#   Meses con viviendas_reservadas = 0 o NULL se excluyen del cálculo.
#
# Uso:
#   Rscript descarga_datos/importar_estancia_media_vv.R
#
# Estrategia de carga: TRUNCATE + reload completo.
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LEER pte_vacacional ---
cat("Leyendo pte_vacacional...\n")
pte <- dbGetQuery(con, "
  SELECT
    year,
    ambito,
    isla_id,
    estancia_media,
    viviendas_reservadas
  FROM pte_vacacional
  WHERE ambito IN ('canarias', 'isla')
    AND estancia_media IS NOT NULL
    AND viviendas_reservadas IS NOT NULL
    AND viviendas_reservadas > 0
")

cat("Registros mensuales leídos:", nrow(pte), "\n")
cat("Rango: ", min(pte$year), "–", max(pte$year), "\n\n")

# --- 2. MEDIA PONDERADA ANUAL ---
tabla_final <- pte %>%
  group_by(ejercicio = year, ambito, isla_id) %>%
  summarise(
    estancia_media = round(
      sum(estancia_media * viviendas_reservadas) / sum(viviendas_reservadas),
      2
    ),
    meses_con_dato = n(),
    .groups = "drop"
  )

cat("Registros anuales calculados:", nrow(tabla_final), "\n")
cat("\nDistribución por ámbito:\n")
print(tabla_final %>% count(ambito))

# Advertir si algún año tiene menos de 12 meses (datos incompletos)
incompletos <- tabla_final %>% filter(meses_con_dato < 12)
if (nrow(incompletos) > 0) {
  cat("\nADVERTENCIA — años con menos de 12 meses de dato:\n")
  print(incompletos %>% select(ejercicio, ambito, isla_id, meses_con_dato))
}

# --- 3. VALIDACIÓN: islas vs canarias ---
cat("\nValidando coherencia islas vs Canarias (últimos 3 años):\n")
media_islas <- tabla_final %>%
  filter(ambito == "isla") %>%
  group_by(ejercicio) %>%
  summarise(media_islas = round(mean(estancia_media), 2), .groups = "drop")

canarias_val <- tabla_final %>%
  filter(ambito == "canarias") %>%
  select(ejercicio, estancia_canarias = estancia_media)

check <- inner_join(canarias_val, media_islas, by = "ejercicio") %>%
  arrange(desc(ejercicio)) %>% head(3)
print(check)

# --- 4. CARGA (TRUNCATE + reload) ---
tabla_carga <- tabla_final %>% select(ejercicio, ambito, isla_id, estancia_media)

cat("\nTRUNCATE + carga...\n")
dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE historico_estancia_media_vv")
  dbWriteTable(con, "historico_estancia_media_vv", tabla_carga,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_carga), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 5. RESUMEN FINAL ---
cat("\nEvolución Canarias (estancia media VV, días):\n")
print(dbGetQuery(con,
  "SELECT ejercicio, estancia_media
   FROM historico_estancia_media_vv
   WHERE ambito = 'canarias'
   ORDER BY ejercicio"))

dbDisconnect(con)
cat("Proceso completado.\n")
