#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_ech_hogares_tipo.R
# Carga en ech_hogares_tipo la serie de hogares por tipo de hogar para Canarias,
# combinando dos fuentes:
#
#   ECH 2013-2020 (Encuesta Continua de Hogares, INE operación 274)
#     Script de descarga: ine_ech_tipo_hogar.py
#     CSV: tmp/ine_ech_tipo_hogar_YYYYMMDD.csv
#     Unidad original: miles de hogares (con decimal)
#     Categorías incluyen: Hogar unipersonal, Dos o más núcleos familiares,
#       Hogar monoparental, Pareja sin hijos, Pareja con hijos (total y desglose),
#       Núcleo familiar con otras personas, Personas sin núcleo entre sí
#
#   ECEPOV 2021+ (Encuesta de Características Esenciales, INE tabla 56531+)
#     Script de descarga: ine_ech_hogares.py
#     CSV: tmp/ine_ech_hogares_YYYYMMDD.csv
#     Unidad original: número de hogares → se convierte a miles al importar
#     Categorías: Hogar unipersonal, Hogar monoparental, Pareja sin hijos,
#       Pareja con hijos, Otros tipos de hogar
#     NOTA: "Otros tipos de hogar" agrupa plurinucleares + formas atípicas;
#       NO equivale a "Dos o más núcleos familiares" de la ECH.
#
# Estrategia: TRUNCATE + reload completo.
# Solo ámbito Canarias — no integrar en base_snapshots / full_snapshots.
#
# Uso:
#   Rscript descarga_datos/importar_ech_hogares_tipo.R
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSVs ---
csv_ech <- {
  candidatos <- Sys.glob("descarga_datos/tmp/ine_ech_tipo_hogar_????????.csv")
  if (length(candidatos) == 0)
    stop("No se encontró ine_ech_tipo_hogar_*.csv. ",
         "Ejecuta primero: python3 descarga_datos/ine_ech_tipo_hogar.py")
  tail(sort(candidatos), 1)
}

csv_ecepov <- {
  candidatos <- Sys.glob("descarga_datos/tmp/ine_ech_hogares_????????.csv")
  if (length(candidatos) == 0)
    stop("No se encontró ine_ech_hogares_*.csv. ",
         "Ejecuta primero: python3 descarga_datos/ine_ech_hogares.py")
  tail(sort(candidatos), 1)
}

cat("Fuente ECH:   ", csv_ech,   "\n")
cat("Fuente ECEPOV:", csv_ecepov, "\n\n")

# --- 2. LEER Y PREPARAR ECH (2013-2020) ---
ech <- read_csv(csv_ech, show_col_types = FALSE,
                col_types = cols(
                  anyo          = col_integer(),
                  tipo_hogar    = col_character(),
                  hogares_miles = col_double()
                )) %>%
  # Excluir subtotales de parejas con hijos para evitar doble conteo
  filter(!tipo_hogar %in% c(
    "Pareja con hijos que convivan en el hogar: 1 hijo",
    "Pareja con hijos que convivan en el hogar: 2 hijos",
    "Pareja con hijos que convivan en el hogar: 3 o más hijos"
  )) %>%
  mutate(fuente = "ECH")

cat("ECH — filas leídas:", nrow(ech),
    "| años:", paste(range(ech$anyo), collapse = "–"), "\n")

# --- 3. LEER Y PREPARAR ECEPOV (2021+) ---
ecepov <- read_csv(csv_ecepov, show_col_types = FALSE,
                   col_types = cols(
                     anyo       = col_integer(),
                     tipo_hogar = col_character(),
                     hogares    = col_double()
                   )) %>%
  mutate(
    hogares_miles = hogares / 1000,
    fuente = "ECEPOV"
  ) %>%
  select(anyo, tipo_hogar, hogares_miles, fuente)

cat("ECEPOV — filas leídas:", nrow(ecepov),
    "| años:", paste(sort(unique(ecepov$anyo)), collapse = ", "), "\n\n")

# --- 4. COMBINAR ---
tabla_final <- bind_rows(ech, ecepov) %>%
  arrange(fuente, anyo, tipo_hogar)

cat("Total filas a cargar:", nrow(tabla_final), "\n")

# --- 5. RESUMEN COMPARADO ---
cat("\nHogares unipersonales (miles):\n")
uni <- tabla_final %>%
  filter(str_detect(tipo_hogar, "(?i)unipersonal")) %>%
  select(anyo, fuente, hogares_miles) %>%
  arrange(anyo)
print(uni, n = Inf)

cat("\nHogares plurinucleares (miles) — ECH: 'Dos o más núcleos', ECEPOV: n/a:\n")
pluri <- tabla_final %>%
  filter(str_detect(tipo_hogar, "(?i)dos o más núcleos|dos o mas nucleos")) %>%
  select(anyo, fuente, hogares_miles) %>%
  arrange(anyo)
if (nrow(pluri) > 0) print(pluri, n = Inf) else cat("  (sin categoría explícita en ECEPOV)\n")

# --- 6. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")
dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE ech_hogares_tipo")
  dbWriteTable(con, "ech_hogares_tipo",
               tabla_final %>% select(anyo, tipo_hogar, hogares_miles, fuente),
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 7. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT fuente, count(*) categorias, min(anyo) anyo_min, max(anyo) anyo_max
   FROM ech_hogares_tipo GROUP BY fuente ORDER BY fuente"))

dbDisconnect(con)
cat("\nProceso completado.\n")
