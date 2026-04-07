#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_censo2021_hogares.R
# Carga los datos del Censo 2021 (INE) sobre hogares según número de núcleos
# familiares, a nivel municipal para los 88 municipios de Canarias.
#
# Uso:
#   Rscript descarga_datos/importar_censo2021_hogares.R
#   Rscript descarga_datos/importar_censo2021_hogares.R ruta/al/fichero.csv
#
# Fuente: descarga_datos/censo2021_hogares.py → tmp/censo2021_hogares_YYYYMMDD.csv
# Tabla destino: nucleos_censales
#
# Campos del CSV: codigo_ine | nombre | num_nucleos | hogares
# Categorías de num_nucleos: "0" | "1" | "2" | "3 o más"
#
# El CSV está en formato largo (una fila por municipio × categoría). Este script
# lo pivota a formato ancho antes de insertar en la tabla:
#   hogares_0  → Sin núcleo familiar
#   hogares_1  → Un núcleo
#   hogares_2  → Dos núcleos
#   hogares_3  → Tres o más núcleos
#
# Nota: algunos municipios pueden estar ausentes por secreto estadístico (en
# 2021: Artenara, Betancuria, Agulo). Se registra un aviso y se generan filas
# con hogares_0..hogares_3 = 0 para garantizar cobertura completa de los 88
# municipios y evitar errores en PT01 al calcular snapshots.
#
# Estrategia: DELETE por year + insert. Preserva datos de otros años censales.
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/censo2021_hogares_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/. ",
                                     "Ejecuta primero: python3 descarga_datos/censo2021_hogares.py")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
municipios_db <- dbGetQuery(con,
  "SELECT id, isla_id, codigo_ine FROM municipios WHERE codigo_ine IS NOT NULL")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE,
                   col_types = cols(
                     codigo_ine  = col_character(),
                     nombre      = col_character(),
                     num_nucleos = col_character(),
                     hogares     = col_integer()
                   ))

cat("Filas en CSV:", nrow(df_raw), "\n")
cat("Municipios:", n_distinct(df_raw$codigo_ine), "\n")
cat("Categorías:", paste(sort(unique(df_raw$num_nucleos)), collapse = " | "), "\n\n")

# --- 4. CRUZAR CON MUNICIPIOS ---
sin_emparejar <- setdiff(df_raw$codigo_ine, municipios_db$codigo_ine)
if (length(sin_emparejar) > 0) {
  cat("ADVERTENCIA — códigos INE sin emparejar en BD:\n")
  print(sin_emparejar)
}

df_joined <- df_raw %>%
  mutate(col = paste0("hogares_", recode(num_nucleos,
    "0"       = "0",
    "1"       = "1",
    "2"       = "2",
    "3 o más" = "3"
  ))) %>%
  select(codigo_ine, col, hogares) %>%
  pivot_wider(names_from = col, values_from = hogares, values_fill = 0L)

# Garantizar que las 4 columnas existen (aunque alguna categoría faltase en el CSV)
for (col in c("hogares_0", "hogares_1", "hogares_2", "hogares_3")) {
  if (!col %in% names(df_joined)) df_joined[[col]] <- 0L
}

# --- 5. UNIR CON MUNICIPIOS (left join desde BD para cubrir los 88) ---
# Los municipios sin dato en el CSV (secreto estadístico) quedan con NA → 0
df_wide <- municipios_db %>%
  left_join(df_joined, by = "codigo_ine") %>%
  mutate(across(starts_with("hogares_"), ~ replace_na(.x, 0L)))

municipios_ausentes <- df_wide %>%
  filter(hogares_0 == 0, hogares_1 == 0, hogares_2 == 0, hogares_3 == 0) %>%
  pull(codigo_ine)

if (length(municipios_ausentes) > 0) {
  mun_nom <- dbGetQuery(con,
    "SELECT codigo_ine, nombre FROM municipios WHERE codigo_ine IS NOT NULL")
  ausentes_df <- mun_nom %>% filter(codigo_ine %in% municipios_ausentes)
  cat("AVISO —", length(municipios_ausentes),
      "municipio(s) sin dato en el Censo (secreto estadístico) → cargados con ceros:\n")
  print(ausentes_df)
  cat("\n")
}

tabla_final <- df_wide %>%
  mutate(year = as.Date("2021-12-31")) %>%
  select(municipio_id = id, isla_id, year, hogares_0, hogares_1, hogares_2, hogares_3)

cat("Municipios a cargar:", nrow(tabla_final), "\n")

# --- 6. VALIDACIÓN ---
cat("\nResumen Canarias (suma de municipios):\n")
totales <- tibble(
  categoria  = c("0 - Sin núcleo", "1 - Un núcleo", "2 - Dos núcleos", "3+ - Tres o más"),
  hogares    = c(sum(tabla_final$hogares_0), sum(tabla_final$hogares_1),
                 sum(tabla_final$hogares_2), sum(tabla_final$hogares_3))
) %>%
  mutate(pct = round(hogares / sum(hogares) * 100, 1))

print(totales)
cat("TOTAL hogares:", formatC(sum(totales$hogares), format = "d", big.mark = ","), "\n")

plurinucleares <- sum(tabla_final$hogares_2) + sum(tabla_final$hogares_3)
if (plurinucleares < 30000 || plurinucleares > 80000) {
  cat("ADVERTENCIA — suma de plurinucleares (", formatC(plurinucleares, format = "d"),
      ") fuera del rango esperado (30k–80k). Verifica el CSV.\n", sep = "")
}

# --- 7. CARGA (TRUNCATE + reload) ---
cat("\nTRUNCATE + carga...\n")

year_carga <- unique(tabla_final$year)

dbBegin(con)
tryCatch({
  dbExecute(con, "DELETE FROM nucleos_censales WHERE year = $1", list(year_carga))
  dbWriteTable(con, "nucleos_censales", tabla_final, append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat("Cargados:", nrow(tabla_final), "municipios.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 8. RESUMEN FINAL ---
cat("\nResumen en BD:\n")
print(dbGetQuery(con,
  "SELECT year,
          count(*) municipios,
          sum(hogares_0) hogares_0,
          sum(hogares_1) hogares_1,
          sum(hogares_2) hogares_2,
          sum(hogares_3) hogares_3
   FROM nucleos_censales
   GROUP BY year ORDER BY year"))

dbDisconnect(con)
cat("\nProceso completado.\n")
