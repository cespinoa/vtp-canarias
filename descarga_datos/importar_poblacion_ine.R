#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_poblacion_ine.R
# Carga la Población Municipal del Padrón Municipal del INE (tabla 29005)
# desde el CSV descargado por ine_poblacion.py.
#
# Uso:
#   Rscript descarga_datos/importar_poblacion_ine.R
#   Rscript descarga_datos/importar_poblacion_ine.R ruta/al/fichero.csv
#
# Cobertura: 1996–año actual. Solo nivel municipio (el INE no proporciona
# totales de isla ni de Canarias en esta tabla).
#
# La tabla poblacion almacena ambos orígenes (ISTAC y INE) mediante el
# campo fuente. Esta carga hace UPSERT solo sobre los registros municipales
# de fuente INE, sin tocar los de ISTAC.
#
# Estrategia municipio: tabla temporal + INSERT ON CONFLICT DO UPDATE.
# La clave única (ambito, isla_id, municipio_id, year) funciona aquí porque
# todos los registros municipales tienen municipio_id NOT NULL.
#
# Agregación isla/canarias: si el INE tiene años más recientes que el ISTAC
# para isla/canarias, se calculan por suma de municipios y se cargan con
# fuente "INE t=29005 (agregado municipal)". El ISTAC los sobreescribirá
# cuando publique el dato oficial (importar_poblacion.R usa TRUNCATE+reload).
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/ine_poblacion_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. TABLAS MAESTRAS ---
municipios_db <- dbGetQuery(con, "SELECT id, isla_id, codigo_ine FROM municipios WHERE codigo_ine IS NOT NULL")

# --- 3. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE) %>%
  mutate(codigo_ine = as.character(codigo_ine))

anyos <- sort(unique(df_raw$anyo))
cat("Años en CSV:", paste(anyos, collapse = " "), "\n")
cat("Municipios en CSV:", n_distinct(df_raw$codigo_ine), "\n")
cat("Filas:", nrow(df_raw), "\n\n")

# --- 4. CRUZAR CON BD ---
tabla_final <- df_raw %>%
  inner_join(municipios_db, by = "codigo_ine") %>%
  transmute(
    ambito       = "municipio",
    isla_id,
    municipio_id = id,
    year         = anyo,
    valor        = as.numeric(poblacion),
    fuente       = "INE t=29005"
  ) %>%
  filter(!is.na(valor))

sin_cruzar <- setdiff(df_raw$codigo_ine, municipios_db$codigo_ine)
if (length(sin_cruzar) > 0) {
  cat("ADVERTENCIA — códigos INE sin emparejar en municipios:\n")
  print(sin_cruzar)
} else {
  cat("OK: todos los códigos INE emparejados.\n")
}

cat("\nRegistros a cargar:", nrow(tabla_final), "\n\n")

# --- 5. VALIDACIÓN ---
cat("Validando rango de valores...\n")
resumen <- tabla_final %>%
  group_by(year) %>%
  summarise(total = sum(valor), municipios = n(), .groups = "drop") %>%
  arrange(desc(year)) %>% head(5)
print(resumen)
cat("\n")

# --- 6. CARGA (UPSERT via tabla temporal) ---
# El ON CONFLICT funciona aquí porque municipio_id nunca es NULL
cat("Cargando con upsert (solo registros INE)...\n")

dbBegin(con)
tryCatch({
  dbWriteTable(con, "pob_ine_tmp", tabla_final, temporary = TRUE,
               overwrite = TRUE, row.names = FALSE)

  n <- dbExecute(con, "
    INSERT INTO poblacion (ambito, isla_id, municipio_id, year, valor, fuente)
    SELECT ambito, isla_id, municipio_id, year, valor, fuente
    FROM pob_ine_tmp
    ON CONFLICT (ambito, isla_id, municipio_id, year)
    DO UPDATE SET valor = EXCLUDED.valor, fuente = EXCLUDED.fuente
  ")

  dbCommit(con)
  cat("Cargados/actualizados:", n, "registros.\n")
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# --- 7. AGREGACIÓN ISLA/CANARIAS PARA AÑOS SIN DATO ISTAC ---
# Solo para años más recientes que el máximo en ISTAC para esos ámbitos.
# ON CONFLICT no funciona con NULLs en la clave única → DELETE + INSERT.
cat("\nAgregando isla/canarias para años sin dato ISTAC...\n")

max_istac <- dbGetQuery(con, "
  SELECT MAX(year) AS max_year
  FROM poblacion
  WHERE ambito IN ('canarias', 'isla')
    AND fuente LIKE 'ISTAC%'
")$max_year

anyos_nuevos <- sort(unique(tabla_final$year[tabla_final$year > max_istac]))

if (length(anyos_nuevos) == 0) {
  cat("Sin años nuevos que agregar (ISTAC ya tiene hasta", max_istac, ").\n")
} else {
  cat("Años a agregar:", paste(anyos_nuevos, collapse = " "), "\n")

  datos_nuevos <- tabla_final %>% filter(year %in% anyos_nuevos)

  agg_isla <- datos_nuevos %>%
    group_by(isla_id, year) %>%
    summarise(valor = sum(valor), .groups = "drop") %>%
    mutate(ambito = "isla", municipio_id = NA_integer_, fuente = "INE t=29005 (agregado municipal)")

  agg_canarias <- datos_nuevos %>%
    group_by(year) %>%
    summarise(valor = sum(valor), .groups = "drop") %>%
    mutate(ambito = "canarias", isla_id = NA_integer_, municipio_id = NA_integer_, fuente = "INE t=29005 (agregado municipal)")

  agg_total <- bind_rows(agg_isla, agg_canarias)
  cat("Registros agregados a insertar:", nrow(agg_total), "\n")

  dbBegin(con)
  tryCatch({
    for (yr in anyos_nuevos) {
      dbExecute(con, sprintf("
        DELETE FROM poblacion
        WHERE ambito IN ('canarias', 'isla')
          AND municipio_id IS NULL
          AND year = %d
          AND fuente = 'INE t=29005 (agregado municipal)'
      ", yr))
    }

    dbWriteTable(con, "pob_agg_tmp", agg_total, temporary = TRUE,
                 overwrite = TRUE, row.names = FALSE)

    n_agg <- dbExecute(con, "
      INSERT INTO poblacion (ambito, isla_id, municipio_id, year, valor, fuente)
      SELECT ambito, isla_id, municipio_id, year, valor, fuente
      FROM pob_agg_tmp
    ")

    dbCommit(con)
    cat("Insertados:", n_agg, "registros agregados.\n")
  }, error = function(e) {
    dbRollback(con)
    stop("Error en la carga agregada: ", conditionMessage(e))
  })

  cat("\nResumen de población agregada (año más reciente):\n")
  resumen_agg <- agg_canarias %>%
    bind_rows(agg_isla %>% arrange(isla_id)) %>%
    arrange(ambito, isla_id) %>%
    select(ambito, isla_id, year, valor, fuente)
  print(resumen_agg)
}

# --- 8. RESUMEN FINAL ---
totales <- dbGetQuery(con, "SELECT ambito, fuente, count(*) n FROM poblacion GROUP BY ambito, fuente ORDER BY ambito, fuente")
cat("\nRegistros en BD por ámbito y fuente:\n")
print(totales)

dbDisconnect(con)
cat("Proceso completado.\n")
