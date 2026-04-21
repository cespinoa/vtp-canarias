#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: importar_hipotecas.R
# Carga la Estadística de Hipotecas del INE desde el CSV producido por
# ine_hipotecas.py y calcula la cuota mensual hipotecaria media.
#
# Cuota (amortización francesa):
#   C = P * [r(1+r)^n] / [(1+r)^n - 1]
#   P = importe_medio_viv (Canarias: dato Canarias; Nacional: dato Nacional)
#   r = tipo_interes / 100 / 12  (solo nacional → se aplica también a Canarias)
#   n = plazo_anios * 12         (solo nacional → se aplica también a Canarias)
#
# Estrategia: TRUNCATE + reload completo (el INE revisa datos retroactivos).
#
# Uso:
#   Rscript descarga_datos/importar_hipotecas.R
#   Rscript descarga_datos/importar_hipotecas.R ruta/al/fichero.csv
# ==============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

# --- 1. LOCALIZAR CSV ---
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/hipotecas_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró ningún CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("Fuente:", csv_path, "\n")

# --- 2. LEER CSV ---
df_raw <- read_csv(csv_path, show_col_types = FALSE)

anyos <- sort(unique(df_raw$anyo))
cat(sprintf("Cobertura: %d-M%02d hasta %d-M%02d\n",
    anyos[1], min(df_raw$mes[df_raw$anyo == anyos[1]]),
    tail(anyos, 1), max(df_raw$mes[df_raw$anyo == tail(anyos, 1)])))
cat("Territorios:", paste(sort(unique(df_raw$territorio)), collapse = ", "), "\n")
cat("Filas:", nrow(df_raw), "\n\n")

# --- 3. CALCULAR CUOTA ---
# Los parámetros de plazo y tipo solo existen en filas "nacional".
# Para Canarias: combinar importe propio con plazo+tipo nacional del mismo mes.

params_nac <- df_raw |>
  filter(territorio == "nacional") |>
  select(anyo, mes, plazo_anios, tipo_interes_total, tipo_interes_fijo, tipo_interes_variable)

cuota_francesa <- function(P, tipo_anual_pct, plazo_anios) {
  r <- tipo_anual_pct / 100 / 12
  n <- plazo_anios * 12
  if (is.na(P) | is.na(r) | is.na(n) | r == 0 | n == 0) return(NA_real_)
  round(P * r * (1 + r)^n / ((1 + r)^n - 1), 2)
}

df_con_params <- df_raw |>
  left_join(params_nac |> rename_with(~ paste0(.x, "_nac"), -c(anyo, mes)),
            by = c("anyo", "mes")) |>
  mutate(
    plazo_anios           = if_else(territorio == "nacional", plazo_anios, plazo_anios_nac),
    tipo_interes_total    = if_else(territorio == "nacional", tipo_interes_total,    tipo_interes_total_nac),
    tipo_interes_fijo     = if_else(territorio == "nacional", tipo_interes_fijo,     tipo_interes_fijo_nac),
    tipo_interes_variable = if_else(territorio == "nacional", tipo_interes_variable, tipo_interes_variable_nac)
  ) |>
  select(-ends_with("_nac")) |>
  rowwise() |>
  mutate(
    cuota_total    = cuota_francesa(importe_medio_viv, tipo_interes_total,    plazo_anios),
    cuota_fija     = cuota_francesa(importe_medio_viv, tipo_interes_fijo,     plazo_anios),
    cuota_variable = cuota_francesa(importe_medio_viv, tipo_interes_variable, plazo_anios)
  ) |>
  ungroup()

cat("Cuotas calculadas (muestra Canarias, último mes con datos):\n")
df_con_params |>
  filter(territorio == "canarias", !is.na(cuota_total)) |>
  slice_tail(n = 3) |>
  select(anyo, mes, importe_medio_viv, plazo_anios, tipo_interes_total,
         cuota_total, cuota_fija, cuota_variable) |>
  print()
cat("\n")

# --- 4. TRUNCATE + RELOAD ---
cat("Truncando hipotecas...\n")
dbExecute(con, "TRUNCATE TABLE hipotecas RESTART IDENTITY")

cat("Insertando", nrow(df_con_params), "filas...\n")
dbWriteTable(con, "hipotecas", df_con_params, append = TRUE, row.names = FALSE)

# --- 5. VERIFICACIÓN ---
n <- as.integer(dbGetQuery(con, "SELECT COUNT(*) AS n FROM hipotecas")$n)
cat(sprintf("\n✓ %d filas en hipotecas.\n\n", n))

# Último mes con cuota calculada para Canarias
resumen <- dbGetQuery(con, "
  SELECT anyo, mes, tipo_dato, n_hipotecas_viv, importe_medio_viv,
         plazo_anios, tipo_interes_total, tipo_interes_fijo, tipo_interes_variable,
         cuota_total, cuota_fija, cuota_variable
  FROM hipotecas
  WHERE territorio = 'canarias' AND cuota_total IS NOT NULL
  ORDER BY anyo DESC, mes DESC LIMIT 3")
cat("Canarias, ultimos meses con cuota calculada:\n")
print(resumen)

dbDisconnect(con)
