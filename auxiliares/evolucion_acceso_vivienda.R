#!/usr/bin/env Rscript
# =============================================================================
# evolucion_acceso_vivienda.R
# Comparativa de los tres vectores de acceso a la vivienda en Canarias:
#   - Precio de compra (IPV, INE t=25171)
#   - Precio de alquiler (SERPAVI, MIVAU)
#   - Cuota hipotecaria calculada (hipotecas, INE t=24457+24458+13896)
#
# Salidas:
#   auxiliares/evolucion_acceso_vivienda.pdf
# =============================================================================

library(RPostgres)
library(DBI)
library(dplyr)
library(tidyr)
library(ggplot2)
library(scales)
library(dotenv)

load_dot_env(".env")

con <- dbConnect(RPostgres::Postgres(),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  dbname   = Sys.getenv("DB_NAME"),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASS")
)

# =============================================================================
# 1. CARGA DE DATOS
# =============================================================================

# IPV: media anual de trimestres, Canarias general
ipv <- dbGetQuery(con, "
  SELECT anyo,
         AVG(indice) AS indice
  FROM ipv_vivienda
  WHERE territorio_codigo = '05' AND tipo_vivienda = 'general'
  GROUP BY anyo ORDER BY anyo")

# SERPAVI: media ponderada anual Canarias
serpavi <- dbGetQuery(con, "
  SELECT anyo,
         SUM(alq_m2_media * n_viviendas) / SUM(n_viviendas) AS alq_m2
  FROM serpavi_alquiler
  WHERE alq_m2_media IS NOT NULL AND n_viviendas IS NOT NULL
  GROUP BY anyo ORDER BY anyo")

# Hipotecas Canarias: mensual y anual
hip_men <- dbGetQuery(con, "
  SELECT anyo, mes, tipo_interes_total, tipo_interes_fijo, tipo_interes_variable,
         cuota_total, cuota_fija, cuota_variable, importe_medio_viv, plazo_anios
  FROM hipotecas
  WHERE territorio = 'canarias' AND cuota_total IS NOT NULL
  ORDER BY anyo, mes")

hip_anual <- hip_men |>
  group_by(anyo) |>
  summarise(
    cuota_media    = mean(cuota_total, na.rm = TRUE),
    tipo_medio     = mean(tipo_interes_total, na.rm = TRUE),
    importe_medio  = mean(importe_medio_viv, na.rm = TRUE),
    plazo_medio    = mean(plazo_anios, na.rm = TRUE),
    .groups = "drop"
  )

dbDisconnect(con)

# =============================================================================
# 2. INDICES BASE 2015 = 100
# =============================================================================

base_ipv     <- ipv$indice[ipv$anyo == 2015]
base_serpavi <- serpavi$alq_m2[serpavi$anyo == 2015]
base_cuota   <- hip_anual$cuota_media[hip_anual$anyo == 2015]

ipv_idx    <- ipv    |> mutate(idx = indice / base_ipv * 100)
serp_idx   <- serpavi|> mutate(idx = alq_m2 / base_serpavi * 100)
hip_idx    <- hip_anual |> mutate(idx = cuota_media / base_cuota * 100)

cat(sprintf("Base 2015: IPV=%.1f  SERPAVI=%.2f EUR/m2  Cuota=%.0f EUR/mes\n",
    base_ipv, base_serpavi, base_cuota))

# Valores finales para anotaciones
anyo_fin  <- min(max(ipv_idx$anyo), max(serp_idx$anyo), max(hip_idx$anyo))
v_ipv     <- ipv_idx$idx[ipv_idx$anyo == anyo_fin]
v_serp    <- serp_idx$idx[serp_idx$anyo == anyo_fin]
v_hip     <- hip_idx$idx[hip_idx$anyo == anyo_fin]
cat(sprintf("Indice %d: compra=%.1f  alquiler=%.1f  cuota=%.1f\n",
    anyo_fin, v_ipv, v_serp, v_hip))

# Contrafactual: cuota si el tipo se hubiera quedado en 2015
tipo_2015  <- hip_anual$tipo_medio[hip_anual$anyo == 2015]
plazo_2015 <- hip_anual$plazo_medio[hip_anual$anyo == 2015]

cuota_francesa <- function(P, tipo_anual_pct, plazo_anios) {
  r <- tipo_anual_pct / 100 / 12
  n <- plazo_anios * 12
  P * r * (1 + r)^n / ((1 + r)^n - 1)
}

hip_contra <- hip_anual |>
  mutate(
    cuota_tipo_fijo_2015  = cuota_francesa(importe_medio, tipo_2015,  plazo_anios = plazo_medio),
    cuota_precio_fijo_2015 = cuota_francesa(importe_medio[anyo == 2015][1], tipo_medio, plazo_medio)
  )

# =============================================================================
# 3. COLORES Y TEMA
# =============================================================================

COL_COMPRA    <- "#d6604d"
COL_ALQUILER  <- "#2166ac"
COL_CUOTA     <- "#4dac26"
COL_TIPO      <- "#b35806"
COL_CONTRA    <- "#999999"

tema <- theme_minimal(base_size = 10) +
  theme(
    panel.grid.minor = element_blank(),
    panel.grid.major = element_line(color = "grey92"),
    axis.text        = element_text(size = 8),
    plot.title       = element_text(size = 12, face = "bold"),
    plot.subtitle    = element_text(size = 8.5, color = "grey40", lineheight = 1.3),
    plot.caption     = element_text(size = 7, color = "grey50"),
    legend.position  = "bottom",
    legend.text      = element_text(size = 8.5)
  )

pdf("auxiliares/evolucion_acceso_vivienda.pdf", width = 11, height = 8.5)

# =============================================================================
# PAGINA 1: Los tres indices base 2015=100
# =============================================================================

periodo <- intersect(intersect(ipv_idx$anyo, serp_idx$anyo), hip_idx$anyo)

dat_p1 <- bind_rows(
  ipv_idx  |> filter(anyo %in% periodo) |> transmute(anyo, valor = idx, serie = "Precio compra (IPV)"),
  serp_idx |> filter(anyo %in% periodo) |> transmute(anyo, valor = idx, serie = "Precio alquiler (SERPAVI)"),
  hip_idx  |> filter(anyo %in% periodo) |> transmute(anyo, valor = idx, serie = "Cuota hipotecaria")
)

p1 <- ggplot(dat_p1, aes(x = anyo, y = valor, color = serie)) +
  geom_hline(yintercept = 100, linetype = "dashed", color = "grey60", linewidth = 0.4) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2.2) +
  geom_text(
    data = dat_p1 |> filter(anyo == anyo_fin),
    aes(label = sprintf("%.0f", valor)),
    hjust = -0.2, size = 3, fontface = "bold", show.legend = FALSE
  ) +
  scale_color_manual(values = c(
    "Precio compra (IPV)"      = COL_COMPRA,
    "Precio alquiler (SERPAVI)" = COL_ALQUILER,
    "Cuota hipotecaria"         = COL_CUOTA
  )) +
  scale_x_continuous(
    breaks = seq(min(periodo), max(periodo), by = 2),
    expand = expansion(mult = c(0.02, 0.07))
  ) +
  labs(
    title    = "Acceso a la vivienda en Canarias: precio de compra, alquiler y cuota hipotecaria",
    subtitle = sprintf(
      "Base 2015 = 100. Datos %d: compra +%.0f%%, cuota +%.0f%%, alquiler +%.0f%%.\nLa cuota supera al alquiler desde ~2021 por la combinacion de precios altos y subida de tipos.",
      anyo_fin, v_ipv - 100, v_hip - 100, v_serp - 100),
    x = NULL, y = "Indice (2015 = 100)", color = NULL,
    caption = "IPV: INE t=25171 (media anual). SERPAVI: MIVAU/AEAT (media ponderada Canarias). Cuota: INE t=24457+24458+13896 (amortizacion francesa, media anual)."
  ) +
  tema

print(p1)

# =============================================================================
# PAGINA 2: Cuota mensual en EUR + tipo de interes (eje secundario), mensual
# =============================================================================

hip_men <- hip_men |>
  mutate(fecha = as.Date(sprintf("%d-%02d-01", anyo, mes)))

escala <- max(hip_men$cuota_total) / max(hip_men$tipo_interes_total)

p2 <- ggplot(hip_men, aes(x = fecha)) +
  geom_area(aes(y = cuota_total), fill = COL_CUOTA, alpha = 0.15) +
  geom_line(aes(y = cuota_total, color = "Cuota media Canarias (EUR/mes)"),
            linewidth = 1.0) +
  geom_line(aes(y = tipo_interes_total * escala, color = "Tipo de interes medio (%)"),
            linewidth = 0.8, linetype = "dashed") +
  annotate("rect",
           xmin = as.Date("2022-07-01"), xmax = as.Date("2024-06-01"),
           ymin = -Inf, ymax = Inf, fill = "#fee090", alpha = 0.25) +
  annotate("text",
           x = as.Date("2023-01-01"), y = max(hip_men$cuota_total) * 0.95,
           label = "Subida BCE\n2022-2023", size = 2.8, color = "#8c510a",
           hjust = 0.5, lineheight = 0.9) +
  scale_color_manual(values = c(
    "Cuota media Canarias (EUR/mes)" = COL_CUOTA,
    "Tipo de interes medio (%)"      = COL_TIPO
  )) +
  scale_y_continuous(
    name   = "Cuota (EUR/mes)",
    labels = label_comma(big.mark = "."),
    sec.axis = sec_axis(~ . / escala, name = "Tipo de interes (%)",
                        labels = function(x) sprintf("%.1f%%", x))
  ) +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  labs(
    title    = "Cuota hipotecaria mensual media en Canarias (2009-2026)",
    subtitle = "La cuota alcanza sus maximos en 2025 por precios record, pese a que los tipos han bajado del pico 2023.",
    x = NULL, color = NULL,
    caption = "Cuota calculada con amortizacion francesa: importe medio Canarias + plazo y tipo nacionales (INE)."
  ) +
  tema

print(p2)

# =============================================================================
# PAGINA 3: Descomposicion precio vs tipos
# =============================================================================

# Cuota real vs dos contrafactuales desde 2015
dat_p3 <- hip_contra |>
  filter(anyo >= 2015) |>
  transmute(
    anyo,
    "Cuota real"                               = cuota_media,
    "Si solo cambia precio (tipo 2015)"        = cuota_tipo_fijo_2015,
    "Si solo cambia tipo (precio 2015)"        = cuota_precio_fijo_2015
  ) |>
  pivot_longer(-anyo, names_to = "escenario", values_to = "cuota")

p3 <- ggplot(dat_p3, aes(x = anyo, y = cuota, color = escenario, linetype = escenario)) +
  geom_line(linewidth = 1.0) +
  geom_point(data = dat_p3 |> filter(escenario == "Cuota real"), size = 2) +
  scale_color_manual(values = c(
    "Cuota real"                               = COL_CUOTA,
    "Si solo cambia precio (tipo 2015)"        = COL_COMPRA,
    "Si solo cambia tipo (precio 2015)"        = COL_TIPO
  )) +
  scale_linetype_manual(values = c(
    "Cuota real"                               = "solid",
    "Si solo cambia precio (tipo 2015)"        = "dashed",
    "Si solo cambia tipo (precio 2015)"        = "dotted"
  )) +
  scale_x_continuous(breaks = 2015:max(hip_contra$anyo)) +
  scale_y_continuous(labels = label_comma(big.mark = "."), limits = c(350, NA)) +
  labs(
    title    = "Cuota hipotecaria: efecto precio vs efecto tipo de interes (Canarias, 2015-2026)",
    subtitle = paste0(
      "La linea roja muestra que el precio por si solo explicaria la mayor parte del encarecimiento.\n",
      "El efecto del tipo de interes fue temporal (pico 2023) y esta siendo compensado por su bajada."),
    x = NULL, y = "Cuota estimada (EUR/mes)",
    color = NULL, linetype = NULL,
    caption = "Contrafactuales: 'solo precio' = tipo congelado en 2015; 'solo tipo' = importe congelado en 2015."
  ) +
  tema

print(p3)

# =============================================================================
# PAGINA 4: Variacion anual de los tres indicadores
# =============================================================================

ipv_var <- ipv_idx |>
  mutate(var = (indice / lag(indice) - 1) * 100) |>
  filter(!is.na(var), anyo %in% periodo) |>
  transmute(anyo, valor = var, serie = "Precio compra (IPV)")

serp_var <- serp_idx |>
  mutate(var = (alq_m2 / lag(alq_m2) - 1) * 100) |>
  filter(!is.na(var), anyo %in% periodo) |>
  transmute(anyo, valor = var, serie = "Precio alquiler (SERPAVI)")

hip_var <- hip_idx |>
  mutate(var = (cuota_media / lag(cuota_media) - 1) * 100) |>
  filter(!is.na(var), anyo %in% periodo) |>
  transmute(anyo, valor = var, serie = "Cuota hipotecaria")

dat_p4 <- bind_rows(ipv_var, serp_var, hip_var)

p4 <- ggplot(dat_p4, aes(x = anyo, y = valor, color = serie)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey50", linewidth = 0.4) +
  geom_line(linewidth = 1.0) +
  geom_point(size = 2) +
  geom_text(aes(label = sprintf("%.1f%%", valor)),
            vjust = -0.8, size = 2.4, show.legend = FALSE) +
  scale_color_manual(values = c(
    "Precio compra (IPV)"       = COL_COMPRA,
    "Precio alquiler (SERPAVI)" = COL_ALQUILER,
    "Cuota hipotecaria"         = COL_CUOTA
  )) +
  scale_x_continuous(breaks = seq(min(periodo), max(periodo), by = 2)) +
  labs(
    title    = "Variacion anual: precio compra, alquiler y cuota hipotecaria en Canarias (%)",
    subtitle = "La cuota amplifica la volatilidad del precio de compra con el efecto adicional de los tipos de interes.",
    x = NULL, y = "Variacion anual (%)", color = NULL,
    caption = "Fuentes: INE (IPV, hipotecas), MIVAU/AEAT (SERPAVI). Cuota basada en medias anuales."
  ) +
  tema

print(p4)

dev.off()
cat("\nPDF generado en: auxiliares/evolucion_acceso_vivienda.pdf\n")
