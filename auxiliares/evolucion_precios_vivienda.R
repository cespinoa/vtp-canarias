#!/usr/bin/env Rscript
# =============================================================================
# evolucion_precios_vivienda.R
# Comparativa de la evolución del precio de compra (IPV, INE) y del alquiler
# (SERPAVI, MIVAU) en Canarias.
#
# Salidas:
#   auxiliares/evolucion_precios_vivienda.pdf
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

# -----------------------------------------------------------------------------
# 1. IPV — media anual de trimestres, Canarias y Nacional
# -----------------------------------------------------------------------------

ipv_raw <- dbGetQuery(con, "
  SELECT territorio_codigo, anyo, trimestre, tipo_vivienda,
         indice, variacion_anual
  FROM ipv_vivienda
  ORDER BY territorio_codigo, anyo, trimestre")

# Media anual para comparación con SERPAVI (anual)
ipv_anual <- ipv_raw |>
  group_by(territorio_codigo, anyo, tipo_vivienda) |>
  summarise(
    indice_anual      = mean(indice, na.rm = TRUE),
    variacion_anual   = mean(variacion_anual, na.rm = TRUE),
    .groups = "drop"
  )

# Etiquetas legibles
ipv_anual <- ipv_anual |>
  mutate(
    territorio = if_else(territorio_codigo == "05", "Canarias", "Nacional"),
    tipo_label = case_match(tipo_vivienda,
      "general"      ~ "General",
      "nueva"        ~ "Vivienda nueva",
      "segunda_mano" ~ "Segunda mano"
    )
  )

# -----------------------------------------------------------------------------
# 2. SERPAVI — media ponderada anual, Canarias; rebase a 2015=100
# -----------------------------------------------------------------------------

serpavi <- dbGetQuery(con, "
  SELECT anyo,
         SUM(alq_m2_media * n_viviendas) / SUM(n_viviendas) AS alq_m2
  FROM serpavi_alquiler
  WHERE alq_m2_media IS NOT NULL AND n_viviendas IS NOT NULL
  GROUP BY anyo ORDER BY anyo")

base_2015 <- serpavi$alq_m2[serpavi$anyo == 2015]
serpavi <- serpavi |>
  mutate(
    indice_alq = alq_m2 / base_2015 * 100,
    variacion_anual = (alq_m2 / lag(alq_m2) - 1) * 100
  )

cat(sprintf("SERPAVI base 2015: %.2f €/m²\n", base_2015))
cat(sprintf("SERPAVI 2024: %.2f €/m²  → índice %.1f\n",
    tail(serpavi$alq_m2, 1), tail(serpavi$indice_alq, 1)))
cat(sprintf("IPV general Canarias 2024: %.1f\n",
    ipv_anual$indice_anual[ipv_anual$territorio_codigo == "05" &
                           ipv_anual$tipo_vivienda == "general" &
                           ipv_anual$anyo == 2024]))

# -----------------------------------------------------------------------------
# 3. Colores y tema
# -----------------------------------------------------------------------------

COL_COMPRA_CAN  <- "#d6604d"
COL_COMPRA_NAC  <- "#f4a582"
COL_ALQUILER    <- "#2166ac"
COL_NUEVA       <- "#e08214"
COL_SEGUNDA     <- "#762a83"

tema_base <- theme_minimal(base_size = 10) +
  theme(
    panel.grid.minor  = element_blank(),
    panel.grid.major  = element_line(color = "grey90"),
    axis.text         = element_text(size = 8),
    plot.title        = element_text(size = 12, face = "bold"),
    plot.subtitle     = element_text(size = 8.5, color = "grey40"),
    plot.caption      = element_text(size = 7, color = "grey50"),
    legend.position   = "bottom",
    legend.text       = element_text(size = 8)
  )

pdf("auxiliares/evolucion_precios_vivienda.pdf", width = 11, height = 8.5)

# =============================================================================
# PÁGINA 1: Índice base 2015=100 — compra vs alquiler, Canarias
# =============================================================================

periodo_comun <- intersect(
  ipv_anual$anyo[ipv_anual$territorio_codigo == "05" & ipv_anual$tipo_vivienda == "general"],
  serpavi$anyo
)

dat_p1_ipv <- ipv_anual |>
  filter(territorio_codigo == "05", tipo_vivienda == "general",
         anyo %in% periodo_comun) |>
  transmute(anyo, valor = indice_anual, serie = "Compra (IPV general)")

dat_p1_alq <- serpavi |>
  filter(anyo %in% periodo_comun) |>
  transmute(anyo, valor = indice_alq, serie = "Alquiler (SERPAVI)")

dat_p1 <- bind_rows(dat_p1_ipv, dat_p1_alq)

# Divergencia al final del período
div_compra <- dat_p1_ipv$valor[dat_p1_ipv$anyo == max(periodo_comun)]
div_alq    <- dat_p1_alq$valor[dat_p1_alq$anyo == max(periodo_comun)]

p1 <- ggplot(dat_p1, aes(x = anyo, y = valor, color = serie)) +
  geom_hline(yintercept = 100, linetype = "dashed", color = "grey60", linewidth = 0.4) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2) +
  annotate("text", x = max(periodo_comun), y = div_compra + 2.5,
           label = sprintf("%.0f", div_compra), size = 3, hjust = 0.5,
           color = COL_COMPRA_CAN, fontface = "bold") +
  annotate("text", x = max(periodo_comun), y = div_alq - 3.5,
           label = sprintf("%.0f", div_alq), size = 3, hjust = 0.5,
           color = COL_ALQUILER, fontface = "bold") +
  scale_color_manual(values = c(
    "Compra (IPV general)" = COL_COMPRA_CAN,
    "Alquiler (SERPAVI)"   = COL_ALQUILER
  )) +
  scale_x_continuous(breaks = seq(min(periodo_comun), max(periodo_comun), by = 2)) +
  scale_y_continuous(labels = function(x) paste0(x)) +
  labs(
    title    = "Evolución del precio de la vivienda en Canarias (base 2015 = 100)",
    subtitle = sprintf(
      "Compra: IPV general anual (INE t=25171). Alquiler: media ponderada SERPAVI (MIVAU), rebasada 2015=100.\nDivergencia %d: compra %.0f vs alquiler %.0f (diferencia de %.0f puntos)",
      max(periodo_comun), div_compra, div_alq, div_compra - div_alq),
    x = NULL, y = "Índice (2015 = 100)",
    color = NULL,
    caption = "Compra: precio de transacción (notaría). Alquiler: precio en contratos vigentes (AEAT/Modelo 100)."
  ) +
  tema_base

print(p1)

# =============================================================================
# PÁGINA 2: Compra vs alquiler + referencia Nacional (índices)
# =============================================================================

dat_p2_nac <- ipv_anual |>
  filter(territorio_codigo == "00", tipo_vivienda == "general",
         anyo %in% periodo_comun) |>
  transmute(anyo, valor = indice_anual, serie = "Compra Nacional (IPV)")

dat_p2 <- bind_rows(dat_p1, dat_p2_nac)

p2 <- ggplot(dat_p2, aes(x = anyo, y = valor, color = serie, linetype = serie)) +
  geom_hline(yintercept = 100, linetype = "dashed", color = "grey60", linewidth = 0.4) +
  geom_line(linewidth = 1.0) +
  geom_point(size = 1.8) +
  scale_color_manual(values = c(
    "Compra (IPV general)"  = COL_COMPRA_CAN,
    "Alquiler (SERPAVI)"    = COL_ALQUILER,
    "Compra Nacional (IPV)" = COL_COMPRA_NAC
  )) +
  scale_linetype_manual(values = c(
    "Compra (IPV general)"  = "solid",
    "Alquiler (SERPAVI)"    = "solid",
    "Compra Nacional (IPV)" = "dashed"
  )) +
  scale_x_continuous(breaks = seq(min(periodo_comun), max(periodo_comun), by = 2)) +
  labs(
    title    = "Canarias vs Nacional: precio de compra y alquiler (base 2015 = 100)",
    subtitle = "Canarias muestra mayor crecimiento del precio de compra que la media nacional",
    x = NULL, y = "Índice (2015 = 100)",
    color = NULL, linetype = NULL,
    caption = "Fuentes: INE tabla 25171 (IPV) - MIVAU/AEAT SERPAVI (alquiler)"
  ) +
  tema_base

print(p2)

# =============================================================================
# PÁGINA 3: Tasas de variación anual (%)
# =============================================================================

dat_p3_ipv <- ipv_anual |>
  filter(territorio_codigo == "05", tipo_vivienda == "general",
         anyo %in% periodo_comun, !is.na(variacion_anual)) |>
  transmute(anyo, valor = variacion_anual, serie = "Compra (IPV general)")

dat_p3_alq <- serpavi |>
  filter(anyo %in% periodo_comun, !is.na(variacion_anual)) |>
  transmute(anyo, valor = variacion_anual, serie = "Alquiler (SERPAVI)")

dat_p3 <- bind_rows(dat_p3_ipv, dat_p3_alq)

p3 <- ggplot(dat_p3, aes(x = anyo, y = valor, color = serie)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey50", linewidth = 0.4) +
  geom_line(linewidth = 1.0) +
  geom_point(size = 2) +
  geom_text(aes(label = sprintf("%.1f%%", valor)),
            vjust = -0.8, size = 2.5, show.legend = FALSE) +
  scale_color_manual(values = c(
    "Compra (IPV general)" = COL_COMPRA_CAN,
    "Alquiler (SERPAVI)"   = COL_ALQUILER
  )) +
  scale_x_continuous(breaks = seq(min(periodo_comun), max(periodo_comun), by = 2)) +
  labs(
    title    = "Variación anual del precio de compra y alquiler en Canarias (%)",
    subtitle = "El precio de compra amplifica los ciclos; el alquiler muestra mayor estabilidad",
    x = NULL, y = "Variación anual (%)",
    color = NULL,
    caption = "IPV: variación media anual de los cuatro trimestres. SERPAVI: variación respecto al año anterior."
  ) +
  tema_base

print(p3)

# =============================================================================
# PÁGINA 4: IPV por tipo de vivienda — Canarias (serie completa 2007-2025)
# =============================================================================

ipv_trim_can <- ipv_raw |>
  filter(territorio_codigo == "05") |>
  mutate(
    fecha = as.Date(sprintf("%d-%02d-01", anyo, (trimestre - 1) * 3 + 1)),
    tipo_label = case_match(tipo_vivienda,
      "general"      ~ "General",
      "nueva"        ~ "Vivienda nueva",
      "segunda_mano" ~ "Segunda mano"
    )
  )

p4 <- ggplot(ipv_trim_can, aes(x = fecha, y = indice, color = tipo_label)) +
  geom_hline(yintercept = 100, linetype = "dashed", color = "grey60", linewidth = 0.4) +
  geom_line(linewidth = 0.9) +
  scale_color_manual(values = c(
    "General"        = COL_COMPRA_CAN,
    "Vivienda nueva" = COL_NUEVA,
    "Segunda mano"   = COL_SEGUNDA
  )) +
  scale_x_date(date_breaks = "2 years", date_labels = "%Y") +
  labs(
    title    = "IPV Canarias por tipo de vivienda (base 2015 = 100, trimestral)",
    subtitle = "Vivienda nueva supera ampliamente a segunda mano desde 2021",
    x = NULL, y = "Índice (2015 = 100)",
    color = NULL,
    caption = "Fuente: INE tabla 25171 - Q4 2007 - Q4 2025"
  ) +
  tema_base

print(p4)

dev.off()

dbDisconnect(con)
cat("\nPDF generado en: auxiliares/evolucion_precios_vivienda.pdf\n")
