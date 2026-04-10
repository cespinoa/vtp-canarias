# =============================================================================
# evolucion_tamanio_hogar_tipo.R
# Análisis de la evolución del tamaño medio del hogar agrupado por tipo de municipio
#
# Salidas:
#   - Tabla de pendientes y anomalías por tipo de municipio (consola)
#   - PDF con pequeños múltiplos agrupados por tipo (auxiliares/evolucion_tamanio_hogar_tipo.pdf)
# =============================================================================

library(RPostgres)
library(DBI)
library(dplyr)
library(ggplot2)
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
# 1. Carga de datos
# -----------------------------------------------------------------------------

raw <- dbGetQuery(con, "
  SELECT
    h.ambito,
    h.isla_id,
    h.municipio_id,
    h.miembros,
    EXTRACT(YEAR FROM h.year)::int AS anyo,
    COALESCE(m.nombre, i.nombre, 'Canarias') AS nombre,
    m.tipo_municipio,
    i.nombre AS isla
  FROM hogares h
  LEFT JOIN municipios m ON h.municipio_id = m.id
  LEFT JOIN islas      i ON h.isla_id      = i.id
  WHERE h.year >= '1981-01-01'
    AND h.miembros IS NOT NULL
  ORDER BY h.ambito, m.tipo_municipio, m.nombre, anyo
")

dbDisconnect(con)

datos <- raw |>
  mutate(
    territorio_id = case_when(
      ambito == "canarias"  ~ "CAN",
      ambito == "isla"      ~ paste0("I", isla_id),
      ambito == "municipio" ~ paste0("M", municipio_id)
    )
  )

# -----------------------------------------------------------------------------
# 2. Análisis numérico de pendientes por tipo de municipio
# -----------------------------------------------------------------------------

pendientes <- datos |>
  filter(ambito == "municipio") |>
  group_by(territorio_id, municipio_id, nombre, tipo_municipio, isla) |>
  summarise(
    n_anyos   = n(),
    val_1981  = miembros[anyo == 1981][1],
    val_2021  = miembros[anyo == 2021][1],
    delta     = val_2021 - val_1981,
    pendiente = coef(lm(miembros ~ anyo))[2],
    .groups   = "drop"
  )

pendiente_canarias <- datos |>
  filter(ambito == "canarias") |>
  summarise(pendiente = coef(lm(miembros ~ anyo))[2]) |>
  pull(pendiente)

pendientes <- pendientes |>
  mutate(desviacion = pendiente - pendiente_canarias)

# Resumen por tipo
cat("\n=== PENDIENTE LINEAL POR TIPO DE MUNICIPIO ===\n")
cat(sprintf("Referencia Canarias: %.4f personas/año\n\n", pendiente_canarias))

tipos_orden <- c("GRANDE", "MEDIO", "TURISTICO", "PEQUEÑO")

pendientes |>
  mutate(tipo_municipio = gsub("\u00cd", "I", tipo_municipio),
         tipo_municipio = gsub("\u00da", "U", tipo_municipio)) |>
  group_by(tipo_municipio) |>
  summarise(
    n          = n(),
    pend_media = mean(pendiente, na.rm = TRUE),
    pend_min   = min(pendiente, na.rm = TRUE),
    pend_max   = max(pendiente, na.rm = TRUE),
    desv_sd    = sd(desviacion, na.rm = TRUE),
    .groups    = "drop"
  ) |>
  as.data.frame() |>
  print(row.names = FALSE)

cat("\n--- Top 10 mayor desviacion respecto a Canarias (por tipo) ---\n")
pendientes |>
  arrange(desc(abs(desviacion))) |>
  slice_head(n = 10) |>
  select(nombre, tipo_municipio, isla, val_1981, val_2021, delta, pendiente, desviacion) |>
  as.data.frame() |>
  print(row.names = FALSE)

# -----------------------------------------------------------------------------
# 3. PDF con pequeños múltiplos agrupados por tipo de municipio
# -----------------------------------------------------------------------------

# Rango Y común a todos los municipios
y_min <- floor(min(datos$miembros[datos$ambito == "municipio"], na.rm = TRUE) * 10) / 10
y_max <- ceiling(max(datos$miembros[datos$ambito == "municipio"], na.rm = TRUE) * 10) / 10

tema_base <- theme_minimal(base_size = 8) +
  theme(
    panel.grid.minor  = element_blank(),
    strip.text        = element_text(size = 6.5, face = "bold"),
    axis.text.x       = element_text(angle = 45, hjust = 1, size = 5),
    axis.text.y       = element_text(size = 6),
    plot.title        = element_text(size = 10, face = "bold"),
    plot.subtitle     = element_text(size = 7, color = "grey40")
  )

# Colores por tipo
colores_tipo <- c(
  "GRANDE"    = "#2166ac",
  "MEDIO"     = "#4dac26",
  "TURISTICO" = "#d6604d",
  "PEQUEÑO"   = "#8073ac"
)

pdf("auxiliares/evolucion_tamanio_hogar_tipo.pdf", width = 11, height = 8.5)

tipos <- datos |>
  filter(ambito == "municipio", !is.na(tipo_municipio)) |>
  distinct(tipo_municipio) |>
  arrange(tipo_municipio) |>
  pull(tipo_municipio)

for (tipo in tipos) {

  dat_tipo <- datos |>
    filter(ambito == "municipio", tipo_municipio == tipo) |>
    arrange(nombre, anyo)

  # Referencia: media del tipo por año
  ref_tipo <- dat_tipo |>
    group_by(anyo) |>
    summarise(miembros_ref = mean(miembros, na.rm = TRUE), .groups = "drop")

  n_mun <- n_distinct(dat_tipo$nombre)
  ncols  <- if (n_mun <= 6) 3 else if (n_mun <= 12) 4 else 5

  # Normalizar nombre del tipo para título (sin tildes)
  tipo_label <- tipo
  tipo_label <- gsub("\u00d1", "N",  tipo_label)  # Ñ
  tipo_label <- gsub("\u00cd", "I",  tipo_label)  # Í
  tipo_label <- gsub("\u00da", "U",  tipo_label)  # Ú

  color <- colores_tipo[tipo_label]
  if (is.na(color)) color <- "steelblue"

  p <- ggplot(dat_tipo, aes(x = anyo, y = miembros)) +
    geom_line(data = cross_join(ref_tipo, dat_tipo |> distinct(nombre)),
              aes(x = anyo, y = miembros_ref),
              color = "grey70", linewidth = 0.5, linetype = "dashed",
              inherit.aes = FALSE) +
    geom_line(color = color, linewidth = 0.7) +
    geom_point(color = color, size = 1.5) +
    geom_text(aes(label = sprintf("%.2f", miembros)),
              vjust = -0.8, size = 2.0, color = "grey30") +
    facet_wrap(~nombre, ncol = ncols) +
    scale_x_continuous(breaks = c(1981, 1991, 2001, 2011, 2021)) +
    scale_y_continuous(limits = c(y_min, y_max + 0.2)) +
    labs(
      title    = paste0("Tamano medio del hogar - Municipios ", tipo_label, " (1981-2021)"),
      subtitle = "Linea discontinua gris: media del tipo",
      x = NULL, y = "Personas / hogar"
    ) +
    tema_base

  print(p)
}

dev.off()

cat("\nPDF generado en: auxiliares/evolucion_tamanio_hogar_tipo.pdf\n")
