# =============================================================================
# evolucion_tamanio_hogar.R
# Análisis de la evolución del tamaño medio del hogar por territorio (1981–2021)
#
# Salidas:
#   - Tabla de pendientes y anomalías por territorio (consola)
#   - PDF con pequeños múltiplos por territorio (auxiliares/evolucion_tamanio_hogar.pdf)
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
    i.nombre AS isla
  FROM hogares h
  LEFT JOIN municipios m ON h.municipio_id = m.id
  LEFT JOIN islas      i ON h.isla_id      = i.id
  WHERE h.year >= '1981-01-01'
    AND h.miembros IS NOT NULL
  ORDER BY h.ambito, h.isla_id, h.municipio_id, anyo
")

# Punto actual desde full_snapshots (tamanio_hogar_actual, si existe)
actual_snap <- tryCatch(dbGetQuery(con, "
  SELECT
    fs.ambito,
    fs.isla_id,
    fs.municipio_id,
    fs.tamanio_hogar_actual AS miembros,
    EXTRACT(YEAR FROM fs.fecha_calculo)::int AS anyo,
    COALESCE(m.nombre, i.nombre, 'Canarias') AS nombre,
    i.nombre AS isla
  FROM full_snapshots fs
  LEFT JOIN municipios m ON fs.municipio_id = m.id
  LEFT JOIN islas      i ON fs.isla_id      = i.id
  WHERE fs.fecha_calculo = (SELECT MAX(fecha_calculo) FROM full_snapshots)
    AND fs.tamanio_hogar_actual IS NOT NULL
    AND fs.ambito IN ('canarias', 'isla', 'municipio')
"), error = function(e) NULL)

dbDisconnect(con)

# Etiqueta única por territorio (para facilitar el análisis)
datos <- raw |>
  mutate(
    territorio_id = case_when(
      ambito == "canarias"  ~ "CAN",
      ambito == "isla"      ~ paste0("I", isla_id),
      ambito == "municipio" ~ paste0("M", municipio_id)
    ),
    tipo_punto = "censal"
  )

if (!is.null(actual_snap) && nrow(actual_snap) > 0) {
  actual_snap <- actual_snap |>
    mutate(
      territorio_id = case_when(
        ambito == "canarias"  ~ "CAN",
        ambito == "isla"      ~ paste0("I", isla_id),
        ambito == "municipio" ~ paste0("M", municipio_id)
      ),
      tipo_punto = "actual"
    )
  datos <- bind_rows(datos, actual_snap)
  cat(sprintf("Añadidos %d puntos 'actual' desde full_snapshots (año %d).\n",
              nrow(actual_snap), actual_snap$anyo[1]))
} else {
  cat("Sin datos de tamanio_hogar_actual en full_snapshots; solo series censales.\n")
}

# -----------------------------------------------------------------------------
# 2. Análisis numérico de pendientes
# -----------------------------------------------------------------------------

# Pendiente por regresión lineal simple (miembros ~ anyo)
pendientes <- datos |>
  group_by(territorio_id, ambito, isla_id, nombre, isla) |>
  summarise(
    n_anyos   = n(),
    val_1981  = miembros[anyo == 1981][1],
    val_2021  = miembros[anyo == 2021][1],
    delta     = val_2021 - val_1981,
    pendiente = coef(lm(miembros ~ anyo))[2],
    .groups   = "drop"
  )

pendiente_canarias <- pendientes |>
  filter(ambito == "canarias") |>
  pull(pendiente)

pendientes <- pendientes |>
  mutate(desviacion = pendiente - pendiente_canarias)

# Resumen en consola
cat("\n=== PENDIENTE LINEAL (personas/hogar por año) ===\n")
cat(sprintf("Canarias: %.4f personas/año\n\n", pendiente_canarias))

cat("--- ISLAS ---\n")
pendientes |>
  filter(ambito == "isla") |>
  arrange(pendiente) |>
  select(nombre, val_1981, val_2021, delta, pendiente, desviacion) |>
  as.data.frame() |>
  print(row.names = FALSE)

cat("\n--- MUNICIPIOS: top 10 mayor desviación respecto a Canarias ---\n")
pendientes |>
  filter(ambito == "municipio") |>
  arrange(desc(abs(desviacion))) |>
  slice_head(n = 10) |>
  select(nombre, isla, val_1981, val_2021, delta, pendiente, desviacion) |>
  as.data.frame() |>
  print(row.names = FALSE)

cat("\n--- MUNICIPIOS: distribución de desviaciones ---\n")
mun_desv <- pendientes |> filter(ambito == "municipio") |> pull(desviacion)
print(summary(mun_desv))
cat(sprintf("SD desviaciones: %.4f\n", sd(mun_desv, na.rm = TRUE)))

# -----------------------------------------------------------------------------
# 3. PDF con pequeños múltiplos
# -----------------------------------------------------------------------------

anyo_actual <- if (!is.null(actual_snap) && nrow(actual_snap) > 0) actual_snap$anyo[1] else NA_integer_
hay_actual  <- !is.na(anyo_actual)

# Referencia Canarias (solo puntos censales) para superponerla en todos los gráficos
ref_canarias <- datos |>
  filter(ambito == "canarias", tipo_punto == "censal") |>
  select(anyo, miembros_can = miembros)

# Rango Y común
y_min <- floor(min(datos$miembros, na.rm = TRUE) * 10) / 10
y_max <- ceiling(max(datos$miembros, na.rm = TRUE) * 10) / 10

x_breaks <- c(1981, 1991, 2001, 2011, 2021)
if (hay_actual) x_breaks <- c(x_breaks, anyo_actual)

subtitulo_base <- "Personas por hogar según Censos de Población"
if (hay_actual) subtitulo_base <- paste0(subtitulo_base,
  sprintf(" + punto actual (%d, calculado)", anyo_actual))

tema_base <- theme_minimal(base_size = 8) +
  theme(
    panel.grid.minor  = element_blank(),
    strip.text        = element_text(size = 6.5, face = "bold"),
    axis.text.x       = element_text(angle = 45, hjust = 1, size = 5),
    axis.text.y       = element_text(size = 6),
    plot.title        = element_text(size = 10, face = "bold"),
    plot.subtitle     = element_text(size = 7, color = "grey40")
  )

pdf("auxiliares/evolucion_tamanio_hogar.pdf", width = 11, height = 8.5)

# --- Página 1: Canarias + islas ---
dat_p1 <- datos |>
  filter(ambito %in% c("canarias", "isla")) |>
  mutate(nombre = factor(nombre, levels = c(
    "Canarias",
    sort(unique(nombre[ambito == "isla"]))
  )))

p1 <- ggplot(dat_p1, aes(x = anyo, y = miembros)) +
  geom_line(data = filter(dat_p1, tipo_punto == "censal"),
            color = "#2166ac", linewidth = 0.8) +
  geom_point(aes(shape = tipo_punto, color = tipo_punto), size = 1.8) +
  geom_text(aes(label = sprintf("%.2f", miembros)),
            vjust = -0.8, size = 2.2, color = "grey30") +
  scale_shape_manual(values = c(censal = 16, actual = 17), guide = "none") +
  scale_color_manual(values = c(censal = "#2166ac", actual = "#d6604d"), guide = "none") +
  facet_wrap(~nombre, ncol = 4) +
  scale_x_continuous(breaks = x_breaks) +
  scale_y_continuous(limits = c(y_min, y_max + 0.2)) +
  labs(
    title    = "Tamano medio del hogar (1981-actual)",
    subtitle = subtitulo_base,
    x = NULL, y = "Personas / hogar"
  ) +
  tema_base

print(p1)

# --- Páginas 2–8: municipios por isla ---
islas_orden <- datos |>
  filter(ambito == "isla", tipo_punto == "censal") |>
  distinct(isla_id, nombre) |>
  arrange(nombre)

for (i in seq_len(nrow(islas_orden))) {
  isla_id_i  <- islas_orden$isla_id[i]
  isla_nombre <- islas_orden$nombre[i]

  dat_isla <- datos |>
    filter(ambito == "municipio", isla_id == isla_id_i) |>
    arrange(nombre, anyo)

  n_mun <- n_distinct(dat_isla$nombre)
  ncols  <- if (n_mun <= 4) 2 else if (n_mun <= 9) 3 else 4

  # Referencia isla (solo censales) para la línea gris de fondo
  ref_isla <- datos |>
    filter(ambito == "isla", isla_id == isla_id_i, tipo_punto == "censal") |>
    select(anyo, miembros_ref = miembros)

  p <- ggplot(dat_isla, aes(x = anyo, y = miembros)) +
    geom_line(data = cross_join(ref_isla, dat_isla |> distinct(nombre)),
              aes(x = anyo, y = miembros_ref),
              color = "grey70", linewidth = 0.5, linetype = "dashed",
              inherit.aes = FALSE) +
    geom_line(data = filter(dat_isla, tipo_punto == "censal"),
              color = "#d6604d", linewidth = 0.7) +
    geom_point(aes(shape = tipo_punto, color = tipo_punto), size = 1.5) +
    geom_text(aes(label = sprintf("%.2f", miembros)),
              vjust = -0.8, size = 2.0, color = "grey30") +
    scale_shape_manual(values = c(censal = 16, actual = 17), guide = "none") +
    scale_color_manual(values = c(censal = "#d6604d", actual = "#b2182b"), guide = "none") +
    facet_wrap(~nombre, ncol = ncols) +
    scale_x_continuous(breaks = x_breaks) +
    scale_y_continuous(limits = c(y_min, y_max + 0.2)) +
    labs(
      title    = paste0("Tamano medio del hogar - ", isla_nombre, " (1981-actual)"),
      subtitle = "Línea discontinua gris: referencia de isla. Triángulo rojo: valor calculado actual",
      x = NULL, y = "Personas / hogar"
    ) +
    tema_base

  print(p)
}

dev.off()

cat("\nPDF generado en: auxiliares/evolucion_tamanio_hogar.pdf\n")
