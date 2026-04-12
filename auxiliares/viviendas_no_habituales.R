# =============================================================================
# viviendas_no_habituales.R
# Carga la tabla viviendas_no_habituales_censos y genera PDF con
# gráficos de pendiente (índice 2001=100) por isla y por tipo de municipio.
#
# Fuentes:
#   - 2001 y 2011: "viviendas no principales" (Censo INE, encuesta de campo)
#   - 2021: vacías + esporádicas (Censo INE, metodología consumo eléctrico)
#   ⚠ Los tres valores NO son directamente comparables entre sí.
#
# Salidas:
#   - BD: tabla viviendas_no_habituales_censos (TRUNCATE + reload)
#   - PDF: auxiliares/viviendas_no_habituales.pdf
#
# Uso:
#   Rscript auxiliares/viviendas_no_habituales.R
#   Rscript auxiliares/viviendas_no_habituales.R ruta/csv
# =============================================================================

library(tidyverse)
library(DBI)
library(RPostgres)
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
# 1. Localizar CSV 2001/2011
# -----------------------------------------------------------------------------
args     <- commandArgs(trailingOnly = TRUE)
csv_path <- if (length(args) > 0) args[1] else {
  candidatos <- Sys.glob("descarga_datos/tmp/viviendas_no_hab_????????.csv")
  if (length(candidatos) == 0) stop("No se encontró CSV en descarga_datos/tmp/")
  tail(sort(candidatos), 1)
}
cat("CSV fuente:", csv_path, "\n")

# -----------------------------------------------------------------------------
# 2. Leer CSV + tablas maestras
# -----------------------------------------------------------------------------
censo_hist <- read_csv(csv_path, show_col_types = FALSE,
  col_types = cols(
    codigo_ine  = col_character(),
    nombre      = col_character(),
    no_hab_2011 = col_integer(),
    no_hab_2001 = col_integer()
  ))

municipios_db <- dbGetQuery(con, "
  SELECT m.id, m.codigo_ine, m.nombre, m.tipo_municipio,
         i.id AS isla_id, i.nombre AS isla
  FROM municipios m
  JOIN islas i ON m.isla_id = i.id
")

# 2021: vacias + esporadicas
viv_2021 <- dbGetQuery(con, "
  SELECT municipio_id, (vacias + esporadicas) AS no_hab_2021
  FROM viviendas_municipios
  WHERE ambito = 'municipio'
")

# -----------------------------------------------------------------------------
# 3. Cruzar
# -----------------------------------------------------------------------------
datos <- municipios_db %>%
  left_join(censo_hist %>% select(-nombre), by = "codigo_ine") %>%
  left_join(viv_2021,   by = c("id" = "municipio_id")) %>%
  arrange(isla, nombre)

cat(sprintf("\nMunicipios con dato 2001/2011: %d de %d\n",
            sum(!is.na(datos$no_hab_2001)), nrow(datos)))
cat(sprintf("Municipios con dato 2021:      %d de %d\n",
            sum(!is.na(datos$no_hab_2021)), nrow(datos)))

# Total Canarias para referencia
cat("\nTotales Canarias:\n")
cat(sprintf("  2001: %s  2011: %s  2021: %s\n",
  format(sum(datos$no_hab_2001, na.rm=TRUE), big.mark=","),
  format(sum(datos$no_hab_2011, na.rm=TRUE), big.mark=","),
  format(sum(datos$no_hab_2021, na.rm=TRUE), big.mark=",")))

# -----------------------------------------------------------------------------
# 4. Cargar tabla en BD
# -----------------------------------------------------------------------------
tabla_bd <- datos %>%
  select(municipio_id = id, no_hab_2001, no_hab_2011, no_hab_2021)

cat("\nCargando viviendas_no_habituales_censos...\n")
dbBegin(con)
tryCatch({
  dbExecute(con, "TRUNCATE TABLE viviendas_no_habituales_censos")
  dbWriteTable(con, "viviendas_no_habituales_censos", tabla_bd,
               append = TRUE, row.names = FALSE)
  dbCommit(con)
  cat(sprintf("Cargados: %d registros.\n", nrow(tabla_bd)))
}, error = function(e) {
  dbRollback(con)
  stop("Error en la carga: ", conditionMessage(e))
})

# -----------------------------------------------------------------------------
# 5. Preparar datos para gráficos
# -----------------------------------------------------------------------------

# Solo municipios con dato 2001 (80 de 88)
graf <- datos %>%
  filter(!is.na(no_hab_2001), !is.na(no_hab_2021)) %>%
  mutate(
    idx_2001 = 100,
    idx_2011 = round(100 * no_hab_2011 / no_hab_2001, 1),
    idx_2021 = round(100 * no_hab_2021 / no_hab_2001, 1)
  )

# Formato largo para ggplot
graf_long <- graf %>%
  select(nombre, isla, isla_id, tipo_municipio,
         no_hab_2001, no_hab_2011, no_hab_2021,
         idx_2001, idx_2011, idx_2021) %>%
  pivot_longer(
    cols      = c(idx_2001, idx_2011, idx_2021),
    names_to  = "periodo",
    values_to = "indice"
  ) %>%
  mutate(anyo = as.integer(str_extract(periodo, "\\d{4}")))

# Referencia de isla: suma real de municipios (no media)
ref_isla <- graf %>%
  group_by(isla) %>%
  summarise(
    ref_2001 = sum(no_hab_2001, na.rm = TRUE),
    ref_2011 = sum(no_hab_2011, na.rm = TRUE),
    ref_2021 = sum(no_hab_2021, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    idx_2001 = 100,
    idx_2011 = round(100 * ref_2011 / ref_2001, 1),
    idx_2021 = round(100 * ref_2021 / ref_2001, 1)
  ) %>%
  pivot_longer(cols = c(idx_2001, idx_2011, idx_2021),
               names_to = "periodo", values_to = "ref_idx") %>%
  mutate(anyo = as.integer(str_extract(periodo, "\\d{4}")))

# Referencia por tipo: suma real
ref_tipo <- graf %>%
  group_by(tipo_municipio) %>%
  summarise(
    ref_2001 = sum(no_hab_2001, na.rm = TRUE),
    ref_2011 = sum(no_hab_2011, na.rm = TRUE),
    ref_2021 = sum(no_hab_2021, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    idx_2001 = 100,
    idx_2011 = round(100 * ref_2011 / ref_2001, 1),
    idx_2021 = round(100 * ref_2021 / ref_2001, 1)
  ) %>%
  pivot_longer(cols = c(idx_2001, idx_2011, idx_2021),
               names_to = "periodo", values_to = "ref_idx") %>%
  mutate(anyo = as.integer(str_extract(periodo, "\\d{4}")))

# -----------------------------------------------------------------------------
# 6. Tema y paleta
# -----------------------------------------------------------------------------
tema_base <- theme_minimal(base_size = 8) +
  theme(
    panel.grid.minor  = element_blank(),
    panel.grid.major.x = element_blank(),
    strip.text        = element_text(size = 6, face = "bold"),
    axis.text.x       = element_text(size = 6),
    axis.text.y       = element_text(size = 6),
    plot.title        = element_text(size = 10, face = "bold"),
    plot.subtitle     = element_text(size = 7, color = "grey40"),
    plot.caption      = element_text(size = 5.5, color = "grey50")
  )

COLOR_MUN  <- "#d6604d"
COLOR_REF  <- "grey60"

colores_tipo <- c(
  "GRANDE"    = "#2166ac",
  "MEDIO"     = "#4dac26",
  "TURISTICO" = "#d6604d",
  "PEQUEÑO"   = "#8073ac"
)

CAPTION <- paste0(
  "2001/2011: viviendas no principales (vacías + secundarias), encuesta de campo.\n",
  "2021: vacías + esporádicas, metodología consumo eléctrico. Las tres cifras no son directamente comparables.\n",
  "Base 2001 = 100. Solo municipios >2.000 hab."
)

# -----------------------------------------------------------------------------
# 7. Función auxiliar para una página de isla o tipo
# -----------------------------------------------------------------------------
pagina_isla <- function(dat_mun, ref_dat, titulo, subtitulo) {
  n_mun <- n_distinct(dat_mun$nombre)
  ncols  <- if (n_mun <= 4) 2 else if (n_mun <= 9) 3 else 4

  ggplot(dat_mun, aes(x = anyo, y = indice)) +
    geom_line(
      data = cross_join(ref_dat, dat_mun %>% distinct(nombre)),
      aes(x = anyo, y = ref_idx),
      color = COLOR_REF, linewidth = 0.5, linetype = "dashed",
      inherit.aes = FALSE
    ) +
    geom_hline(yintercept = 100, color = "grey80", linewidth = 0.3) +
    geom_line(color = COLOR_MUN, linewidth = 0.8) +
    geom_point(color = COLOR_MUN, size = 1.8) +
    geom_text(aes(label = round(indice)),
              vjust = -0.9, size = 2.0, color = "grey30") +
    facet_wrap(~nombre, ncol = ncols) +
    scale_x_continuous(breaks = c(2001, 2011, 2021)) +
    labs(
      title    = titulo,
      subtitle = subtitulo,
      x = NULL, y = "Índice (2001 = 100)",
      caption  = CAPTION
    ) +
    tema_base
}

# -----------------------------------------------------------------------------
# 8. Generar PDF
# -----------------------------------------------------------------------------
pdf("auxiliares/viviendas_no_habituales.pdf", width = 11, height = 8.5)

# --- Páginas 1–7: por isla ---
islas_orden <- graf %>%
  distinct(isla_id, isla) %>%
  arrange(isla)

for (i in seq_len(nrow(islas_orden))) {
  isla_id_i  <- islas_orden$isla_id[i]
  isla_nombre <- islas_orden$isla[i]

  dat_i  <- graf_long %>% filter(isla_id == isla_id_i)
  ref_i  <- ref_isla   %>% filter(isla  == isla_nombre)

  subtitulo <- paste0(
    "Línea gris discontinua: evolución agregada de la isla\n",
    sprintf("Total isla 2001: %s  →  2011: %s  →  2021: %s",
      format(sum(graf$no_hab_2001[graf$isla_id == isla_id_i]), big.mark=","),
      format(sum(graf$no_hab_2011[graf$isla_id == isla_id_i], na.rm=TRUE), big.mark=","),
      format(sum(graf$no_hab_2021[graf$isla_id == isla_id_i], na.rm=TRUE), big.mark=","))
  )

  print(pagina_isla(dat_i, ref_i,
    titulo    = paste0("Viviendas no habituales — ", isla_nombre),
    subtitulo = subtitulo))
}

# --- Páginas 8–11: por tipo de municipio ---
tipos_orden <- c("GRANDE", "MEDIO", "TURISTICO", "PEQUEÑO")
tipos_etiqueta <- c(
  "GRANDE"    = "Grandes",
  "MEDIO"     = "Medios",
  "TURISTICO" = "Turísticos",
  "PEQUEÑO"   = "Pequeños"
)

for (tipo in tipos_orden) {
  dat_t <- graf_long %>% filter(tipo_municipio == tipo)
  ref_t <- ref_tipo  %>% filter(tipo_municipio == tipo)
  if (nrow(dat_t) == 0) next

  color_t <- colores_tipo[tipo]
  n_mun   <- n_distinct(dat_t$nombre)
  ncols   <- if (n_mun <= 6) 3 else if (n_mun <= 12) 4 else 5

  subtitulo <- paste0(
    "Línea gris discontinua: evolución agregada del grupo\n",
    sprintf("Total grupo 2001: %s  →  2011: %s  →  2021: %s",
      format(sum(graf$no_hab_2001[graf$tipo_municipio == tipo]), big.mark=","),
      format(sum(graf$no_hab_2011[graf$tipo_municipio == tipo], na.rm=TRUE), big.mark=","),
      format(sum(graf$no_hab_2021[graf$tipo_municipio == tipo], na.rm=TRUE), big.mark=","))
  )

  p <- ggplot(dat_t, aes(x = anyo, y = indice)) +
    geom_line(
      data = cross_join(ref_t, dat_t %>% distinct(nombre)),
      aes(x = anyo, y = ref_idx),
      color = COLOR_REF, linewidth = 0.5, linetype = "dashed",
      inherit.aes = FALSE
    ) +
    geom_hline(yintercept = 100, color = "grey80", linewidth = 0.3) +
    geom_line(color = color_t, linewidth = 0.8) +
    geom_point(color = color_t, size = 1.8) +
    geom_text(aes(label = round(indice)),
              vjust = -0.9, size = 2.0, color = "grey30") +
    facet_wrap(~nombre, ncol = ncols) +
    scale_x_continuous(breaks = c(2001, 2011, 2021)) +
    labs(
      title    = paste0("Viviendas no habituales — Municipios ", tipos_etiqueta[tipo]),
      subtitle = subtitulo,
      x = NULL, y = "Índice (2001 = 100)",
      caption  = CAPTION
    ) +
    tema_base

  print(p)
}

dev.off()
cat("\nPDF generado: auxiliares/viviendas_no_habituales.pdf\n")

dbDisconnect(con)
cat("Completado.\n")
