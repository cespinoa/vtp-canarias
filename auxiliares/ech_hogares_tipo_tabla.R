# =============================================================================
# ech_hogares_tipo_tabla.R
# Tabla de evolución de hogares por tipo (2013–2021), Canarias
#
# Fuentes combinadas en ech_hogares_tipo:
#   ECH 2013–2020  (INE op.274) — miles de hogares
#   ECEPOV 2021    (INE tabla 56531)
#   CENSO 2021     (nucleos_censales) — solo "Dos o más núcleos"
#
# Salida:
#   auxiliares/ech_hogares_tipo_tabla.pdf
# =============================================================================

library(RPostgres)
library(DBI)
library(dplyr)
library(tidyr)
library(flextable)
library(officer)
library(dotenv)

load_dot_env(".env")

con <- dbConnect(RPostgres::Postgres(),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  dbname   = Sys.getenv("DB_NAME"),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASS")
)

datos <- dbGetQuery(con, "
  SELECT anyo, tipo_hogar, hogares_miles, fuente
  FROM ech_hogares_tipo
  ORDER BY tipo_hogar, anyo
")

dbDisconnect(con)

# -----------------------------------------------------------------------------
# 1. Normalizar nombres para alinear ECH y ECEPOV
# -----------------------------------------------------------------------------

# Orden de filas y etiqueta canónica para mostrar
orden_filas <- c(
  "Hogar unipersonal",
  "Pareja sin hijos",
  "Pareja con hijos",
  "Hogar monoparental",
  "Núcleo familiar con otras personas",
  "Personas sin núcleo entre sí",
  "Otros tipos de hogar (ECEPOV)",
  "Dos o más núcleos familiares"
)

datos_norm <- datos |>
  mutate(categoria = case_when(
    tipo_hogar == "Hogar unipersonal"
      ~ "Hogar unipersonal",
    tipo_hogar %in% c("Pareja sin hijos que convivan en el hogar",
                      "Pareja sin hijos que conviven en el hogar")
      ~ "Pareja sin hijos",
    tipo_hogar %in% c("Pareja con hijos que convivan en el hogar: Total",
                      "Pareja con hijos que conviven en el hogar")
      ~ "Pareja con hijos",
    tipo_hogar %in% c("Hogar monoparental",
                      "Padre/madre sólo/a con hijos que conviven en el hogar")
      ~ "Hogar monoparental",
    tipo_hogar == "Núcleo familiar con otras personas que no forman núcleo familiar"
      ~ "Núcleo familiar con otras personas",
    tipo_hogar == "Personas que no forman ningún núcleo familiar entre sí"
      ~ "Personas sin núcleo entre sí",
    tipo_hogar == "Otros tipos de hogar"
      ~ "Otros tipos de hogar (ECEPOV)",
    tipo_hogar == "Dos o más núcleos familiares"
      ~ "Dos o más núcleos familiares",
    TRUE ~ tipo_hogar
  )) |>
  mutate(categoria = factor(categoria, levels = orden_filas))

# -----------------------------------------------------------------------------
# 2. Pivotar: filas = categoría, columnas = año
# -----------------------------------------------------------------------------

anyos_ech    <- as.character(2013:2020)
anyos_ecepov <- "2021"
anyos_orden  <- c(anyos_ech, anyos_ecepov)

tabla_wide <- datos_norm |>
  select(categoria, anyo, hogares_miles) |>
  pivot_wider(names_from = anyo, values_from = hogares_miles) |>
  # Forzar orden cronológico de columnas (pivot no garantiza orden)
  select(categoria, any_of(anyos_orden)) |>
  arrange(categoria)

# -----------------------------------------------------------------------------
# 3. Totales por año (suma de categorías no solapadas)
# Para ECH 2013–2020: todas las filas excepto "Otros tipos de hogar (ECEPOV)"
# Para 2021 ECEPOV: todas las filas excepto las ECH-exclusivas
# -----------------------------------------------------------------------------

cats_ech_exclusivas <- c(
  "Núcleo familiar con otras personas",
  "Personas sin núcleo entre sí"
)
cats_ecepov_exclusivas <- c("Otros tipos de hogar (ECEPOV)")

totales_ech <- datos_norm |>
  filter(anyo %in% 2013:2020,
         !categoria %in% cats_ecepov_exclusivas) |>
  group_by(anyo) |>
  summarise(total = sum(hogares_miles, na.rm = TRUE), .groups = "drop")

totales_ecepov <- datos_norm |>
  filter(anyo == 2021,
         !categoria %in% cats_ech_exclusivas) |>
  summarise(anyo = 2021, total = sum(hogares_miles, na.rm = TRUE))

totales <- bind_rows(totales_ech, totales_ecepov) |>
  mutate(categoria = factor("TOTAL", levels = c(levels(tabla_wide$categoria), "TOTAL"))) |>
  pivot_wider(names_from = anyo, values_from = total)

tabla_final <- bind_rows(tabla_wide, totales)

# -----------------------------------------------------------------------------
# 4. Formatear: redondear a 1 decimal, NA → "—"
# -----------------------------------------------------------------------------

formatear <- function(x) {
  ifelse(is.na(x), "—",
         gsub("\\.", ",", formatC(round(x, 1), format = "f", digits = 1)))
}

tabla_fmt <- tabla_final |>
  mutate(across(where(is.numeric), formatear)) |>
  mutate(categoria = as.character(categoria))

# -----------------------------------------------------------------------------
# 5. Tabla flextable
# -----------------------------------------------------------------------------

col_names <- names(tabla_fmt)  # "categoria", "2013", ..., "2021"
anyos_all  <- col_names[-1]

# Índices de fila para cada grupo
idx_ecepov_excl <- which(tabla_fmt$categoria %in% cats_ecepov_exclusivas)
idx_ech_excl    <- which(tabla_fmt$categoria %in% cats_ech_exclusivas)
idx_total       <- which(tabla_fmt$categoria == "TOTAL")
col_idx_2021    <- which(col_names == "2021")

ft <- flextable(tabla_fmt) |>
  set_header_labels(categoria = "Tipo de hogar") |>
  # Spanners en cabecera
  add_header_row(
    values    = c("", "ECH (Encuesta Continua de Hogares)", "ECEPOV / Censo"),
    colwidths = c(1, length(anyos_ech), 1)
  ) |>
  # Alineación
  align(align = "left",  part = "all", j = 1) |>
  align(align = "right", part = "all", j = -1) |>
  # Ancho columnas
  width(j = 1, width = 2.2) |>
  width(j = -1, width = 0.72) |>
  # Fila TOTAL: negrita + fondo gris
  bold(i = idx_total, part = "body") |>
  bg(i = idx_total, bg = "#f0f0f0", part = "body") |>
  # Columna 2021: fondo crema
  bg(j = col_idx_2021, bg = "#fff8e1", part = "body") |>
  bg(j = col_idx_2021, bg = "#fff0c0", part = "header") |>
  # Filas exclusivas de ECEPOV: fondo crema
  bg(i = idx_ecepov_excl, bg = "#fff8e1", part = "body") |>
  # Texto gris para "—" en filas ECH-exclusivas en columna 2021
  color(i = idx_ech_excl,    j = col_idx_2021, color = "#aaaaaa", part = "body") |>
  color(i = idx_ecepov_excl, j = seq_along(anyos_ech) + 1, color = "#aaaaaa", part = "body") |>
  italic(i = idx_ecepov_excl, j = 1, part = "body") |>
  italic(i = idx_ech_excl,    j = 1, part = "body") |>
  # Bordes
  border_outer(part = "all", border = fp_border(width = 1.5)) |>
  border_inner_h(part = "header", border = fp_border(width = 0.5)) |>
  hline(i = idx_total - 1, border = fp_border(width = 1, style = "dashed")) |>
  # Fuente
  fontsize(size = 9.5, part = "all") |>
  fontsize(size = 8.5, i = idx_ecepov_excl, part = "body") |>
  fontsize(size = 8.5, i = idx_ech_excl,    part = "body") |>
  set_caption(
    caption = "Hogares por tipo de hogar — Canarias (miles). Evolución 2013–2021.",
    style   = "Table Caption"
  ) |>
  add_footer_lines(paste0(
    "ECH 2013–2020: Encuesta Continua de Hogares (INE op. 274). ",
    "ECEPOV 2021: INE tabla 56531. ",
    "CENSO 2021 (Dos o más núcleos): nucleos_censales.\n",
    "Las filas en cursiva no tienen equivalente directo entre fuentes: ",
    "«Núcleo familiar con otras personas» y «Personas sin núcleo entre sí» ",
    "quedan absorbidas en «Otros tipos de hogar (ECEPOV)» en 2021."
  )) |>
  fontsize(size = 7.5, part = "footer") |>
  color(color = "#555555", part = "footer")

# -----------------------------------------------------------------------------
# 6. Exportar PDF vía officer
# -----------------------------------------------------------------------------

output_path <- "auxiliares/ech_hogares_tipo_tabla.pdf"

doc <- read_docx() |>
  body_add_par("Hogares por tipo de hogar — Canarias (miles)", style = "heading 1") |>
  body_add_par("Evolución 2013–2021 · Fuentes: ECH, ECEPOV, Censo 2021",
               style = "Normal") |>
  body_add_par("", style = "Normal") |>
  body_add_flextable(ft)

# Guardar como docx temporal y convertir a PDF con LibreOffice
tmp_docx <- tempfile(fileext = ".docx")
print(doc, target = tmp_docx)

lo_bin <- Sys.which(c("libreoffice", "soffice"))
lo_bin <- lo_bin[lo_bin != ""][1]

if (!is.na(lo_bin)) {
  conv <- system2(lo_bin,
    args = c("--headless", "--convert-to", "pdf", "--outdir",
             dirname(output_path), tmp_docx),
    stdout = TRUE, stderr = TRUE
  )
  pdf_tmp <- sub("\\.docx$", ".pdf", tmp_docx)
  if (file.exists(pdf_tmp)) {
    file.rename(pdf_tmp, output_path)
    cat("PDF generado en:", output_path, "\n")
  } else {
    cat("LibreOffice no generó PDF. Mensajes:\n", paste(conv, collapse = "\n"), "\n")
  }
} else {
  output_docx <- sub("\\.pdf$", ".docx", output_path)
  file.copy(tmp_docx, output_docx, overwrite = TRUE)
  cat("LibreOffice no encontrado — guardado como DOCX en:", output_docx, "\n")
}
