#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: generar_documentacion.R
# Genera las páginas HTML del libro de documentación de la base de datos
# VTP-Canarias para importar en Drupal (módulo Book).
#
# Cada fichero generado corresponde a un nodo del libro.
# Los fragmentos HTML no incluyen <html>/<head>: se pegan directamente
# en el campo "Body" del nodo Drupal con formato "Full HTML".
#
# Uso:
#   Rscript documentacion/generar_documentacion.R
# ==============================================================================

source("importar_gobcan/helper.R")
con <- conecta_db()

OUT <- "documentacion/html"
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

# ---- Helpers -----------------------------------------------------------------

h <- function(...) paste0(...)

# Operador de concatenación
`%+%` <- paste0

tag <- function(t, contenido, cls = NULL) {
  atr <- if (!is.null(cls)) paste0(' class="', cls, '"') else ""
  paste0("<", t, atr, ">", contenido, "</", t, ">")
}

p      <- function(..., cls = NULL) tag("p",      paste0(...), cls)
h2     <- function(...) tag("h2",    paste0(...))
h3     <- function(...) tag("h3",    paste0(...))
h4     <- function(...) tag("h4",    paste0(...))
li     <- function(...) tag("li",    paste0(...))
ul     <- function(items) tag("ul",  paste(sapply(items, function(x) tag("li", x)), collapse = "\n"))
ol     <- function(items) tag("ol",  paste(sapply(items, function(x) tag("li", x)), collapse = "\n"))
code   <- function(...) tag("code",   paste0(...))
pre    <- function(...) tag("pre",    tag("code", paste0(...)))
strong <- function(...) tag("strong", paste0(...))
em     <- function(...) tag("em",     paste0(...))
hr     <- function() "<hr>"

badge <- function(x, color = "secondary") {
  paste0('<span class="badge badge-', color, '">', x, '</span>')
}

tabla_columnas <- function(tabla_db) {
  cols <- DBI::dbGetQuery(con, paste0("
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '", tabla_db, "'
    ORDER BY ordinal_position"))
  if (nrow(cols) == 0) return(p(em("Sin columnas disponibles.")))

  filas <- apply(cols, 1, function(r) {
    nul <- if (r["is_nullable"] == "NO") badge("NOT NULL", "danger") else ""
    paste0("<tr><td><code>", r["column_name"], "</code></td>",
           "<td>", r["data_type"], "</td>",
           "<td>", nul, "</td></tr>")
  })
  paste0(
    '<table class="table table-sm table-bordered">',
    "<thead><tr><th>Columna</th><th>Tipo</th><th></th></tr></thead>",
    "<tbody>", paste(filas, collapse = "\n"), "</tbody>",
    "</table>"
  )
}

n_filas <- function(tabla_db) {
  tryCatch(
    format(DBI::dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM ", tabla_db))$n,
           big.mark = "."),
    error = function(e) "?"
  )
}

info_tabla <- function(tabla_db) {
  paste0(
    '<p class="text-muted"><small>',
    strong("Tabla: "), code(tabla_db), " &nbsp;|&nbsp; ",
    strong("Filas actuales: "), n_filas(tabla_db),
    "</small></p>"
  )
}

escribir <- function(nombre, titulo, cuerpo) {
  ruta <- file.path(OUT, nombre)
  writeLines(
    c(paste0("<!-- ", titulo, " -->"), cuerpo),
    ruta, useBytes = TRUE
  )
  cat("  Generado:", ruta, "\n")
}

# ==============================================================================
# PÁGINA 0 — ÍNDICE / PORTADA
# ==============================================================================
cat("Generando página 0 — Índice...\n")

stats <- DBI::dbGetQuery(con, "
  SELECT tablename,
    (SELECT reltuples::bigint FROM pg_class WHERE relname = tablename) AS filas
  FROM pg_tables WHERE schemaname='public'
  ORDER BY tablename")

filas_tab <- apply(stats, 1, function(r) {
  f <- tryCatch(
    format(as.integer(DBI::dbGetQuery(con, paste0("SELECT COUNT(*) FROM ", r["tablename"]))$count),
           big.mark = "."),
    error = function(e) "—"
  )
  paste0("<tr><td><code>", r["tablename"], "</code></td><td>", f, "</td></tr>")
})

# Agrupamos las tablas por categoría para el índice
grupos <- list(
  "Pipeline turístico"        = c("staging_import", "alojamientos"),
  "Geografía de referencia"   = c("islas", "municipios", "localidades", "canarias",
                                   "centroides_localidad", "centroides_cp", "centroides_municipio",
                                   "callejero_portales", "portales_canarias",
                                   "mapa_municipios", "recintos_municipales", "codigos_postales"),
  "Datos estadísticos"        = c("poblacion", "pte_reglada", "pte_vacacional",
                                   "hogares", "viviendas_municipios", "superficies",
                                   "vivienda_iniciada_terminada_canarias",
                                   "historico_plazas_regladas", "turistas_llegadas"),
  "Clasificación turística"   = c("modalidades", "tipologias", "clasificaciones",
                                   "destinos_turisticos", "at_canarias_no_microdestino"),
  "Salida y visualización"    = c("base_snapshots", "full_snapshots",
                                   "diccionario_de_datos")
)

tabla_indice <- paste0(
  '<table class="table table-bordered table-sm">',
  "<thead><tr><th>Categoría</th><th>Tabla</th><th>Filas</th></tr></thead><tbody>"
)
for (cat in names(grupos)) {
  for (t in grupos[[cat]]) {
    f <- tryCatch(
      format(DBI::dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM ", t))$n, big.mark = "."),
      error = function(e) "—")
    tabla_indice <- paste0(tabla_indice,
      "<tr><td>", cat, "</td><td><code>", t, "</code></td><td>", f, "</td></tr>")
  }
}
tabla_indice <- paste0(tabla_indice, "</tbody></table>")

cuerpo_00 <- paste(
  h2("VTP-Canarias — Documentación de la base de datos"),
  p("Este libro documenta el esquema, el contenido y los procesos de carga de la base de datos ",
    strong("viviendas_canarias"), ", que integra registros turísticos, datos estadísticos ",
    "y cartografía de referencia de Canarias para el cálculo de indicadores de presión ",
    "turística sobre la población y el territorio."),
  h3("Estructura del libro"),
  ul(list(
    "<strong>Cap. 1 — Visión general y arquitectura:</strong> objetivos, stack tecnológico y flujo de datos.",
    "<strong>Cap. 2 — Pipeline de importación turística:</strong> scripts P00–P12, tablas <code>staging_import</code> y <code>alojamientos</code>.",
    "<strong>Cap. 3 — Tablas geográficas de referencia:</strong> islas, municipios, localidades, callejero y centroides.",
    "<strong>Cap. 4 — Datos estadísticos:</strong> población, PTE, hogares, viviendas, superficies, plazas históricas y turistas.",
    "<strong>Cap. 5 — Clasificación turística:</strong> modalidades, tipologías, clasificaciones, destinos turísticos.",
    "<strong>Cap. 6 — Salida calculada y visualización:</strong> snapshots, vista materializada y diccionario de datos.",
    "<strong>Cap. 7 — Scripts de descarga y mantenimiento:</strong> scripts Python de descarga desde CKAN, ISTAC e INE."
  )),
  h3("Inventario de tablas"),
  tabla_indice,
  p(em("Filas actualizadas al ", format(Sys.Date(), "%d/%m/%Y"), "."),
    ' class="text-muted"')
)

escribir("00-indice.html", "VTP-Canarias — Documentación de la base de datos", cuerpo_00)


# ==============================================================================
# PÁGINA 1 — VISIÓN GENERAL Y ARQUITECTURA
# ==============================================================================
cat("Generando página 1 — Visión general...\n")

cuerpo_01 <- paste(
  h2("Cap. 1 — Visión general y arquitectura"),

  h3("Objetivo del proyecto"),
  p("VTP-Canarias (", em("Vivienda, Turismo y Población — Canarias"), ") integra registros ",
    "oficiales de turismo, estadísticas de población y cartografía para construir indicadores ",
    "que permiten evaluar la presión que el alojamiento turístico ejerce sobre el parque ",
    "residencial, la población y el territorio en Canarias."),
  p("El ámbito de análisis cubre cuatro niveles: ",
    strong("Canarias"), " (total archipiélago), ",
    strong("isla"), " (7 islas), ",
    strong("municipio"), " (88 municipios) y ",
    strong("localidad"), " (2.346 entidades de población)."),

  h3("Stack tecnológico"),
  ul(list(
    strong("Base de datos: ") %+% "PostgreSQL 16 con extensión PostGIS (arm64, Raspberry Pi). Contenedor Docker <code>imresamu/postgis-arm64:16-3.5</code>.",
    strong("Servidor de tiles: ") %+% "Martin (Rust), contenedor Docker, expone geometrías como vector tiles.",
    strong("Frontend: ") %+% "Módulo Drupal personalizado con visor cartográfico (<code>/home/carlos/visor/</code>).",
    strong("Procesamiento: ") %+% "R directo en el sistema (sin contenedor). Librerías principales: <code>DBI</code>, <code>RPostgres</code>, <code>tidyverse</code>.",
    strong("Descarga de datos: ") %+% "Scripts Python (<code>descarga_datos/</code>) para APIs de CKAN, ISTAC e INE."
  )),

  h3("Flujo general de datos"),
  pre(
"Fuentes externas
  ├── GobCan (CKAN)   →  vv.csv, at.csv, ht.csv
  ├── ISTAC (API)     →  poblacion, PTE, hogares, plazas, turistas
  └── INE (API)       →  poblacion municipal, viviendas

Importación y geocodificación
  └── P00–P12 (importar_gobcan/)
        P00  Prepara ficheros CSV (fusiona AT+HT → at.csv)
        P01  Ingesta → staging_import
        P02  Geocodificación por callejero (fuzzy, similitud > 0.45)
        P03  Fallback: centroide de localidad
        P04  Fallback: centroide de código postal
        P05  Fallback: centroide de municipio
        P06  Rescate de coordenadas en el mar
        P07  Validación espacial (auditoría pasiva)
        P08  Asignación de municipio y localidad por geometría
        P09  Asignación de isla_id, clasificación y microdestino
        P10  Detección de duplicados
        P11  Migración staging_import → alojamientos
        P12  Informe de auditoría final

Cálculo de indicadores
  └── PT01–PT03 (informes/)
        PT01  Captura datos base → base_snapshots
        PT02  Calcula ratios y benchmarks → full_snapshots
        PT03  Reconstruye vista materializada + exporta JSONs

Visualización
  └── Drupal + Martin → visor cartográfico web"),

  h3("Cadencia de actualización"),
  ul(list(
    strong("Registros turísticos: ") %+% "frecuente (cada publicación del GobCan, típicamente mensual). Requiere ejecutar P00–P12.",
    strong("PTE vacacional: ") %+% "mensual (ISTAC publica con ~2 meses de retraso).",
    strong("PTE reglada, plazas históricas, turistas: ") %+% "anual o cuando el ISTAC actualiza.",
    strong("Población: ") %+% "el INE publica datos del padrón a principios de año para el año anterior.",
    strong("Viviendas, hogares, superficies: ") %+% "censal o puntual (cada 10 años aproximadamente).",
    strong("Indicadores (PT01–PT03): ") %+% "se ejecutan tras cualquier actualización de datos."
  ))
)

escribir("01-vision-general.html", "Cap. 1 — Visión general y arquitectura", cuerpo_01)


# ==============================================================================
# PÁGINA 2 — PIPELINE DE IMPORTACIÓN TURÍSTICA
# ==============================================================================
cat("Generando página 2 — Pipeline turístico...\n")

cuerpo_02 <- paste(
  h2("Cap. 2 — Pipeline de importación turística (P00–P12)"),

  p("El pipeline transforma los ficheros CSV publicados por el Gobierno de Canarias ",
    "en registros de producción geocodificados y auditados en la tabla ",
    code("alojamientos"), "."),

  h3("Ficheros de entrada"),
  ul(list(
    strong("vv-YYYY-MM-DD.csv") %+% " — Viviendas vacacionales inscritas en el Registro General Turístico.",
    strong("ht-YYYY-MM-DD.csv") %+% " — Establecimientos hoteleros.",
    strong("ap-YYYY-MM-DD.csv") %+% " — Establecimientos extrahoteleros (excluidas viviendas vacacionales)."
  )),
  p("Los ficheros se descargan con ", code("descarga_datos/importar_registro_alojamientos.py"),
    " y se guardan en ", code("importar_gobcan/historico/"), ". La fecha del fichero ",
    "se obtiene del campo ", code("metadata_modified"), " de la API CKAN del GobCan."),

  h3("Scripts del pipeline"),

  h4("P00 — Preparación de ficheros"),
  p("Script: ", code("importar_gobcan/P00-preparar_ficheros.R")),
  ul(list(
    "Determina la fecha de proceso (argumento o fichero más reciente en historico/).",
    "Corrige la errata <code>direcion_municipio_nombre</code> en los ficheros de hoteles.",
    "Fusiona <code>ap</code> + <code>ht</code> → <code>tmp/at.csv</code>.",
    "Copia <code>vv</code> → <code>tmp/vv.csv</code>.",
    "Limpia saltos de línea embebidos en los campos."
  )),

  h4("P01 — Ingesta controlada"),
  p("Script: ", code("importar_gobcan/P01-ingesta_controlada.R")),
  ul(list(
    "Lee <code>tmp/at.csv</code> y <code>tmp/vv.csv</code>.",
    "Normaliza municipio, tipología y clasificación.",
    "Hace TRUNCATE de <code>staging_import</code> e inserta todos los registros con estado <code>bruto</code>.",
    "Asigna <code>municipio_id</code> y <code>localidad_id</code> por join con tablas maestras.",
    "Filtra coordenadas fuera del bounding box de Canarias (marca <code>COORDS_FUERA_RANGO</code> en <code>audit_nota</code>)."
  )),

  h4("P02 — Geocodificación por callejero"),
  p("Script: ", code("importar_gobcan/P02-geocodificacion_filtrada_callejero.R")),
  ul(list(
    "Busca cada dirección en <code>callejero_portales</code> usando similitud fuzzy (pg_trgm).",
    "Umbral mínimo: similitud ≥ 0.45. Filtro adicional: el portal geocodificado debe estar en el mismo municipio.",
    "Resultado almacenado en <code>fuente_geocodigo = 'callejero_fuzzy:cp_portal'</code>.",
    "Procesamiento por lotes de 500 registros con pausa de 0.05 s entre lotes."
  )),

  h4("P03 — Fallback: centroide de localidad"),
  p("Script: ", code("importar_gobcan/P03-geocodificacion_por_centroide_localidad.R")),
  p("Asigna el centroide de la entidad de población (<code>centroides_localidad</code>) ",
    "a los registros que tienen <code>localidad_id</code> pero no coordenadas. ",
    "Los que no encuentran centroide pasan a estado <code>geocod_cp_pendiente</code> para que P04 los reintente."),

  h4("P04 — Fallback: centroide de código postal"),
  p("Script: ", code("importar_gobcan/P04-geocodificacion_por_centroide_cp.R")),
  p("Usa la tabla ", code("centroides_cp"), " (calculada como media de portales del callejero por CP). ",
    "Alcance: ~92% de los candidatos."),

  h4("P05 — Fallback: centroide de municipio"),
  p("Script: ", code("importar_gobcan/P05-geocodificacion_por_centroide_municipio.R")),
  p("Último recurso geométrico. Los registros sin coordenadas tras P05 quedan en estado ",
    code("sin_posicion"), " y no se migran a producción."),

  h4("P06 — Rescate de coordenadas en el mar"),
  p("Script: ", code("importar_gobcan/P06-rescate_del_mar.R")),
  ul(list(
    "Genera la columna <code>geom</code> (PostGIS Point) desde latitud/longitud para todos los registros.",
    "Detecta registros cuya geometría cae fuera de cualquier polígono municipal (en el mar).",
    "Intenta rescatarlos asignando el centroide de la localidad más cercana dentro de 1 km.",
    "Los que no se rescatan reciben el centroide del municipio de origen declarado."
  )),

  h4("P07 — Validación espacial (auditoría pasiva)"),
  p("Script: ", code("importar_gobcan/P07-validacion_espacial.R")),
  p("Clasifica cada registro en <code>audit_resultado</code>:"),
  ul(list(
    code("OK") %+% " — la geometría está en el municipio declarado por el GobCan.",
    code("DISCREPANCIA") %+% " — la geometría apunta a un municipio distinto al declarado.",
    code("SIN_GEOMETRIA") %+% " — sin coordenadas válidas.",
    code("FUERA_DE_TIERRA") %+% " — coordenada en el mar no rescatada.",
    code("SIN_MUNICIPIO_ORIGEN") %+% " — municipio de origen no identificado en la BD."
  )),

  h4("P08 — Asignación de municipio y localidad por geometría"),
  p("Script: ", code("importar_gobcan/P08-asignacion_de_municipio_localidad_por_geo.R")),
  ul(list(
    strong("Método directa: ") %+% "ST_Intersects con polígonos de localidades.",
    strong("Método proximidad: ") %+% "distancia al centroide de localidad más cercano (máx. 5 km) para los que no intersectan.",
    "Actualiza <code>municipio_id</code>, <code>localidad_id</code> y <code>muni_detectado_geo</code>."
  )),

  h4("P09 — Asignación de isla, clasificación y microdestino"),
  p("Script: ", code("importar_gobcan/P09-asignar_isla_y_clasificacion.R")),
  ul(list(
    "Mapea <code>isla_id</code> desde la tabla <code>municipios</code>.",
    "Mapea <code>modalidad_id</code>, <code>tipologia_id</code>, <code>clasificacion_id</code> mediante joins con las tablas de clasificación.",
    "Asigna <code>tipo_oferta</code> (VV/AR).",
    "Cruza con <code>destinos_turisticos</code> por ST_Intersects para determinar si el establecimiento está en área turística reglada."
  )),

  h4("P10 — Detección de duplicados"),
  p("Script: ", code("importar_gobcan/P10-comprobar_duplicados.R")),
  p("Detecta y documenta establecimientos con <code>establecimiento_id</code> repetido en ",
    code("staging_import"), ". Los duplicados ", strong("no se eliminan aquí"), ": P11 los resuelve ",
    "con DISTINCT ON al migrar. Exporta un CSV de auditoría a ", code("importar_gobcan/logs/"), "."),

  h4("P11 — Migración a producción"),
  p("Script: ", code("importar_gobcan/P11-migrar_a_alojamientos.R")),
  ul(list(
    "Usa <code>INSERT ... SELECT DISTINCT ON (establecimiento_id) ... ORDER BY id DESC</code>: conserva siempre el registro más reciente.",
    "<code>ON CONFLICT (establecimiento_id) DO UPDATE</code>: actualiza campos clave de los registros ya existentes.",
    "Gestión de altas: <code>fecha_alta = fecha_proceso</code> (fecha del fichero CSV procesado).",
    "Gestión de reactivaciones: <code>fecha_baja = NULL</code> para establecimientos que reaparecen.",
    "Gestión de bajas: marca <code>fecha_baja = fecha_proceso</code> en los establecimientos que ya no figuran en el CSV."
  )),

  h4("P12 — Informe de auditoría final"),
  p("Script: ", code("importar_gobcan/P12-informe_de_auditoria.R")),
  p("Genera un informe por pantalla con secciones: duplicados, totales por modalidad, ",
    "origen de geocodificación, precisión fuzzy, proximidad a núcleo, integridad de plazas ",
    "y balance de correcciones municipales. Exporta CSV del balance municipal a ", code("importar_gobcan/logs/"), "."),

  hr(),
  h3("Tabla: staging_import"),
  info_tabla("staging_import"),
  p("Tabla de trabajo temporal. Se hace TRUNCATE al inicio de cada ejecución de P01. ",
    "Contiene todos los registros en proceso de geocodificación y clasificación. ",
    strong("No debe considerarse una fuente de verdad"), ": su contenido es transitorio."),
  p("El campo ", code("estado"), " refleja el avance en el pipeline:"),
  ul(list(
    code("bruto") %+% " → recién ingestado, sin geocodificar.",
    code("geocod_cp_pendiente") %+% " → sin centroide de localidad, pendiente de P04.",
    code("sin_posicion") %+% " → sin coordenadas recuperables.",
    code("finalizado_geo") %+% " → geocodificación completa, listo para P09–P11."
  )),
  tabla_columnas("staging_import"),

  hr(),
  h3("Tabla: alojamientos"),
  info_tabla("alojamientos"),
  p("Tabla de producción. Contiene el registro definitivo de todos los establecimientos ",
    "turísticos de Canarias procesados por el pipeline, con trazabilidad completa de ",
    "geocodificación, clasificación y auditoría. Es la principal fuente de datos para ",
    "el cálculo de indicadores en PT01."),
  p(strong("Campos de auditoría:"),
    " <code>audit_resultado</code> (OK/DISCREPANCIA/…), <code>audit_nota</code> (texto libre con eventos del pipeline), ",
    "<code>geo_erronea_gobcan</code> (flag si las coords originales del GobCan eran incorrectas)."),
  p(strong("Ciclo de vida:"), " un establecimiento entra con <code>fecha_alta</code> y sale ",
    "(si desaparece del registro oficial) con <code>fecha_baja</code>. Los registros con baja ",
    "se conservan para mantener el histórico."),
  tabla_columnas("alojamientos")
)

escribir("02-pipeline-importacion.html", "Cap. 2 — Pipeline de importación turística", cuerpo_02)


# ==============================================================================
# PÁGINA 3 — TABLAS GEOGRÁFICAS DE REFERENCIA
# ==============================================================================
cat("Generando página 3 — Tablas geográficas...\n")

cuerpo_03 <- paste(
  h2("Cap. 3 — Tablas geográficas de referencia"),

  p("Estas tablas proporcionan la base cartográfica y los identificadores geográficos ",
    "que utilizan todos los demás componentes del sistema. La mayoría se cargaron manualmente ",
    "a partir de fuentes cartográficas oficiales (IGN, GRAFCAN, INE)."),

  h3("Jerarquía territorial"),
  pre("canarias (1)
  └── islas (7)
        └── municipios (88)
              └── localidades (2.346)"),

  hr(),
  h3("Tabla: islas"),
  info_tabla("islas"),
  p("Catálogo de las 7 islas. Incluye el campo ", code("tipo_isla"),
    " (segmentación para los benchmarks del diccionario) y el polígono ", code("geom"), "."),
  p(em("Carga: manual. Sin script automatizado.")),
  tabla_columnas("islas"),

  hr(),
  h3("Tabla: municipios"),
  info_tabla("municipios"),
  p("Los 88 municipios de Canarias. Contiene el campo ", code("tipo_municipio"),
    " (segmentación para benchmarks: turístico, mixto, residencial), ",
    "el código INE (", code("codigo_ine"), "), y el polígono ", code("geom"), "."),
  p(em("Carga: manual. Sin script automatizado.")),
  tabla_columnas("municipios"),

  hr(),
  h3("Tabla: localidades"),
  info_tabla("localidades"),
  p("Las 2.346 entidades de población (núcleos y diseminados) del INE para Canarias. ",
    "Se usa para asignar el núcleo de pertenencia de cada alojamiento (P08) y para ",
    "el nivel de análisis de localidad en PT01."),
  p(em("Carga: manual. Sin script automatizado.")),
  tabla_columnas("localidades"),

  hr(),
  h3("Tabla: canarias"),
  info_tabla("canarias"),
  p("Tabla de una sola fila con el polígono del archipiélago completo. ",
    "Se usa en PT03 para hacer el JOIN geométrico del nivel ", em("canarias"), " en la vista materializada."),
  p(em("Carga: manual.")),

  hr(),
  h3("Tabla: mapa_municipios"),
  info_tabla("mapa_municipios"),
  p("Geometrías municipales simplificadas para uso en el frontend. ",
    "Incluye los 88 municipios más una entrada adicional para el nivel insular agrupado."),
  p(em("Carga: manual.")),

  hr(),
  h3("Tabla: recintos_municipales"),
  info_tabla("recintos_municipales"),
  p("Polígonos de recintos municipales, versión más detallada que ", code("municipios"),
    ". Usada en determinadas operaciones espaciales de auditoría."),
  p(em("Carga: manual.")),

  hr(),
  h3("Tabla: centroides_localidad"),
  info_tabla("centroides_localidad"),
  p("Un centroide por localidad, calculado como media ponderada de los portales del callejero ",
    "pertenecientes a esa localidad. Se usa en P03 (fallback de geocodificación) y en P08 ",
    "(asignación de localidad por proximidad)."),
  p(em("Carga: derivada del callejero. Script de construcción no automatizado actualmente.")),
  tabla_columnas("centroides_localidad"),

  hr(),
  h3("Tabla: centroides_cp"),
  info_tabla("centroides_cp"),
  p("Un centroide por código postal, calculado como media de portales con ese CP en el callejero. ",
    "Campo adicional ", code("num_portales_usados"), " para diagnóstico. Se usa en P04."),
  p(em("Carga: derivada del callejero. Script de construcción no automatizado actualmente.")),
  tabla_columnas("centroides_cp"),

  hr(),
  h3("Tabla: centroides_municipio"),
  info_tabla("centroides_municipio"),
  p("Centroide oficial de cada municipio (ST_Centroid del polígono). Se usa en P05 y como ",
    "fallback final en P06."),
  p(em("Carga: derivada de la tabla municipios. Script de construcción no automatizado.")),
  tabla_columnas("centroides_municipio"),

  hr(),
  h3("Tabla: callejero_portales"),
  info_tabla("callejero_portales"),
  p("Callejero de Canarias a nivel de portal (número de calle), con geometría de punto, ",
    "dirección normalizada y código postal. Se usa en P02 para la geocodificación fuzzy ",
    "de establecimientos turísticos mediante similitud trigonométrica (pg_trgm)."),
  p("Campos clave para la geocodificación: ", code("dir_normalizada"),
    " (dirección estandarizada para la búsqueda fuzzy), ",
    code("municipio_id"), ", ", code("localidad_id"), ", ", code("geom"), "."),
  p(em("Fuente: cartografía oficial (GRAFCAN/Cartociudad). Carga manual.")),
  tabla_columnas("callejero_portales"),

  hr(),
  h3("Tabla: portales_canarias"),
  info_tabla("portales_canarias"),
  p("Tabla auxiliar con contenido equivalente a ", code("callejero_portales"),
    ". Mantiene compatibilidad con consultas heredadas. No se usa en el pipeline actual."),

  hr(),
  h3("Tabla: codigos_postales"),
  info_tabla("codigos_postales"),
  p("Catálogo de códigos postales de Canarias con su isla y municipio de referencia. ",
    "Tabla auxiliar para validación y diagnóstico."),
  p(em("Carga: manual.")),
  tabla_columnas("codigos_postales"),

  hr(),
  h3("Tabla: destinos_turisticos"),
  info_tabla("destinos_turisticos"),
  p("Los 23 microdestinos turísticos reglados de Canarias, delimitados por el planeamiento ",
    "urbanístico de suelo de uso turístico. Se usan en P09 para clasificar cada alojamiento ",
    "según si está en un área turística oficial (", code("en_area_turistica = TRUE"),
    ") o en suelo residencial."),
  p("El campo ", code("turistica"), " distingue entre zonas con ordenación turística activa ",
    "y las ", em("bolsas"), " de suelo sin microdestino definido (que se tratarán como ",
    "suelo residencial a efectos del análisis)."),
  p(em("Fuente: delimitación oficial de zonas turísticas del GobCan / GRAFCAN. Carga manual.")),
  tabla_columnas("destinos_turisticos")
)

escribir("03-tablas-geograficas.html", "Cap. 3 — Tablas geográficas de referencia", cuerpo_03)


# ==============================================================================
# PÁGINA 4 — DATOS ESTADÍSTICOS
# ==============================================================================
cat("Generando página 4 — Datos estadísticos...\n")

# Rangos reales de años
rango <- function(tabla, campo_year) {
  tryCatch({
    r <- DBI::dbGetQuery(con, paste0(
      "SELECT MIN(", campo_year, ") AS mn, MAX(", campo_year, ") AS mx FROM ", tabla))
    paste0(r$mn, " – ", r$mx)
  }, error = function(e) "?")
}

cuerpo_04 <- paste(
  h2("Cap. 4 — Datos estadísticos"),

  p("Tablas que recogen datos demográficos, turísticos y de vivienda procedentes de fuentes ",
    "estadísticas oficiales (ISTAC, INE). La mayoría dispone de scripts de descarga en ",
    code("descarga_datos/"), " y estrategia de carga documentada."),

  hr(),
  h3("Tabla: poblacion"),
  info_tabla("poblacion"),
  p("Población de derecho por año y ámbito (canarias, isla, municipio). ",
    "Cobertura: ", rango("poblacion", "year"), "."),
  p(strong("Fuente dual:"),
    ul(list(
      strong("ISTAC — ") %+% "dataset C00025A_000002 para canarias, islas y municipios históricos (1986–2024). Script: <code>descarga_datos/istac_poblacion.py</code> + <code>importar_poblacion.R</code>.",
      strong("INE — ") %+% "tabla 29005 (Padrón Municipal) para municipios desde 1996, con datos del año en curso disponibles antes que el ISTAC. Script: <code>descarga_datos/ine_poblacion.py</code> + <code>importar_poblacion_ine.R</code>."
    ))),
  p(strong("Estrategia de carga:"),
    " TRUNCATE + reload completo para ISTAC. UPSERT (tabla temporal + ON CONFLICT DO UPDATE) para INE: sobreescribe municipios 1996+ preservando años anteriores y los niveles canarias/isla."),
  p(strong("Caso especial Frontera (El Hierro, 38013):"),
    " el ISTAC usa dos códigos distintos según el período. Ambos se reasignan a 38013 sin solapamiento."),
  p(strong("Caso conocido — Moya duplicada (35013):"),
    " existe otra Moya en Cuenca con el mismo nombre en la API del INE. Se conserva el valor mayor por (codigo_ine, año)."),
  tabla_columnas("poblacion"),

  hr(),
  h3("Tabla: pte_reglada"),
  info_tabla("pte_reglada"),
  p("Población Turística Equivalente reglada (PTEr) calculada por el ISTAC a partir de ",
    "la Encuesta de Ocupación en Alojamientos Turísticos. Organizada por microdestinos ",
    "turísticos (zonas con suelo clasificado)."),
  p(strong("Fuente:"), " ISTAC, dataset C00065A_000042 ",
    em("Población Turística Equivalente"), ". ",
    "Script: ", code("descarga_datos/istac_poblacion_turistica.py"), " + ",
    code("descarga_datos/importar_pte_reglada.R"), "."),
  p(strong("Niveles almacenados:"),
    ul(list(
      strong("canarias:") %+% " código ES70, directo del CSV.",
      strong("isla:") %+% " códigos ES703–ES709, directo del CSV.",
      strong("localidad_turistica:") %+% " microdestinos con código de 7 caracteres, join con <code>destinos_turisticos</code>.",
      strong("municipio:") %+% " NO existe en el CSV — se calcula sumando localidades + reparto proporcional de bolsas insulo-municipales."
    ))),
  p(strong("Bolsas de suelo sin microdestino:"),
    " las plazas fuera de microdestinos se concentran en códigos B9 por isla (FV, GC, LZ, TF) ",
    "o en el código de isla completo (EH, LG, LP). Se reparten entre municipios en proporción ",
    "a sus camas turísticas fuera de microdestinos, usando la tabla ",
    code("at_canarias_no_microdestino"), "."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo (el ISTAC revisa valores retroactivamente)."),
  p(strong("Nota conocida — Descuadre ISTAC 2020:"),
    " GC (ES705) e TF (ES709) presentan un descuadre simétrico de ±15,86 en todos los períodos de 2020. ",
    "No es un error propio del sistema; se preserva sin corrección."),
  tabla_columnas("pte_reglada"),

  hr(),
  h3("Tabla: pte_vacacional"),
  info_tabla("pte_vacacional"),
  p("Población Turística Equivalente vacacional (PTEv), calculada a partir de la estadística ",
    em("Estadística de Vivienda Vacacional"), " del ISTAC."),
  p(strong("Metodología PTEv:"),
    pre("noches_vv = plazas_disponibles × (tasa_vivienda_reservada / 100) × días_mes
ptev      = noches_vv / días_mes
→ media diaria de plazas VV ocupadas en el mes")),
  p(strong("Fuente:"), " ISTAC, dataset C00065A_000061. ",
    "Script: ", code("descarga_datos/istac_pte_vv.py"), " + ",
    code("descarga_datos/importar_pte_vv.R"), ". Cobertura mensual desde 2019-M01."),
  p(strong("Ámbitos:"), " canarias (ES70), 7 islas (ES703–ES709), 88 municipios (35xxx/38xxx). ",
    "Territorios con sufijo _U se descartan. Total: 8.256 registros."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo."),
  tabla_columnas("pte_vacacional"),

  hr(),
  h3("Tabla: hogares"),
  info_tabla("hogares"),
  p("Total de hogares y tamaño medio del hogar (personas por hogar) según censos de población. ",
    "Cobertura: ediciones censales de 1842 a 2021. Ámbitos: canarias, isla y municipio."),
  p(strong("Fuente:"), " ISTAC, dataset C00025A_000001 ",
    em("Población, hogares y tamaño medio según censos. Municipios"), ". ",
    "Script: ", code("descarga_datos/istac_hogares.py"), " + ",
    code("descarga_datos/importar_hogares.R"), "."),
  p(strong("Uso en el pipeline:"), " PT01 lee el valor más reciente de ", code("miembros"),
    " para calcular ", code("viviendas_necesarias = poblacion / personas_por_hogar"),
    ". Disponible para los tres ámbitos principales (canarias, isla, municipio)."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo."),
  tabla_columnas("hogares"),

  hr(),
  h3("Tabla: viviendas_municipios"),
  info_tabla("viviendas_municipios"),
  p("Snapshot censal único (Censo 2021) del parque de viviendas de Canarias. Niveles: ",
    "canarias, isla (agregados desde municipios) y 88 municipios."),
  p(strong("Campos:"), " total, vacías, esporádicas, habituales (= total − vacías − esporádicas)."),
  p(strong("Restricción CHECK:"), " ", code("total = vacias + esporadicas + habituales"), "."),
  p(strong("Fuente:"), " INE, tabla 59531 (Censo de Población y Viviendas 2021). ",
    "Script: ", code("descarga_datos/ine_viviendas.py"), " + ",
    code("descarga_datos/importar_viviendas.R"), "."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo."),
  tabla_columnas("viviendas_municipios"),

  hr(),
  h3("Tabla: superficies"),
  info_tabla("superficies"),
  p("Superficie en hectáreas por ámbito (canarias, isla, municipio). ",
    "Se divide entre 100 en PT01 para obtener km²."),
  p(em("Carga: manual. Sin script automatizado.")),
  tabla_columnas("superficies"),

  hr(),
  h3("Tabla: vivienda_iniciada_terminada_canarias"),
  info_tabla("vivienda_iniciada_terminada_canarias"),
  p("Serie histórica de viviendas iniciadas y terminadas. Ámbitos: Canarias total (ES70), ",
    "Las Palmas (ES701) y Santa Cruz de Tenerife (ES702). Períodos anuales y mensuales desde 2002."),
  p(strong("Fuente:"), " ISTAC, dataset E25004A_000001 ",
    em("Viviendas iniciadas y terminadas en Canarias"), ". ",
    "Script: ", code("descarga_datos/istac_vivienda_construccion.py"), " + ",
    code("descarga_datos/importar_vivienda_construccion.R"), "."),
  p(strong("Medidas:"), " terminadas/iniciadas × total/libres/protegidas (6 columnas)."),
  p(strong("Nota:"), " la tabla anterior ", code("vivienda_terminada_canarias"),
    " (solo ES70, año y unidades totales) queda obsoleta."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo."),
  tabla_columnas("vivienda_iniciada_terminada_canarias"),

  hr(),
  h3("Tabla: historico_plazas_regladas"),
  info_tabla("historico_plazas_regladas"),
  p("Serie anual de plazas turísticas regladas para canarias y las 7 islas. ",
    "Cobertura: 2009 hasta el último año publicado. Incluye todas las categorías ",
    "(ALOJAMIENTO_TURISTICO_CATEGORIA = _T)."),
  p(strong("Fuente:"), " ISTAC, dataset C00065A_000033 ",
    em("Encuesta de Ocupación en Alojamientos Turísticos"), ". ",
    "Script: ", code("descarga_datos/istac_plazas.py"), " + ",
    code("descarga_datos/importar_plazas.R"), ". Total: 136 registros."),
  p(strong("Nota:"), " pronunciada caída en 2020 (395k → 190k plazas) por cierres COVID."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo."),
  tabla_columnas("historico_plazas_regladas"),

  hr(),
  h3("Tabla: turistas_llegadas"),
  info_tabla("turistas_llegadas"),
  p("Serie mensual de turistas llegados por isla desde 2010. Cobertura: Fuerteventura, ",
    "Gran Canaria, La Palma, Lanzarote y Tenerife (el ISTAC no publica El Hierro ni La Gomera)."),
  p(strong("Fuente:"), " ISTAC, dataset E16028B_000011 ",
    em("Encuesta de Gasto Turístico"), ". ",
    "Script: ", code("descarga_datos/istac_turistas.py"), " + ",
    code("descarga_datos/importar_turistas.R"), ". Total: 960 registros."),
  p(strong("Estrategia de carga:"), " TRUNCATE + reload completo."),
  tabla_columnas("turistas_llegadas")
)

escribir("04-datos-estadisticos.html", "Cap. 4 — Datos estadísticos", cuerpo_04)


# ==============================================================================
# PÁGINA 5 — CLASIFICACIÓN TURÍSTICA
# ==============================================================================
cat("Generando página 5 — Clasificación turística...\n")

# Contenidos reales de las tablas pequeñas
mod_df   <- DBI::dbGetQuery(con, "SELECT id, nombre FROM modalidades ORDER BY id")
tip_df   <- DBI::dbGetQuery(con, "SELECT t.id, m.nombre AS modalidad, t.nombre FROM tipologias t JOIN modalidades m ON t.modalidad_id=m.id ORDER BY m.id, t.id")
clas_df  <- DBI::dbGetQuery(con, "SELECT c.id, t.nombre AS tipologia, c.nombre FROM clasificaciones c JOIN tipologias t ON c.tipologia_id=t.id ORDER BY t.id, c.id")

tabla_simple <- function(df) {
  enc <- paste0("<thead><tr>", paste0("<th>", names(df), "</th>", collapse=""), "</tr></thead>")
  fils <- apply(df, 1, function(r) paste0("<tr>", paste0("<td>", r, "</td>", collapse=""), "</tr>"))
  paste0('<table class="table table-sm table-bordered table-striped">',
         enc, "<tbody>", paste(fils, collapse="\n"), "</tbody></table>")
}

cuerpo_05 <- paste(
  h2("Cap. 5 — Clasificación turística"),

  p("Tablas que definen la jerarquía de clasificación de los establecimientos turísticos ",
    "y la delimitación geográfica de las zonas turísticas regladas."),

  hr(),
  h3("Jerarquía de clasificación"),
  pre("Modalidad (2)
  └── Tipología (18)
        └── Clasificación (41)"),
  p("La jerarquía se usa en P09 para asignar los IDs correspondientes a cada establecimiento ",
    "a partir de los textos del CSV del GobCan."),

  hr(),
  h3("Tabla: modalidades"),
  info_tabla("modalidades"),
  p(em("Carga: manual. Valores estables.")),
  tabla_simple(mod_df),
  tabla_columnas("modalidades"),

  hr(),
  h3("Tabla: tipologias"),
  info_tabla("tipologias"),
  p(em("Carga: manual. Se amplía cuando el GobCan introduce nuevas tipologías en sus CSVs.")),
  tabla_simple(tip_df),
  tabla_columnas("tipologias"),

  hr(),
  h3("Tabla: clasificaciones"),
  info_tabla("clasificaciones"),
  p(em("Carga: manual. Incluye estrellas, llaves, categorías únicas, etc.")),
  tabla_simple(clas_df),
  tabla_columnas("clasificaciones"),

  hr(),
  h3("Tabla: destinos_turisticos"),
  info_tabla("destinos_turisticos"),
  p("Ya documentada en el capítulo de tablas geográficas (Cap. 3). ",
    "Se incluye aquí por su papel en la clasificación de establecimientos: ",
    "el campo ", code("turistica"), " determina si el establecimiento está en suelo turístico reglado."),

  hr(),
  h3("Tabla: at_canarias_no_microdestino"),
  info_tabla("at_canarias_no_microdestino"),
  p("Plazas turísticas regladas fuera de microdestinos, agregadas por (isla, municipio). ",
    "Se usa como pesos para el reparto de las ", em("bolsas"), " insulo-municipales en ",
    code("importar_pte_reglada.R"), "."),
  p(strong("Lógica:"), " ",
    code("ratio_municipio = plazas_municipio / total_plazas_isla"), ", y ",
    code("PTE_municipio = PTE_localidades_propias + (bolsa_isla × ratio)"), "."),
  p(em("Carga: derivada de alojamientos. Se recalcula al actualizar alojamientos. Sin script automatizado actualmente.")),
  tabla_columnas("at_canarias_no_microdestino")
)

escribir("05-clasificacion-turistica.html", "Cap. 5 — Clasificación turística", cuerpo_05)


# ==============================================================================
# PÁGINA 6 — SALIDA CALCULADA Y VISUALIZACIÓN
# ==============================================================================
cat("Generando página 6 — Salida calculada...\n")

# Muestra del diccionario
dic_df <- DBI::dbGetQuery(con, "
  SELECT id_campo, descripcion, formula, formato
  FROM diccionario_de_datos
  WHERE formula IS NOT NULL AND formula != ''
  ORDER BY orden_de_calculo NULLS LAST
  LIMIT 20")

dic_filas <- apply(dic_df, 1, function(r) {
  paste0("<tr><td><code>", r["id_campo"], "</code></td>",
         "<td>", r["descripcion"], "</td>",
         "<td><code>", r["formula"], "</code></td>",
         "<td>", r["formato"], "</td></tr>")
})

cuerpo_06 <- paste(
  h2("Cap. 6 — Salida calculada y visualización"),

  p("Tablas y vistas que contienen los indicadores finales calculados por el pipeline ",
    "de informes (PT01–PT03) y que alimentan el visor cartográfico."),

  hr(),
  h3("Tabla: base_snapshots"),
  info_tabla("base_snapshots"),
  p("Tabla de trabajo intermedia, generada por PT01. Contiene los datos brutos de entrada ",
    "(sin ratios calculados) para todos los ámbitos y la fecha de proceso en curso. ",
    "Se hace TRUNCATE al inicio de cada ejecución de PT01."),
  p(strong("No debe consultarse directamente: "), "su contenido es transitorio entre PT01 y PT02."),

  hr(),
  h3("Tabla: full_snapshots"),
  info_tabla("full_snapshots"),
  p("Tabla histórica de indicadores. Contiene una fila por (ámbito × fecha_calculo), ",
    "con todos los campos base, ratios literales y benchmarks calculados por PT02. ",
    "Es la fuente de verdad para las consultas del visor."),
  p(strong("Ámbitos:"), " canarias (1), isla (7), municipio (88), localidad (2.346). ",
    "Total por snapshot: 2.442 filas."),
  p(strong("Deduplicación:"), " PT01 elimina los registros previos para la misma fecha ",
    "antes de comenzar el procesamiento. PT02 incluye una comprobación de seguridad adicional."),
  p(strong("Campos de benchmark:"), " cada ratio principal tiene campos ",
    code("_max"), " y ", code("_avg"), " segmentados por (ambito, tipo_municipio), ",
    "calculados por PT02 mediante el motor de benchmarks del diccionario."),

  hr(),
  h3("Vista materializada: mv_full_snapshots_dashboard"),
  p("Vista materializada creada por PT03 a partir de ", code("full_snapshots"), ". ",
    "Contiene solo los campos marcados con ", code("en_mv = TRUE"), " en el diccionario, ",
    "más la geometría de la entidad (polígono de canarias/isla/municipio). ",
    "Se excluye el nivel localidad."),
  p(strong("Índices:"), " GIST sobre ", code("geom_martin"), " + índice sobre ", code("ambito"), ". ",
    "Se ejecuta CLUSTER para optimizar las lecturas en Raspberry Pi."),
  p(strong("Uso:"), " Martin la expone como vector tiles para el visor Drupal. ",
    "La MV se reconstruye completamente en cada ejecución de PT03."),

  hr(),
  h3("JSONs exportados por PT03"),
  ul(list(
    strong("datos_dashboard.json") %+% " — snapshot más reciente (canarias + isla + municipio). Fuente principal del visor.",
    strong("series.json") %+% " — histórico completo de campos con <code>comparable = TRUE</code> en el diccionario. Usado para las series temporales.",
    strong("localidades.json") %+% " — snapshot actual del nivel localidad, campos con <code>en_localidades = TRUE</code>."
  )),
  p("Destino: ", code("/home/carlos/visor/web/sites/default/files/visor/"), "."),

  hr(),
  h3("Tabla: diccionario_de_datos"),
  info_tabla("diccionario_de_datos"),
  p("Metadatos de todos los campos de ", code("full_snapshots"), ". Es leída en tiempo de ejecución ",
    "por PT02 para calcular los ratios y por PT03 para filtrar los campos a exportar. ",
    "También se mantiene como referencia documental."),
  p(strong("Campos clave:")),
  ul(list(
    code("id_campo") %+% " — nombre exacto de la columna en <code>full_snapshots</code>.",
    code("descripcion") %+% " — etiqueta legible para el visor.",
    code("formula") %+% " — expresión R evaluable por <code>eval(parse())</code>, o función de benchmark (<code>avg()</code>, <code>max()</code>).",
    code("orden_de_calculo") %+% " — orden estricto de evaluación (dependencias entre campos).",
    code("formato") %+% " — tipo de visualización: entero, decimal_2, porcentaje_2, texto, fecha, date_year.",
    code("en_mv") %+% " — ¿se incluye en la vista materializada del visor?",
    code("comparable") %+% " — ¿se incluye en el JSON de series históricas?",
    code("en_localidades") %+% " — ¿se incluye en el JSON de localidades?"
  )),

  h4("Primeros 20 campos calculados (muestra)"),
  paste0(
    '<table class="table table-sm table-bordered">',
    "<thead><tr><th>Campo</th><th>Descripción</th><th>Fórmula</th><th>Formato</th></tr></thead>",
    "<tbody>", paste(dic_filas, collapse="\n"), "</tbody>",
    "</table>"
  ),
  p(em("El diccionario completo contiene 153 entradas (campos base + ratios + benchmarks + metadatos)."))
)

escribir("06-salida-calculada.html", "Cap. 6 — Salida calculada y visualización", cuerpo_06)


# ==============================================================================
# PÁGINA 7 — SCRIPTS DE DESCARGA Y MANTENIMIENTO
# ==============================================================================
cat("Generando página 7 — Scripts de descarga...\n")

cuerpo_07 <- paste(
  h2("Cap. 7 — Scripts de descarga y mantenimiento"),

  p("Scripts Python para la descarga de datos desde las APIs públicas del GobCan, ISTAC e INE. ",
    "Se encuentran en ", code("descarga_datos/"), ". Cada script genera un fichero CSV en ",
    code("tmp/"), "que el script R de importación correspondiente carga en la base de datos."),

  h3("Registros turísticos (GobCan / CKAN)"),

  h4("importar_registro_alojamientos.py"),
  p("Descarga los tres datasets de registros turísticos del GobCan desde la API CKAN. ",
    "Usa el campo ", code("metadata_modified"), " del dataset como fecha del fichero, ",
    "lo que permite detectar si hay datos nuevos sin necesidad de comparar contenidos."),
  ul(list(
    strong("Viviendas vacacionales:") %+% " → <code>importar_gobcan/historico/vv-YYYY-MM-DD.csv</code>",
    strong("Establecimientos hoteleros:") %+% " → <code>importar_gobcan/historico/ht-YYYY-MM-DD.csv</code>",
    strong("Extrahoteleros sin VV:") %+% " → <code>importar_gobcan/historico/ap-YYYY-MM-DD.csv</code>"
  )),
  p("No sobreescribe si ya existe un fichero con la misma fecha. ",
    "Tras la descarga, ejecutar el pipeline P00–P12."),

  h3("Estadísticas ISTAC"),

  h4("istac_pte_vv.py"),
  p("PTE vacacional. Dataset C00065A_000061. → ", code("tmp/pte_vv_YYYYMMDD.csv"),
    ". Importar con ", code("importar_pte_vv.R"), "."),

  h4("istac_poblacion_turistica.py"),
  p("PTE reglada. Dataset C00065A_000042. → ", code("tmp/poblacion_turistica_equivalente_YYYYMMDD.csv"),
    ". Importar con ", code("importar_pte_reglada.R"), "."),

  h4("istac_poblacion.py"),
  p("Población de derecho (ISTAC). Dataset C00025A_000002. → ", code("tmp/poblacion_YYYYMMDD.csv"),
    ". Importar con ", code("importar_poblacion.R"), "."),

  h4("istac_hogares.py"),
  p("Hogares y tamaño medio. Dataset C00025A_000001. → ", code("tmp/hogares_YYYYMMDD.csv"),
    ". Importar con ", code("importar_hogares.R"), "."),

  h4("istac_plazas.py"),
  p("Plazas turísticas regladas históricas. Dataset C00065A_000033. → ", code("tmp/plazas_YYYYMMDD.csv"),
    ". Importar con ", code("importar_plazas.R"), "."),

  h4("istac_turistas.py"),
  p("Turistas llegados por isla. Dataset E16028B_000011. → ", code("tmp/turistas_YYYYMMDD.csv"),
    ". Importar con ", code("importar_turistas.R"), "."),

  h4("istac_vivienda_construccion.py"),
  p("Viviendas iniciadas y terminadas. Dataset E25004A_000001. → ", code("tmp/vivienda_YYYYMMDD.csv"),
    ". Importar con ", code("importar_vivienda_construccion.R"), "."),

  h3("Estadísticas INE"),

  h4("ine_poblacion.py"),
  p("Padrón municipal (INE tabla 29005). Municipios de Canarias, 1996–año actual. ",
    "→ ", code("tmp/ine_poblacion_YYYYMMDD.csv"), ". Importar con ", code("importar_poblacion_ine.R"), "."),
  p(strong("Ventaja:"), " publica el año en curso antes que el ISTAC (datos 2025 disponibles desde enero 2026)."),

  h4("ine_viviendas.py"),
  p("Censo de viviendas 2021 (INE tabla 59531). ",
    "→ ", code("tmp/ine_viviendas_YYYYMMDD.csv"), ". Importar con ", code("importar_viviendas.R"), "."),

  h3("Orden de actualización recomendado"),
  ol(list(
    "Descargar y procesar registros turísticos: <code>importar_registro_alojamientos.py</code> → P00–P12.",
    "Actualizar PTE vacacional: <code>istac_pte_vv.py</code> → <code>importar_pte_vv.R</code>.",
    "Actualizar población: <code>istac_poblacion.py</code> → <code>importar_poblacion.R</code>, luego <code>ine_poblacion.py</code> → <code>importar_poblacion_ine.R</code>.",
    "Actualizar PTE reglada (anual): <code>istac_poblacion_turistica.py</code> → <code>importar_pte_reglada.R</code>.",
    "Recalcular indicadores: <code>PT01-Capturar_datos_base.R</code> → <code>PT02-Calcular_ratios_dinamicos.R</code> → <code>PT03-Exportar_datos.R</code>."
  )),

  h3("Logs"),
  p("Todos los scripts de importación escriben en la tabla de logs de la base de datos ",
    "mediante ", code("helper.R::escribir_log()"), ". Los logs del pipeline P00–P12 también ",
    "se guardan en ficheros de texto diarios en ", code("importar_gobcan/logs/"), ".")
)

escribir("07-scripts-descarga.html", "Cap. 7 — Scripts de descarga y mantenimiento", cuerpo_07)


# ==============================================================================
# FIN
# ==============================================================================
DBI::dbDisconnect(con)
cat("\nDocumentacion generada en", OUT, "\n")
cat("Ficheros creados:\n")
for (f in list.files(OUT, pattern = "\\.html$", full.names = FALSE)) {
  cat("  ", f, "\n")
}
