# -------------------------------------------------------------------------
# SCRIPT: helper.R (VERSIÓN 2.0 - NORMALIZACIÓN UNIFICADA)
# -------------------------------------------------------------------------
library(RPostgres)
library(dotenv)
load_dot_env(".env")

# --- FUNCIÓN MAESTRA DE NORMALIZACIÓN ---
# Esta función es el "Estándar de Oro". Se usará para TODO.
normalizar_geo <- function(texto) {
  if (is.na(texto) || texto == "" || texto == "_U") return(NA)

  # ELIMINA (D) Y (N) - diseminado y núcleo, en los nombres de localidades
  texto <- gsub("\\(N\\)", "", texto, ignore.case = TRUE)
  texto <- gsub("\\(D\\)", "", texto, ignore.case = TRUE)
  texto <- gsub("\\s+", " ", texto) # Limpiar espacios dobles resultantes
  
  # 1. Minúsculas y quitar tildes/eñes
  res <- tolower(texto)
  res <- chartr("áéíóúüñ", "aeiouun", res)
  
  # 2. Limpieza de puntuación y ruido común
  # Sustituimos puntuación por espacios para no pegar palabras (ej: "c/real" -> "c real")
  res <- gsub("[[:punct:]]", " ", res)
  
  # 3. Eliminar artículos y preposiciones SUELTAS
  # Usamos fronteras de palabra (\\b) para no romper nombres como "Adeje"
  patrones <- c("el", "la", "los", "las", "de", "del", "y", "en")
  for (p in patrones) {
    res <- gsub(paste0("\\b", p, "\\b"), " ", res)
  }
  
  # 4. Limpieza de tipos de vía (Solo para direcciones, pero no estorba en municipios)
  vias <- c("\\bcalle\\b", "\\bc\\b", "\\bcl\\b", "\\bavenida\\b", "\\bavda\\b", 
            "\\bpaseo\\b", "\\bcarretera\\b", "\\burb\\b", "\\burbanizacion\\b")
  for (v in vias) {
    res <- gsub(v, " ", res)
  }
  
  # 5. Colapsar espacios y limpieza final
  res <- gsub("\\s+", " ", res)
  res <- trimws(res)
  
  return(if(res == "") NA else res)
}

# Wrapper para Islas (mantiene el formato slug si es necesario)
normalizar_isla <- function(texto) {
  res <- normalizar_geo(texto)
  if (is.na(res)) return(NA)
  return(gsub(" ", "_", res))
}

# --- GESTIÓN DE BASE DE DATOS Y LOGS ---
conecta_db <- function() {
  dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("DB_HOST"), port = as.integer(Sys.getenv("DB_PORT")),
    dbname = Sys.getenv("DB_NAME"), user = Sys.getenv("DB_USER"), password = Sys.getenv("DB_PASS")
  )
}

escribir_log <- function(accion, resultado) {
  log_path <- "importar_gobcan/logs"
  if (!dir.exists(log_path)) dir.create(log_path, recursive = TRUE)
  linea <- paste(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "|", accion, "|", resultado)
  write(linea, file = file.path(log_path, paste0("importacion-", Sys.Date(), ".log")), append = TRUE)
}
