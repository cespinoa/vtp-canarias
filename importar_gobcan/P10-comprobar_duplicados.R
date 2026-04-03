#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: P10-comprobar_duplicados.R
# Detecta registros con el mismo establecimiento_id en staging_import y
# genera un informe de auditoría. Los duplicados no se eliminan aquí —
# P11 usa DISTINCT ON para resolverlos al migrar a alojamientos.
#
# Uso:
#   Rscript importar_gobcan/P10-comprobar_duplicados.R
# ==============================================================================

library(DBI)
library(dplyr)
source("importar_gobcan/helper.R")
con <- conecta_db()

cat("========================================\n")
cat("P10 — Comprobación de duplicados\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("========================================\n\n")

escribir_log("P10_INICIO", "Comprobación de duplicados iniciada")

dups_df <- dbGetQuery(con, "
  WITH ids_repetidos AS (
    SELECT establecimiento_id
    FROM staging_import
    WHERE estado = 'finalizado_geo'
    GROUP BY establecimiento_id
    HAVING COUNT(*) > 1
  )
  SELECT id, establecimiento_id, nombre_comercial, muni_nombre,
         direccion, plazas, fuente_geocodigo, audit_resultado, ultimo_procesado
  FROM staging_import
  WHERE establecimiento_id IN (SELECT establecimiento_id FROM ids_repetidos)
  ORDER BY establecimiento_id, id DESC")

if (nrow(dups_df) > 0) {
  n_establecimientos <- length(unique(dups_df$establecimiento_id))

  cat("Duplicados detectados:", nrow(dups_df), "filas en",
      n_establecimientos, "establecimientos.\n\n")

  # Análisis de variabilidad
  resumen <- dups_df %>%
    group_by(establecimiento_id) %>%
    summarise(
      mismo_nombre    = n_distinct(nombre_comercial) == 1,
      mismas_plazas   = n_distinct(plazas) == 1,
      misma_direccion = n_distinct(direccion) == 1,
      .groups = "drop"
    )

  cat("Grupos con nombres distintos   :", sum(!resumen$mismo_nombre), "\n")
  cat("Grupos con plazas distintas    :", sum(!resumen$mismas_plazas), "\n")
  cat("Grupos con direcciones distintas:", sum(!resumen$misma_direccion), "\n\n")

  cat("Muestra (primeros 10):\n")
  print(head(dups_df[, c("establecimiento_id", "nombre_comercial",
                          "muni_nombre", "plazas", "fuente_geocodigo")], 10))

  # Guardar CSV de auditoría
  fecha_slug <- format(Sys.Date(), "%Y-%m-%d")
  csv_path   <- file.path("importar_gobcan/logs",
                           paste0("duplicados_staging_", fecha_slug, ".csv"))
  write.csv(dups_df, csv_path, row.names = FALSE, fileEncoding = "UTF-8")
  cat("\nAuditoria guardada en:", csv_path, "\n")

  escribir_log("P10_RESULTADO", paste(
    n_establecimientos, "establecimientos con duplicados.",
    "CSV:", csv_path))
} else {
  cat("Sin duplicados — todos los establecimiento_id son únicos.\n")
  escribir_log("P10_RESULTADO", "Sin duplicados detectados")
}

dbDisconnect(con)
cat("\n✓ P10 completado.\n")
