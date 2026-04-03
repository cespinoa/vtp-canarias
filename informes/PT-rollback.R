#!/usr/bin/env Rscript
# ==============================================================================
# SCRIPT: PT-rollback.R
# Deshace la última importación y restaura el estado anterior:
#
#   1. Elimina de alojamientos todos los registros con fecha_alta = última fecha
#   2. Elimina de full_snapshots el snapshot de esa fecha
#   3. Reactiva la fecha_baja de los registros que se dieron de baja en esa importación
#   4. Regenera PT01 → PT02 → PT03 sobre el snapshot anterior
#   5. Restaura los JSONs de backup si los hay
#   6. Reinicia Martin
#
# Uso:
#   Rscript informes/PT-rollback.R              # deshace la última importación
#   Rscript informes/PT-rollback.R --dry-run    # muestra qué haría sin ejecutar
# ==============================================================================

library(DBI)
library(RPostgres)

source("importar_gobcan/helper.R")
con <- conecta_db()

dry_run <- "--dry-run" %in% commandArgs(trailingOnly = TRUE)

cat("========================================\n")
cat("PT-ROLLBACK\n")
cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
if (dry_run) cat("*** MODO DRY-RUN — no se ejecutará ningún cambio ***\n")
cat("========================================\n\n")

# --- 1. IDENTIFICAR LA ÚLTIMA FECHA DE IMPORTACIÓN ---
ultima_fecha <- dbGetQuery(con,
  "SELECT MAX(fecha_alta)::text AS f FROM alojamientos")$f

if (is.null(ultima_fecha) || is.na(ultima_fecha))
  stop("No hay registros en alojamientos.")

cat("Última fecha de importación en alojamientos:", ultima_fecha, "\n")

n_afectados <- dbGetQuery(con, paste0(
  "SELECT COUNT(*)::int AS n FROM alojamientos WHERE fecha_alta = '", ultima_fecha, "'"))$n
cat("Registros con esa fecha_alta:", n_afectados, "\n")

# Fecha anterior (la que quedará activa tras el rollback)
fecha_anterior <- dbGetQuery(con, paste0(
  "SELECT MAX(fecha_alta)::text AS f FROM alojamientos WHERE fecha_alta < '", ultima_fecha, "'"))$f

if (is.null(fecha_anterior) || is.na(fecha_anterior))
  stop("No existe fecha anterior — no hay nada a lo que volver.")

cat("Fecha anterior (snapshot objetivo):", fecha_anterior, "\n\n")

n_snapshot_actual <- dbGetQuery(con, paste0(
  "SELECT COUNT(*)::int AS n FROM full_snapshots WHERE fecha_calculo = '", ultima_fecha, " 00:00:00'"))$n
cat("Filas en full_snapshots para fecha actual:", n_snapshot_actual, "\n")

n_snapshot_anterior <- dbGetQuery(con, paste0(
  "SELECT COUNT(*)::int AS n FROM full_snapshots WHERE fecha_calculo = '", fecha_anterior, " 00:00:00'"))$n
cat("Filas en full_snapshots para fecha anterior:", n_snapshot_anterior, "\n\n")

if (dry_run) {
  cat("DRY-RUN: se eliminarían", n_afectados, "registros de alojamientos con fecha_alta =", ultima_fecha, "\n")
  cat("DRY-RUN: se eliminarían", n_snapshot_actual, "filas de full_snapshots para", ultima_fecha, "\n")
  cat("DRY-RUN: se reactivarían los registros con fecha_baja =", ultima_fecha, "\n")
  cat("DRY-RUN: se regeneraría PT01→PT02→PT03 para", fecha_anterior, "\n")
  dbDisconnect(con)
  quit(status = 0)
}

# --- 2. ELIMINAR REGISTROS DE LA ÚLTIMA IMPORTACIÓN ---
cat("Eliminando registros de alojamientos con fecha_alta =", ultima_fecha, "...\n")
n_del <- dbExecute(con, paste0(
  "DELETE FROM alojamientos WHERE fecha_alta = '", ultima_fecha, "'"))
cat("  Eliminados:", n_del, "registros.\n")

# --- 3. REACTIVAR BAJAS REGISTRADAS EN LA ÚLTIMA IMPORTACIÓN ---
n_reactivados <- dbExecute(con, paste0(
  "UPDATE alojamientos SET fecha_baja = NULL WHERE fecha_baja = '", ultima_fecha, "'"))
cat("  Reactivados (fecha_baja anulada):", n_reactivados, "registros.\n")

# --- 4. ELIMINAR SNAPSHOT DE LA ÚLTIMA FECHA ---
cat("Eliminando snapshot de full_snapshots para", ultima_fecha, "...\n")
n_snap <- dbExecute(con, paste0(
  "DELETE FROM full_snapshots WHERE fecha_calculo = '", ultima_fecha, " 00:00:00'"))
cat("  Eliminadas:", n_snap, "filas.\n\n")

escribir_log("ROLLBACK", paste(
  "Revertida importación", ultima_fecha,
  "| alojamientos eliminados:", n_del,
  "| reactivados:", n_reactivados,
  "| snapshot eliminado:", n_snap))

dbDisconnect(con)

# --- 5. REGENERAR PIPELINE PT PARA LA FECHA ANTERIOR ---
cat("Regenerando PT01→PT02→PT03 para", fecha_anterior, "...\n\n")

ret1 <- system(paste("Rscript informes/PT01-Capturar_datos_base.R", fecha_anterior))
if (ret1 != 0) stop("PT01 falló. Revisa los logs.")

ret2 <- system("Rscript informes/PT02-Calcular_ratios_dinamicos.R")
if (ret2 != 0) stop("PT02 falló. Revisa los logs.")

ret3 <- system(paste("Rscript informes/PT03-Exportar_datos.R", fecha_anterior))
if (ret3 != 0) stop("PT03 falló. Revisa los logs.")

# --- 6. REINICIAR MARTIN ---
cat("\nReiniciando Martin...\n")
system("docker restart martin-canarias-production")

cat("\n✓ Rollback completado. Estado restaurado a", fecha_anterior, "\n")
