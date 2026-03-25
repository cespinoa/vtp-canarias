library(DBI)
library(dplyr)
library(knitr)
source("importar_gobcan/helper.R")

con <- conecta_db()

cat("\n--- INICIANDO EXAMEN DE DUPLICADOS EN STAGING ---\n")

# 1. Extraemos todos los registros que comparten establecimiento_id
query_dups <- "
WITH ids_repetidos AS (
    SELECT establecimiento_id 
    FROM staging_import 
    WHERE estado = 'finalizado_geo'
    GROUP BY establecimiento_id HAVING COUNT(*) > 1
)
SELECT 
    id, establecimiento_id, nombre_comercial, muni_nombre, 
    direccion, plazas, fuente_geocodigo, audit_resultado, ultimo_procesado
FROM staging_import
WHERE establecimiento_id IN (SELECT establecimiento_id FROM ids_repetidos)
ORDER BY establecimiento_id, id DESC"

dups_df <- dbGetQuery(con, query_dups)

if (nrow(dups_df) > 0) {
    # Guardar para examen externo
    write.csv(dups_df, "auditoria_duplicados_staging.csv", row.names = FALSE, fileEncoding = "UTF-8")
    
    cat(paste0("✅ Archivo 'auditoria_duplicados_staging.csv' generado con ", nrow(dups_df), " filas.\n"))
    cat(paste0("📊 Se han detectado ", length(unique(dups_df$establecimiento_id)), " establecimientos con conflictos.\n\n"))
    
    # 2. Análisis rápido de variabilidad
    cat("--- RESUMEN DE DISCREPANCIAS ENCONTRADAS ---\n")
    resumen_variacion <- dups_df %>%
      group_by(establecimiento_id) %>%
      summarise(
        mismo_nombre = n_distinct(nombre_comercial) == 1,
        mismas_plazas = n_distinct(plazas) == 1,
        misma_direccion = n_distinct(direccion) == 1,
        .groups = 'drop'
      )
    
    cat(paste0("- Grupos con nombres distintos: ", sum(!resumen_variacion$mismo_nombre), "\n"))
    cat(paste0("- Grupos con plazas distintas: ", sum(!resumen_variacion$mismas_plazas), "\n"))
    cat(paste0("- Grupos con direcciones distintas: ", sum(!resumen_variacion$misma_direccion), "\n"))
    
    cat("\n--- MUESTRA DE LOS PRIMEROS CASOS ---\n")
    print(kable(head(dups_df, 15), format = "markdown"))
    
} else {
    cat("✅ No se han encontrado registros duplicados para procesar.\n")
}

dbDisconnect(con)
