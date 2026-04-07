---
name: PT02 modo histórico — solo seguro en snapshots de pipeline completo
description: Limitación crítica del modo histórico de PT02 para evitar destruir datos de snapshots manuales
type: feedback
---

El modo histórico de PT02 (`Rscript informes/PT02-Calcular_ratios_dinamicos.R YYYY-MM-DD`) recalcula TODAS las fórmulas del diccionario sobre el snapshot indicado. Si los campos base de alguna fórmula son NULL en ese snapshot, el resultado calculado pasa a ser NULL, sobreescribiendo el valor que existía.

**Why:** Al probar el modo histórico con 2024-04-30 (snapshot cargado manualmente por PT04), `uds_vv_total` quedó a NULL porque `uds_vv_turisticas` y `uds_vv_residenciales` eran NULL en ese snapshot. PT04 carga totales directamente sin el desglose turístico/residencial.

**How to apply:** Antes de ejecutar PT02 en modo histórico sobre una fecha, verificar que el snapshot fue generado por el pipeline completo (PT01+PT02), no por PT04. Los snapshots de PT04 (pre-sistema, cargados en `auxiliares/`) tienen base incompleta y el recálculo los degrada.
