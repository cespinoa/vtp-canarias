---
name: Clasificación tipo_municipio — criterios y casos límite
description: Decisiones tomadas sobre la clasificación tipo_municipio y su relación con destinos_turisticos
type: project
---

La columna `tipo_municipio` en la tabla `municipios` es una etiqueta analítica para
agrupar municipios comparables entre sí (radar charts, benchmarks). NO es un indicador
de presencia turística — ese control lo hace el pipeline de geocodificación (P02-P09)
cruzando cada establecimiento con los polígonos de `destinos_turisticos`.

## Casos límite revisados (2026-04-12)

**Verificación realizada en dos pasos:**
1. JOIN por `municipio_id`: municipios con/sin entrada en `destinos_turisticos`
2. `ST_Intersects` espacial: municipios cuyos polígonos se solapan con zonas turísticas

### Municipios marcados TURÍSTICO sin entrada en destinos_turisticos

| Municipio | Código | Decisión |
|-----------|--------|----------|
| San Miguel de Abona | 38035 | **Mantener TURÍSTICO**. El ISTAC lo agrupa con Granadilla de Abona en la zona "Abona"; el polígono cubre la zona costera de ambos municipios. |

### Municipios con destinos_turisticos pero NO marcados TURÍSTICO

| Municipio | tipo_municipio | Decisión | Razón |
|-----------|---------------|----------|-------|
| Las Palmas de GC | GRANDE | **Mantener GRANDE** | Ciudad residencial; el turismo (Las Canteras) no define su perfil |
| Granadilla de Abona | MEDIO | **Mantener MEDIO** | Comparte zona ISTAC "Abona" con San Miguel; perfil radar no turístico; 4.473 plazas pero municipio de 58k hab |
| Guía de Isora | MEDIO | **Mantener MEDIO** | 281 plazas; el grueso de Costa Adeje está asignado a San Miguel |
| Los Realejos | MEDIO | **Mantener MEDIO** | Solapamiento ligero con Puerto de la Cruz; 760 plazas AT, 10 VV |
| La Orotava | MEDIO | **Mantener MEDIO** | Cero plazas; el polígono de Puerto de la Cruz simplemente roza su límite |

**Why:** La clasificación sirve para comparar municipios similares en benchmarks y radares.
Mover un municipio a TURÍSTICO cuando su perfil no lo es distorsionaría el grupo.

**How to apply:** Ante dudas futuras sobre tipo_municipio, cruzar con `plazas_at_turisticas`
y `plazas_vv_turisticas` en full_snapshots y verificar el perfil radar. El dato de plazas
por zona turística es fiable porque viene del cruce espacial en el pipeline, no de la etiqueta.
