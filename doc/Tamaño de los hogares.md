# Tamaño de los hogares en Canarias (1981–2021)

## Objetivo

Analizar la evolución del tamaño medio del hogar (personas por hogar) a lo largo del tiempo,
tanto a nivel agregado (Canarias e islas) como a nivel municipal, con el fin de detectar
tendencias, convergencias y anomalías territoriales.

El tamaño medio del hogar es un dato clave en el cálculo de la **demanda de vivienda**:
a menos personas por hogar, más viviendas se necesitan para alojar a la misma población.

## Fuente de datos

- **Tabla:** `hogares`
- **Fuente original:** ISTAC, dataset C00025A_000001 "Población, hogares y tamaño medio según censos. Municipios"
- **Cobertura temporal:** ediciones censales 1981, 1991, 2001, 2011, 2021
- **Cobertura geográfica:** Canarias, 7 islas, 88 municipios
- **Campo analizado:** `miembros` (tamaño medio del hogar, personas/hogar)

## Scripts

| Script | Salidas |
|--------|---------|
| `auxiliares/evolucion_tamanio_hogar.R` | Tabla de pendientes en consola + PDF por territorio |
| `auxiliares/evolucion_tamanio_hogar_tipo.R` | Tabla de pendientes por tipo de municipio + PDF por tipo |

Los PDFs generados se guardan en `auxiliares/` junto a los scripts.

## Metodología

### Pendiente lineal

Para cada territorio se estima una **regresión lineal simple** (`miembros ~ anyo`) sobre
los cinco puntos censales disponibles (1981, 1991, 2001, 2011, 2021).
La pendiente resultante expresa la **variación media anual** en personas/hogar.

Se calcula también la **desviación** de cada territorio respecto a la pendiente de Canarias,
lo que permite identificar territorios que se alejan del patrón regional.

### Visualización

- **`evolucion_tamanio_hogar.pdf`** — Pequeños múltiplos con escala Y común:
  - Página 1: Canarias + 7 islas
  - Páginas 2–8: municipios de cada isla, con la media de la isla como línea de referencia (gris discontinuo)

- **`evolucion_tamanio_hogar_tipo.pdf`** — Pequeños múltiplos agrupados por tipo de municipio
  (GRANDE, MEDIO, TURÍSTICO, PEQUEÑO), con la media del tipo como referencia.
  Colores: azul (Grande), verde (Medio), rojo (Turístico), violeta (Pequeño).

## Tendencia general

La tendencia en Canarias, como en el conjunto de España, es de **reducción sostenida**
del tamaño medio del hogar a lo largo del período. Esta reducción refleja:

- Mayor proporción de hogares unipersonales y sin hijos
- Retraso en la emancipación
- Envejecimiento de la población
- Reducción de la fecundidad

## Interpretación para el análisis de vivienda

Una caída en el tamaño del hogar **incrementa la demanda de viviendas** aunque la
población se mantenga estable. Este efecto está integrado en el pipeline de cálculo
de snapshots a través del campo `personas_por_hogar` (procedente de `hogares`) que
alimenta los ratios `viviendas_necesarias` y `deficit` en PT01/PT02.

## Relación con otras tablas y análisis

| Elemento | Detalle |
|----------|---------|
| `hogares` | Tabla fuente; también almacena el total de hogares por territorio |
| `nucleos_censales` | Complementario: distribución de hogares por nº de núcleos familiares (Censo 2021) |
| `ech_hogares_tipo` | Complementario: hogares por tipo (unipersonal, pareja, etc.) solo a nivel Canarias |
| PT01 | Usa `personas_por_hogar` vía `get_hogares_limitado()` para calcular `viviendas_necesarias` |
| `deficit_de_vivienda.md` | Análisis narrativo completo que usa estas cifras como base |
