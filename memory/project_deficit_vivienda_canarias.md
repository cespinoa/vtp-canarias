---
name: Análisis déficit vivienda Canarias — metodología y cifras defensibles
description: Cálculo del déficit habitacional en Canarias con dos métodos convergentes, incluyendo matices metodológicos del Censo 2021
type: project
---

## Cifras defensibles (revisadas abril 2026)

**Déficit de vivienda habitual en Canarias: ~49.000–64.000 viviendas**

Dos métodos independientes convergen en el mismo orden de magnitud:

### Método 1 — Stock vs demanda
- Viviendas habituales Censo 2021 (medición eléctrica): **793.529**
- Hogares estimados 2024 (Padrón 2.228.862 / ratio 2,60): **~857.000**
- Brecha: **~64.000** (cota superior; ratio 2,60 podría haber subido levemente → cota inferior ~52k)
- Nueva construcción 2021-2024: **10.930** → despreciable (<1% del stock), no altera los cálculos

### Método 2 — Demanda latente plurinuclear (Censo 2021)
- Hogares con 2 núcleos: **41.109** → 41.109 viviendas adicionales necesarias
- Hogares con 3+ núcleos: **3.834** → 7.668 viviendas adicionales (mínimo, asumiendo media=3)
- **Total demanda latente: ~48.800 viviendas**
- Fuente: `descarga_datos/censo2021_hogares.py` → CSV municipal con num_nucleos

## Matices metodológicos críticos

**Censo 2021 y el consumo eléctrico:**
- Las viviendas habituales se determinan por consumo >750 kWh/año — dato objetivo, sin declaraciones
- El censo usó datos de consumo de **2020** (año COVID): muchas VV tuvieron consumo bajo ese año
- Algunas VV que normalmente serían "habituales" quedaron clasificadas como esporádicas/vacías
- Por eso NO se debe restar VV de las habituales del censo 2021: el efecto COVID ya las "expulsó" de habituales de forma artificial

**VV y su clasificación:**
- En Canarias (destino turístico de alta ocupación anual), las VV tienen consumo elevado → normalmente clasifican como habituales
- En 2019 (pre-COVID): ~37.500 VV disponibles (ISTAC) → habrían estado en habituales
- En 2021 (post-COVID): ~30.400 VV → parte ya en esporádicas/vacías por COVID
- Cualquier corrección por VV introduce incertidumbre asumible → se optó por NO corregir y usar las 793k como base

**Hogar ≠ Vivienda en la práctica:**
- El censo dice hogar = vivienda, pero se miden con instrumentos distintos (Padrón vs electricidad)
- VV: cuentan como habituales (electricidad alta) pero no como hogares (turistas no empadronados)
- Infravivienda/irregularidad: personas en el Padrón en espacios no clasificados como vivienda convencional
- Hogares plurinucleares: 2 familias en 1 vivienda = 1 hogar → la demanda oculta no sube el ratio medio
- El déficit no son familias en la calle: es demanda latente comprimida en hogares más grandes

**Ratio personas/hogar:**
- 2021: **2,60** (confirmado en BD, tabla hogares ISTAC)
- 2024 estimado: podría haber subido a ~2,63–2,68 por presión habitacional
- Impacto en la estimación: reduce la demanda de 857k a 830k-846k → déficit baja a ~37k-52k
- Usar 2,60 (sin cambiar) da la cota superior más defensible

## Datos en la BD

| Tabla | Dato | Año |
|---|---|---|
| viviendas_municipios | habituales=793.529, vacías=211.452, esporádicas=83.719, total=1.088.700 | 2021 |
| hogares | 820.343 hogares, miembros_medio=2,60 | 2021 |
| poblacion | 2.172.944 (2021), 2.228.862 (2024) | anual |
| vivienda_iniciada_terminada_canarias | 10.930 terminadas 2021-2024 (ES70) | 2002-2024 |
| pte_vacacional | VV disponibles: 30.386 (2021), 45.901 (2024 media) | 2019-2024 |

**Pendiente de importar:** datos de hogares por número de núcleos del Censo 2021 (municipal)
Script listo: `descarga_datos/censo2021_hogares.py` → `tmp/censo2021_hogares_20260405.csv`

## Why / How to apply

**Why:** Análisis de presión habitacional para el proyecto VTP-Canarias, abril 2026.
**How to apply:** Usar estas cifras en ratios del diccionario de datos. La demanda latente plurinuclear (~49k) y el déficit de stock (~64k) son complementarios, no alternativos. Citar siempre como "al menos X viviendas de déficit" dado que ambas son cotas.
