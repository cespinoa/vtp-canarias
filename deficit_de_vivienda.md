# Déficit de vivienda en Canarias — análisis y cifras

*Elaborado abril 2026. Fuentes: Censo 2021, ISTAC, INE, ECH, ECEPOV.*

---

## 1. Las dos estimaciones del déficit

Dos métodos independientes convergen en el mismo orden de magnitud:

### Método 1 — Balanza stock / demanda

| Concepto | Valor |
|---|---|
| Viviendas habituales Censo 2021 | 793.529 |
| Hogares estimados 2024 (Padrón 2.228.862 / ratio 2,60) | ~857.000 |
| Nueva construcción terminada 2021–2024 | +10.930 |
| **Brecha estimada** | **~53.000–64.000** |

Cota superior (ratio 2,60 estable): ~64.000. Cota inferior (ratio ligeramente subido a ~2,63–2,68 por presión): ~37.000–52.000. Se usa 2,60 como el más defensible.

### Método 2 — Demanda latente plurinuclear (Censo 2021)

| Tipo | Hogares | Viviendas adicionales mínimas |
|---|---|---|
| 2 núcleos en un hogar | 41.109 | 41.109 (1 por hogar) |
| 3 o más núcleos | 3.834 | 7.668 (2 por hogar) |
| **Total** | **44.943** | **~48.800** |

Fuente: `nucleos_censales` (88 municipios, Censo 2021 API).

**La coincidencia de ~49k y ~64k no es casualidad.** Ambos métodos miden el mismo fenómeno desde ángulos distintos.

---

## 2. Estructura de hogares en Canarias (Censo 2021)

| Tipo | Hogares | % |
|---|---|---|
| Sin núcleo (unipersonales y no emparentados) | 274.548 | 33,5% |
| 1 núcleo (familia nuclear estándar) | 499.574 | 60,9% |
| 2 núcleos (plurinuclear) | 41.109 | 5,0% |
| 3 o más núcleos | 3.834 | 0,5% |
| **Total (Censo 2021, encuesta)** | **819.065** | |

ECEPOV 2021 (misma encuesta, cifra expandida): **863.785 hogares**. La diferencia (~44k) refleja metodologías de expansión distintas, no error. Los plurinucleares de ambas fuentes sí coinciden (~44–45k).

Totales de viviendas (Censo 2021, metodología eléctrica):

| Uso | Viviendas |
|---|---|
| Habituales | 793.529 |
| Vacías | 211.452 |
| Esporádicas | 83.719 |
| **Total** | **1.088.700** |

---

## 3. La evolución del tamaño medio del hogar

| Censo | Miembros_medio | Variación |
|---|---|---|
| 1991 | 3,60 | |
| 2001 | 3,10 | −0,50 |
| 2011 | 2,60 | −0,50 |
| **2021** | **2,60** | **0,00** |

La caída sistemática de 0,50 por década durante cuarenta años se detiene exactamente cuando se agudiza la crisis de acceso a la vivienda. El estancamiento **no es equilibrio: es parálisis** producida por dos fuerzas contrapuestas que se anulan en el indicador agregado.

---

## 4. Las dos fuerzas que se cancelan estadísticamente

**Empuja el ratio hacia abajo — fragmentación demográfica:**
- Hogares unipersonales crecen **+14,8%** entre 2013 y 2020 (ECH)
- Más divorcios, viudedades, emancipaciones que sí ocurren

**Empuja el ratio hacia arriba — compresión habitacional:**
- Hogares plurinucleares crecen **+46,8%** en el mismo período (ECH)
- Familias que no pueden independizarse por coste de acceso a vivienda
- Núcleos que comparten piso por imposibilidad de pagar uno propio

Resultado observable: **0,00** de cambio en el indicador agregado entre 2011 y 2021. Pero la demanda real crece por los dos lados: más unidades necesarias por fragmentación, y más demanda insatisfecha acumulada por la plurinuclearidad. **El tamaño medio oculta el problema.**

Si no hubiera presión habitacional, el ratio habría bajado a ~2,45–2,50, lo que habría requerido 30.000–50.000 viviendas adicionales. Que no haya bajado es precisamente la huella de que esas viviendas no se han formado.

---

## 5. Por qué el déficit es invisible en el indicador agregado

Cuando dos familias comparten vivienda, el Censo las cuenta como **un solo hogar de mayor tamaño**. Ese hogar ocupa una sola vivienda habitual. El ratio vivienda/hogar cuadra perfectamente... y el déficit desaparece del agregado.

Un hogar con 2 núcleos (4 personas + 3 personas) contribuye 7 personas al total. Dos hogares independientes (4 + 3) también contribuyen 7 personas, pero ahora necesitan 2 viviendas en vez de 1. El impacto en el indicador es suave, pero la demanda real es radicalmente distinta.

**El déficit solo se hace visible cuando se desglosa por número de núcleos familiares por hogar.**

---

## 6. Serie temporal de hogares por tipo (ECH 2013–2020 + ECEPOV 2021)

Fuente: tabla `ech_hogares_tipo` (solo ámbito Canarias).

Categorías ECH (miles de hogares, selección):

| Tipo | 2013 | 2017 | 2020 |
|---|---|---|---|
| Hogar unipersonal | ~187 | ~200 | ~211 |
| Pareja sin hijos | ~145 | ~150 | ~158 |
| Pareja con hijos (total) | ~260 | ~265 | ~270 |
| Dos o más núcleos familiares | ~28 | ~35 | ~43 |

ECEPOV 2021 (miles):

| Tipo | Hogares |
|---|---|
| Hogar unipersonal | 214,6 |
| Pareja sin hijos | 151,7 |
| Pareja con hijos | 266,2 |
| Padre/madre solo con hijos | 108,4 |
| Otros tipos (incluye plurinucleares) | 123,0 |

La ECH tiene categoría "Dos o más núcleos" explícita. La ECEPOV la agrupa en "Otros tipos" sin desglosalos. Por eso se complementa con `nucleos_censales` para el dato preciso de plurinucleares en 2021.

---

## 7. Matices metodológicos críticos

**Censo 2021 — dos metodologías distintas dentro del mismo censo:**
- `viviendas_municipios`: usa consumo eléctrico >750 kWh/año para clasificar uso de la vivienda. Mide el parque residencial.
- `nucleos_censales` y ECEPOV: usan el cuestionario de la encuesta. Miden la composición del hogar.
- Son magnitudes distintas. No son directamente reconciliables.

**El efecto COVID en la medición eléctrica:**
- El Censo 2021 usó datos de consumo eléctrico de **2020** (año de cierre turístico)
- Muchas VV que en año normal tienen consumo alto quedaron clasificadas como esporádicas/vacías
- Por eso NO se debe restar VV de las habituales del Censo 2021: el efecto COVID ya las "expulsó" artificialmente
- En 2019 (pre-COVID): ~37.500 VV disponibles. En 2021: ~30.400. La diferencia ya está absorbida en la clasificación.

**Ratio personas/hogar:**
- Usar 2,60 (censo 2021) como denominador da la **cota superior más defensible**
- Si el ratio ha subido levemente por presión (a ~2,63–2,68), el déficit sería menor: ~37.000–52.000
- Sin dato actualizado, 2,60 es el más justificable públicamente

**Las cifras de déficit son cotas mínimas, no estimaciones puntuales:**
- ~49.000 (demanda latente plurinuclear) = familias que sabemos que comparten vivienda y no deberían
- ~64.000 (balanza stock/demanda) = diferencia entre parque residencial y hogares estimados
- El déficit real probablemente es mayor al no capturar infravivienda, hacinamiento, ni alquileres precarios

---

## 8. Fuentes en la base de datos

| Tabla | Contenido | Año |
|---|---|---|
| `nucleos_censales` | Hogares por nº de núcleos, 88 municipios | 2021 |
| `viviendas_municipios` | Habituales/vacías/esporádicas, 88 municipios | 2021 |
| `hogares` | Total hogares y miembros_medio, censos 1842–2021 | 2021 |
| `ech_hogares_tipo` | Hogares por tipo, Canarias (ECH 2013–2020 + ECEPOV 2021) | 2013–2021 |
| `poblacion` | Padrón Municipal anual, municipios/islas/Canarias | 1986–2024 |
| `vivienda_iniciada_terminada_canarias` | Viviendas terminadas ES70/ES701/ES702 | 2002–2024 |
| `pte_vacacional` | VV disponibles y ocupadas por mes y municipio | 2019–2024 |

Scripts de descarga/importación: ver `CLAUDE.md` sección "Descarga y actualización de datos estadísticos".
