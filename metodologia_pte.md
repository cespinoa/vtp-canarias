# Metodología del cálculo de la Población Turística Equivalente (PTE)

## 1. Concepto

La **Población Turística Equivalente (PTE)** expresa cuántos turistas se encuentran de media en Canarias cada día del año. Su utilidad es hacer comparables la presión turística y la población residente, que conviven en el mismo territorio y comparten los mismos recursos e infraestructuras.

La fórmula general es:

```
PTE = Noches_turísticas_anuales / 365
    = Turistas_anuales × Estancia_media / 365
```

El sistema distingue dos componentes:

| Componente | Denominación | Fuente base |
|---|---|---|
| Alojamiento reglado (hoteles, apartamentos, campings, albergues) | **PTEr** | ISTAC C00065A_000042 |
| Vivienda vacacional y resto de alojamientos | **PTEv** | FRONTUR + EGT (ver §3) |
| **Total** | **PTEt** | Suma de las dos anteriores |

---

## 2. PTEr — Población Turística Equivalente reglada

### 2.1 Fuente y fórmula

El ISTAC publica la PTEr directamente en el dataset **C00065A_000042** ("Estadística de Población Turística Equivalente"), calculada a partir de las pernoctaciones de la Encuesta de Ocupación en Alojamientos Turísticos:

```
PTEr = Pernoctaciones_anuales / 365
     = Plazas × (Tasa_ocupación_por_plaza / 100)
```

La segunda expresión es equivalente a la primera y es la que aparece en la tabla `historico_plazas_regladas` × `historico_tasa_ocupacion_reglada` del sistema.

### 2.2 Cobertura directa del ISTAC

El ISTAC publica PTEr para:

- **Canarias total** (ES70): dato directo.
- **7 islas** (ES703–ES709): dato directo.
- **Microdestinos turísticos** (zonas con suelo clasificado turístico): dato directo por zona.

El ISTAC **no publica** PTEr a nivel de municipio. Para obtenerlo, el sistema realiza un reparto interno descrito en §2.3.

### 2.3 Distribución a municipios

La distribución municipal combina dos fuentes:

**A. Municipios con microdestino turístico propio**
Reciben directamente la PTEr de las localidades turísticas (zonas ISTAC) que contienen. La asignación es exacta.

**B. Municipios sin microdestino (la «bolsa»)**
Cada isla acumula una "bolsa" con la PTEr que no corresponde a ningún microdestino:
- Islas con microdestinos (TF, GC, LZ, FV): código `ES7xxB9`.
- Islas sin microdestinos (EH, LG, LP): el código de isla completo actúa como bolsa.

La bolsa se reparte entre los municipios de la isla **en proporción a sus plazas turísticas fuera de microdestinos**, almacenadas en la tabla `at_canarias_no_microdestino`:

```
PTEr_municipio = PTEr_localidades_propias
               + PTEr_bolsa_isla × (plazas_municipio / plazas_totales_isla_no_microdestino)
```

**Garantía de consistencia:** la suma de PTEr municipal cuadra exactamente con la PTEr de isla (verificado en el script `importar_pte_reglada.R`).

### 2.4 Tabla de resultados 2024 (islas)

| Isla | Plazas regladas | Tasa ocup. | PTEr 2024 |
|---|---:|---:|---:|
| Tenerife | — | — | 98.759 |
| Gran Canaria | — | — | 76.490 |
| Lanzarote | — | — | 50.544 |
| Fuerteventura | — | — | 43.807 |
| La Gomera | — | — | 2.082 |
| La Palma | — | — | 2.717 |
| El Hierro | — | — | 270 |
| **Canarias** | — | — | **274.387** |

*(Fuente: `pte_reglada`, año 2024. Las plazas y tasa exactas se obtienen de `historico_plazas_regladas` y `historico_tasa_ocupacion_reglada`.)*

---

## 3. PTEv — Población Turística Equivalente vacacional

### 3.1 El método original del ISTAC y sus limitaciones

El ISTAC publica, dentro del dataset **C00065A_000061** ("Estadística de Vivienda Vacacional"), el campo `plazas_disponibles` y la `tasa_vivienda_reservada` (% de viviendas reservadas). El sistema empleaba inicialmente:

```
PTEv_sistema = plazas_disponibles × (tasa_vivienda_reservada / 100)
```

Este cálculo tiene una **limitación conceptual**: la `tasa_vivienda_reservada` es el porcentaje de *viviendas* reservadas, pero se aplica sobre *plazas* (camas). Una vivienda de 6 plazas reservada por dos personas genera 6 cama-noches pero solo 2 personas reales. El resultado mide **camas ocupadas**, no personas.

### 3.2 El método correcto: FRONTUR × EGT

El informe *"Presión Turística sobre la población y el territorio en Canarias"* (Turismo de Islas Canarias, mayo 2025) propone una metodología más precisa:

```
PTEt = Turistas_FRONTUR × Estancia_media_EGT / 365
```

Donde:
- **FRONTUR**: total de turistas que entran en Canarias, independientemente del alojamiento elegido. Fuente: ISTAC E16028B_000016.
- **Estancia media EGT**: duración total de la estancia del turista en Canarias (no la duración de cada reserva). Fuente: ISTAC C00028A (Encuesta de Gasto Turístico). Calculada como NOCHES_PERNOCTADAS / TURISTAS (C00028A_000004 / C00028A_000003). Descargada automáticamente con `istac_egt_estancia.py`; disponible por isla desde 2010, incluido el año en curso.

La diferencia respecto al ISTAC tradicional: este último solo considera turistas en alojamiento reglado, con una estancia media de ~7 días. La metodología FRONTUR+EGT incluye a todos los turistas, cuya estancia media real es de ~9,4 días.

### 3.3 Validación del método FRONTUR+EGT

La réplica exacta de los valores publicados en el informe TIC confirma la validez del método:

| Año | Turistas FRONTUR | Estancia EGT (días) | PTEt calculado | PTEt informe | Diferencia |
|---:|---:|---:|---:|---:|---:|
| 2010 | 10.432.046 | 9,41 | 268.947 | 268.947 | 0 |
| 2011 | 12.000.324 | 9,50 | 312.337 | 312.337 | 0 |
| 2012 | 11.767.792 | 9,62 | 310.154 | 310.154 | 0 |
| 2013 | 12.187.823 | 9,56 | 319.221 | 319.221 | 0 |
| 2014 | 12.924.434 | 9,57 | 338.868 | 338.868 | 0 |
| 2015 | 13.301.251 | 9,55 | 348.019 | 348.019 | 0 |
| 2016 | 14.981.113 | 9,36 | 384.173 | 384.173 | 0 |
| 2017 | 15.975.507 | 9,17 | 401.357 | 401.357 | 0 |
| 2018 | 15.560.965 | 9,34 | 398.190 | 398.190 | 0 |
| 2019 | 15.115.709 | 9,13 | 378.100 | 378.100 | 0 |
| 2020 | 4.631.804 | 10,14 | 128.675 | 128.675 | 0 |
| 2021 | 6.697.165 | 9,54 | 175.044 | 175.044 | 0 |
| 2022 | 14.617.383 | 9,24 | 370.040 | 370.040 | 0 |
| 2023 | 16.210.911 | 9,47 | 420.595 | 420.595 | 0 |
| 2024 | 17.767.833 | 9,37 | 456.122 | 456.122 | 0 |

Diferencia = 0 en los 15 años. Reproducción exacta.

### 3.4 PTEv por diferencia

Dado que PTEt = PTEr + PTEv, el componente vacacional se obtiene directamente:

```
PTEv_real = PTEt − PTEr
          = (Turistas_FRONTUR × Estancia_EGT / 365) − (Plazas_regladas × Tasa_ocupación / 100)
```

Este es el valor que el sistema almacena en `base_snapshots.pte_v` para Canarias y las cinco islas con datos FRONTUR.

### 3.5 Evolución del PTEv real y comparación con el método original

| Año | PTEv real | PTEv sistema (camas) | Factor corrección | Desviación |
|---:|---:|---:|---:|---:|
| 2019 | 109.686 | 147.035 | 0,746 | −25,4% |
| 2020* | 48.723 | 114.236 | 0,427 | −57,3% |
| 2021* | 65.191 | 116.970 | 0,557 | −44,3% |
| 2022 | 132.504 | 153.642 | 0,862 | −13,8% |
| 2023 | 158.431 | 163.241 | 0,971 | −2,9% |
| 2024 | 181.735 | 181.478 | 1,001 | +0,1% |

*Años COVID: interpretación limitada por la composición atípica del turismo.*

**Lectura:** el método original (camas) sobreestimaba sistemáticamente la presión vacacional. La brecha se ha cerrado progresivamente conforme el mercado de VV ha madurado y la ocupación por plaza se ha aproximado a la ocupación por persona. En 2024 ambos métodos coinciden con un error de ±0,1%.

---

## 4. Cobertura geográfica y disponibilidad de datos

La disponibilidad de FRONTUR y estancia EGT determina qué ámbitos pueden usar el método preciso:

| Ámbito | FRONTUR | EGT estancia | Método PTEv |
|---|:---:|:---:|---|
| Canarias total | ✓ | ✓ | FRONTUR × EGT / 365 − PTEr |
| Tenerife | ✓ | ✓ | FRONTUR × EGT / 365 − PTEr |
| Gran Canaria | ✓ | ✓ | FRONTUR × EGT / 365 − PTEr |
| Lanzarote | ✓ | ✓ | FRONTUR × EGT / 365 − PTEr |
| Fuerteventura | ✓ | ✓ | FRONTUR × EGT / 365 − PTEr |
| La Palma | ✓ | ✓ | FRONTUR × EGT / 365 − PTEr |
| La Gomera | ✗ | ✗ | pte_vacacional ISTAC (sin corrección) |
| El Hierro | ✗ | ✗ | pte_vacacional ISTAC (sin corrección) |
| Municipios | ✗ | ✗ | Corrector proporcional (ver §5) |

El ISTAC no incluye El Hierro ni La Gomera en los datasets FRONTUR (E16028B_000016) ni EGT por isla. Su peso conjunto en el total es inferior al 3% del PTEv de Canarias.

---

## 5. Distribución del PTEv a municipios — corrector proporcional

### 5.1 El problema

El ISTAC publica datos de VV a nivel municipal en el dataset C00065A_000061, lo que permite calcular un PTEv municipal con el método original (camas). Pero como ese método sobreestima o subestima en función de la madurez del mercado VV de cada isla, usar esos valores directamente generaría inconsistencias con los totales insulares validados.

### 5.2 El mecanismo

Se aplica un **corrector proporcional** por isla:

```
Factor_isla = PTEv_real_isla / PTEv_sistema_isla

PTEv_municipio_corregido = PTEv_municipio_sistema × Factor_isla
```

Donde `PTEv_sistema_isla` es la media anual del PTEv según el ISTAC para esa isla (promedio de 12 meses de `pte_vacacional`).

El resultado garantiza que:
1. **La suma de municipios cuadra con el total de isla** (validado en cada ejecución de PT01).
2. La **distribución relativa** entre municipios de la misma isla conserva las proporciones del ISTAC, que reflejan la actividad VV real de cada territorio.
3. Los municipios con más actividad VV reciben proporcionalmente más PTEv, igual que ocurre con el PTEr.

### 5.3 Factores de corrección 2024 por isla

| Isla | PTEv real | PTEv sistema | Factor | Interpretación |
|---|---:|---:|---:|---|
| Tenerife | 79.107 | 71.849 | 1,101 | Sistema subestimaba un 10% |
| Gran Canaria | 55.226 | 37.005 | 1,492 | Sistema subestimaba un 49% |
| Lanzarote | 27.347 | 31.931 | 0,856 | Sistema sobreestimaba un 17% |
| Fuerteventura | 18.965 | 25.080 | 0,756 | Sistema sobreestimaba un 32% |
| La Palma | 1.693 | 6.719 | 0,252 | Sistema daba valor imposible (>PTEt total) |
| La Gomera | — | 2.627 | 1,000 | Sin datos FRONTUR, sin corrección |
| El Hierro | — | 1.718 | 1,000 | Sin datos FRONTUR, sin corrección |

El caso de La Palma ilustra por qué la corrección es necesaria: el PTEv según el método original (6.719) supera al PTEt total de la isla (4.410), lo que es matemáticamente imposible. La corrección lleva el PTEv al valor consistente (1.693).

### 5.4 Verificación de consistencia suma municipal = isla

| Isla | PTEv isla | Suma municipios | Diferencia |
|---|---:|---:|---:|
| Tenerife | 79.107 | 79.086 | −21 |
| Gran Canaria | 55.226 | 55.198 | −28 |
| Lanzarote | 27.347 | 27.341 | −6 |
| Fuerteventura | 18.965 | 18.968 | +3 |
| La Gomera | 2.627 | 2.629 | +2 |
| El Hierro | 1.718 | 1.716 | −2 |
| La Palma | 1.693 | 1.695 | +2 |

Las diferencias (máximo ±28 sobre valores de decenas de miles) son residuo de redondeos en el promedio mensual. Son irrelevantes en la práctica.

---

## 6. PTEt total resultante (snapshot 2026-03-31)

| Ámbito | PTEr | PTEv | PTEt |
|---|---:|---:|---:|
| **Canarias** | **274.387** | **181.735** | **456.122** |
| Tenerife | 98.759 | 79.107 | 177.866 |
| Gran Canaria | 76.490 | 55.226 | 131.716 |
| Lanzarote | 50.544 | 27.347 | 77.891 |
| Fuerteventura | 43.807 | 18.965 | 62.772 |
| La Gomera | 2.082 | 2.627 | 4.709 |
| La Palma | 2.717 | 1.693 | 4.410 |
| El Hierro | 270 | 1.718 | 1.988 |

El total Canarias (456.122) reproduce exactamente el valor publicado por Turismo de Islas Canarias para 2024.

---

## 7. Fuentes de datos y tablas del sistema

| Dato | Dataset ISTAC | Tabla en BD | Cobertura |
|---|---|---|---|
| PTEr por microdestino, isla, Canarias | C00065A_000042 | `pte_reglada` | Anual, 2009– |
| PTEv municipal y de isla (método original) | C00065A_000061 | `pte_vacacional` | Mensual, 2019– |
| Turistas FRONTUR por isla y Canarias | E16028B_000016 | `frontur_turistas` | Mensual, 2010– |
| Estancia media EGT por isla y Canarias | C00028A_000003/000004 | `egt_estancia_media` | Anual, 2010– |
| Plazas regladas anuales | C00065A_000033 | `historico_plazas_regladas` | Anual, 2009– |
| Tasa de ocupación por plaza | C00065A_000033 | `historico_tasa_ocupacion_reglada` | Anual, 2009– |
| Plazas fuera de microdestinos | Registro GobCan | `at_canarias_no_microdestino` | Estático (última importación) |

### Limitaciones conocidas

- **El Hierro y La Gomera** no tienen cobertura en FRONTUR ni en EGT por isla; sus PTEv se calculan con el método original sin corrección.
- La estancia EGT de **2025 es provisional**: el ISTAC la publica con actualizaciones retroactivas, por lo que el valor actual puede variar.
- El cálculo es **anual**. No existe una corrección mensual del PTEv: el factor se aplica al promedio de 12 meses de `pte_vacacional`.

---

## 8. Implementación en el pipeline

El cálculo se ejecuta en **PT01-Capturar_datos_base.R**:

1. Se calculan los factores de corrección por isla consultando `frontur_turistas`, `egt_estancia_media`, `historico_plazas_regladas`, `historico_tasa_ocupacion_reglada` y `pte_vacacional`.
2. Para **Canarias** y las **5 islas con FRONTUR**, `pte_v` en `base_snapshots` se reemplaza con el valor PTEv_real (PTEt − PTEr).
3. Para **municipios**, el `pte_v` obtenido de `pte_vacacional` se multiplica por el factor de su isla.
4. Para **La Gomera y El Hierro**, factor = 1,0 (sin modificación).
5. **PT02** aplica las fórmulas del diccionario de datos sobre los valores ya corregidos, sin ninguna modificación adicional.
