---
name: PTEv y PTEt — metodología y trabajo pendiente
description: Investigación sobre cómo calcular la Población Turística Equivalente total (reglada + vacacional) y las limitaciones del PTEv actual
type: project
---

Trabajo en curso (iniciado 2026-04-18). Contexto: análisis del informe "Presión Turística sobre la población y el territorio en Canarias" (Turismo de Islas Canarias, mayo 2025).

## Hallazgos clave

**PTEv actual en el sistema** (`pte_v` en base_snapshots):
- Fórmula: `plazas_disponibles × (tasa_vivienda_reservada / 100)`
- Mide camas ocupadas en VV, NO personas reales
- La `tasa_vivienda_reservada` es % de viviendas reservadas, pero se aplica sobre plazas (camas)
- Sobreestima la presión real de personas en un factor desconocido (antes estimado ~40%, pero ver corrección abajo)

**`estancia_media` en pte_vacacional**:
- Es `ESTANCIA_MEDIA_VIVIENDA_VACACIONAL` del ISTAC (C00065A_000061)
- Mide la duración media de cada RESERVA individual (~4-5 días)
- NO es la estancia del turista en Canarias — son conceptos distintos

**Metodología del informe (Turismo de Islas Canarias)**:
- PTEt = `turistas_FRONTUR × estancia_media_EGT / 365`
- Resultado 2024 Canarias: 456.122 (+60% vs ISTAC tradicional)
- EGT estancia media 2024: 9,37 días (vs 7,09 días en alojamiento tradicional)
- Validado: nuestra réplica da 472.843 (diferencia por usar EGT 18,4M vs FRONTUR 17,8M)

**Disponibilidad de datos en API ISTAC**:
- FRONTUR por isla: solo 5 islas (sin El Hierro ni La Gomera) — E16028B_000016
- Estancia media EGT por isla: SÍ disponible en la API — C00028A_000003 (TURISTAS) + C00028A_000004 (NOCHES_PERNOCTADAS)
- Tabla `egt_estancia_media`: cargada automáticamente vía `istac_egt_estancia.py` + `importar_egt_estancia.R`. 96 registros (2010–2025, Canarias + 5 islas). El año 2025 ya está disponible.

## Método alternativo validado — cálculo por diferencia (nivel Canarias)

```
turistas_reglados = plazas × (tasa_ocupacion/100) × 365 / estancia_media_reglada
turistas_vacacionales = turistas_llegadas - turistas_reglados
estancia_vv_real = (turistas_total × estancia_EGT - turistas_reglados × estancia_reglada) / turistas_vac
PTEt = turistas_llegadas × estancia_EGT / 365
```

### Resultados a nivel Canarias (fiables desde ~2015)

| Año  | T.Total    | %VV  | Est.VV_real | PTEt    |
|------|-----------|------|-------------|---------|
| 2015 | 14.011.490 | 15,1% | 16,02 días | 366.602 |
| 2017 | 16.713.969 | 18,8% | 13,79 días | 419.910 |
| 2019 | 15.594.520 | 20,6% | 14,25 días | 390.077 |
| 2022 | 14.997.838 | 21,6% | 16,30 días | 379.671 |
| 2023 | 16.706.543 | 23,0% | 16,40 días | 433.455 |
| 2024 | 18.419.195 | 26,9% | 14,71 días | 472.843 |
| 2025 | 19.018.233 | 28,7% | — (sin EGT) | — |

**Por qué fiables desde 2015:** antes, el % VV era <10% y cualquier redondeo dispara el cálculo.
**COVID (2020-2021):** est. VV_real sube a 19-22 días (lógico: quedaron solo turistas de larga estancia).

### Hallazgo clave: estancia del turista vacacional
- El turista VV se queda ~14-16 días en Canarias (2× más que el reglado)
- Aunque VV = 27% de turistas, representa ~42% de la presión turística total (PTEt)
- Este dato NO existe en ninguna fuente oficial

## Validación pipeline FRONTUR (2026-04-18)

- Tabla `frontur_turistas` creada y cargada: 1.152 registros, canarias + 5 islas, 2010-M01 a 2026-M02
- Scripts: `descarga_datos/frontur_canarias.py` + `descarga_datos/importar_frontur.R`
- Paranoia check: PTEt calculado = valor del informe en los **15 años** (diferencia = 0 en todos)

## Desviación PTEv sistema vs PTEv vacacional real (nivel Canarias)

PTEv_real = PTEt_total − PTEr_reglado (ambos validados contra fuentes oficiales)

| Año  | PTEv real | PTEv sistema | Desviación |
|------|-----------|--------------|------------|
| 2019 | 109.687   | 147.035      | +34%       |
| 2020 | 48.722    | 114.236      | +135% (COVID) |
| 2021 | 65.192    | 116.970      | +79% (COVID) |
| 2022 | 132.504   | 153.642      | +16%       |
| 2023 | 158.431   | 163.241      | +3%        |
| 2024 | 181.735   | 181.478      | ≈0%        |

**Lectura:** el mercado VV ha madurado y en 2024 ambos métodos coinciden. La sobreestimación histórica se debe a que hogares grandes se reservaban con poca gente (camas >> personas). Ya no es así.

## Debate abierto: cobertura municipal del PTEv

Opciones discutidas:
1. **Un solo método** (actual, camas): consistente en todos los niveles; en 2024 el error es mínimo.
2. **Corrector proporcional**: ratio `PTEv_real_isla / PTEv_sistema_isla` aplicado a municipios. Municipios conservan distribución relativa; totales de isla y Canarias son exactos; suma municipal cuadra con isla.

Pendiente: comparar cómo se hace el PTEr reglado a nivel municipal (mismo debate).

## Limitaciones del enfoque

- Solo funciona a nivel **Canarias total** con fiabilidad (a nivel isla, EGT y FRONTUR difieren por isla)
- Datos anuales (no mensuales) por la naturaleza de los datos reglados
- Sin EGT 2025 todavía → PTEt 2025 no calculable hasta que ISTAC publique

## Tablas relevantes del sistema
- `turistas_llegadas`: EGT total, 5 islas, mensual 2010–presente
- `historico_plazas_regladas` / `historico_tasa_ocupacion_reglada` / `historico_estancia_media_reglada`: anual, canarias+7 islas
- `pte_vacacional`: mensual, canarias+7 islas+municipios, 2019–
- `egt_estancia_media`: tabla estática cargada del informe TIC, Canarias+5 islas, 2010–2024
