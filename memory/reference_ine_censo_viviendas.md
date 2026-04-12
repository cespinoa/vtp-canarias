---
name: INE — Censo de Viviendas histórico (2001, 2011, 2021)
description: Qué datos hay sobre viviendas vacías/esporádicas por censo, cómo accederlos y sus limitaciones metodológicas
type: reference
---

## Datos disponibles por edición censal

### Censo 2021
- **Tabla API Tempus3**: 59531
- **Desglose**: habituales + vacías + esporádicas (3 categorías)
- **Metodología**: consumo eléctrico (>750 kWh/año = habitual)
- **Niveles**: Nacional, CCAA, provincias, todos los municipios
- **Ya en BD**: tabla `viviendas_municipios` (snapshot único 2021)
- **Cifras Canarias**: 793.529 hab. + 211.452 vacías + 83.719 esporádicas = 1.088.700 total

### Censo 2011
- **No disponible en API Tempus3** (endpoints SERIES_OPERACION/TABLAS_OPERACION devuelven null)
- **Acceso via PC-Axis** (descarga directa, sin autenticación):
  - Nacional + CCAA + provincias: `https://www.ine.es/jaxi/files/_px/es/px/t20/e244/viviendas/p07/nal02.px`
  - Municipios > 2.000 hab: `https://www.ine.es/jaxi/files/_px/es/px/t20/e244/viviendas/p07/02mun00.px`
- **Desglose**: Solo **principales** vs **no principales** (sin separar vacías de esporádicas/secundarias)
- **Años incluidos**: 2011 y 2001 (fichero comparativo)
- **Cifras Canarias**: ~117.617 deshabitadas en 2001, ~138.252 en 2011

### Censo 2001
- Incluido en el mismo fichero comparativo que el 2011 (ver URLs arriba)

## Limitaciones clave

1. **Ruptura metodológica 2021**: el cambio a consumo eléctrico no es comparable con 2001/2011
2. **Desglose diferente**: 2001/2011 solo "no principales" (vacías + secundarias juntas); 2021 las separa
3. **Municipios pequeños**: el fichero municipal 2011 solo cubre > 2.000 hab (Canarias: todos los relevantes sí incluidos)
4. **Solo dos puntos temporales**: 2001 y 2011, separados 10 años

## Evolución viviendas no habituales en Canarias

| Año | Deshabitadas/Vacías | Fuente |
|-----|---------------------|--------|
| 2001 | ~117.617 | Censo INE (no principales, parcial) |
| 2011 | ~138.252 | Censo INE (no principales, parcial) |
| 2021 | 211.452 (solo vacías) + 83.719 (esporádicas) = 295.171 total no hab. | Censo INE tabla 59531 |

El salto 2011→2021 es muy superior al 2001→2011, y coincide con el boom de VV y compra para inversión.
