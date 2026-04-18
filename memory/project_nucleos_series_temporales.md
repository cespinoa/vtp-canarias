---
name: Series temporales hogares plurinucleares INE
description: Qué APIs del INE están disponibles para hogares por núcleos familiares — qué funciona, qué no, y cómo retomar la investigación
type: project
---

Investigación realizada en abril 2026 para obtener series históricas de hogares plurinucleares (2+ núcleos familiares por hogar) en Canarias a nivel provincial y/o municipal.

## Lo que SÍ funciona

**Censo 2021 (API POST propia del INE)**
- Endpoint: `POST https://www.ine.es/Censo2021/api`
- Payload: `{"tabla":"hog","idioma":"ES","metrica":["SHOGARES"],"variables":["ID_RESIDENCIA_N3","ID_NUC_HOG"]}`
- Respuesta: clave `"data"` (minúscula), `ID_NUC_HOG` es numérico: `"0"` (sin núcleo), `"1"`, `"2"`, `"3 o más"`
- Devuelve toda España, se filtra client-side por código 35xxx/38xxx
- Script: `descarga_datos/censo2021_hogares.py` → `tmp/censo2021_hogares_YYYYMMDD.csv`
- Resultado: 85/88 municipios (3 faltan por secreto estadístico), foto fija 2021

**ECEPOV 2021 (Tempus3, tabla 56531)**
- Endpoint: `GET https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/56531`
- Territorio: "Canarias" (CCAA, no provincial), separador `, ` (coma-espacio)
- Formato nombre: `"Territorio, Tipo_hogar, Habitaciones"` → filtrar `Habitaciones=="Total"`
- 5 categorías: Hogar unipersonal / Padre-madre solo con hijos / Pareja sin hijos / Pareja con hijos / Otros tipos de hogar
- **Sin campo Anyo en Data** → snapshot single-point sin dimensión temporal; año inferido = 2021
- "Otros tipos de hogar" INCLUYE plurinucleares pero también otras formas atípicas; no se puede aislar
- Script: `descarga_datos/ine_ech_hogares.py` → `tmp/ine_ech_hogares_YYYYMMDD.csv`

## Lo que NO funciona / no existe via API

**ECH 2013–2020 (Encuesta Continua de Hogares)**
- Es la única fuente con categoría "Hogares con dos o más núcleos" explícita, a nivel provincial (35, 38)
- Vive en el JAXI antiguo del INE (operación `/t20/p274/`), NOT en Tempus3
- Confirmado: NO está en `OPERACIONES_DISPONIBLES` (solo 112 ops, ECH/ECEPOV ausentes)
- Endpoints JAXI intentados: `TABLAS_OPERACION/274` → vacío; `datos.json` → 404; `csv_sc` URL → 599
- Solo accesible descargando manualmente Excel desde:
  https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176952

**ECEPOV 2022+ (ediciones posteriores)**
- La ECEPOV es quinquenal, NO anual. Solo existe la edición 2021 (publicada feb 2023).
- La próxima edición sería 2026, sin fecha anunciada a abril 2026.
- No hay datos 2022–2025 en ninguna fuente para hogares por tipo de hogar en Canarias.
- Cuando salga la próxima edición: buscar su ID Tempus3 en la página de la operación (código INE 30280) y añadirlo en `ECEPOV_TABLAS` del script `ine_ech_hogares.py`.

## Arquitectura del sistema Tempus3 del INE

- La ECH y ECEPOV NO están en `OPERACIONES_DISPONIBLES` (112 operaciones listadas, ninguna es ECH/ECEPOV)
- `TABLAS_OPERACION/{id}` solo funciona para las 112 ops de Tempus3
- El número de path JAXI (`/t20/p274/`) NO es el ID de operación Tempus3
- La ECV (ID=155) sí está en Tempus3 pero usa categorías EU-SILC, no "núcleos"

## Why / How to apply

**Why:** Aparcado en abril 2026 para continuar con el análisis del Censo 2021.
**How to apply:** Cuando se retome la serie temporal, punto de partida: descarga manual de ECH Excel (2013-2020) + ECEPOV anual por tabla independiente (2021+). Para pivot del CSV ECH Excel a formato BD, ver patrón de `importar_poblacion_ine.R`.
