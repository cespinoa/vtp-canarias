Nombre del Proyecto: VTP-Canarias
Objetivo: Integrar información procedente de diferentes registros oficiales relacionadas con la vivienda, el turismo y la población en Canarias para obtener datos base y ratios de interés. La visualización se delega en un módulo personalizado de Drupal.

# Stack
- R instalado directamente en el sistema
- PostGIS y Martin ejecutándose en contenedores Docker (arm64, Raspberry Pi)
- Base de datos: PostgreSQL con extensión PostGIS
- Visualización: módulo Drupal personalizado (visor en /home/carlos/visor/)

# Estructura de carpetas

- importar_gobcan/   Pipeline de importación de datos desde el Gobierno de Canarias
- informes/          Pipeline de cálculo de snapshots y exportación
- descarga_datos/    Scripts de descarga e importación de estadísticas periódicas (ISTAC, INE)
- documentacion/     Script generar_documentacion.R + htmls/ generados para el Book de Drupal
- Los ficheros estructura_de_la_base_de_datos.csv y diccionario_de_datos.csv documentan el esquema y los campos calculados. El diccionario_de_datos también existe como tabla en la base de datos y es leído en tiempo de ejecución por PT02.

# Pipeline de importación (importar_gobcan/)

Secuencia de 13 scripts (P00–P12) que transforman CSVs del GobCan en datos de producción:

  - helper.R           Utilidades compartidas: conexión DB, logging, normalización de texto
  - P00               Prepara ficheros: fusiona ap+ht → at, corrige cabeceras, limpia saltos de línea.
                       Acepta fecha como parámetro; sin él usa el conjunto más reciente en historico/.
                       Deja vv.csv y at.csv listos en importar_gobcan/tmp/ para P01.
                       Escribe tmp/fecha_proceso.txt para que P01 sepa qué fecha está procesando.
  - P01               Lee tmp/fecha_proceso.txt al arrancar (para si P00 no fue el último si no existe).
                       Ingesta controlada desde vv.csv y at.csv → staging_import
  - P02               Geocodificación por callejero (fuzzy, similitud >0.45, filtro municipal)
  - P03               Fallback por centroide de localidad
  - P04               Fallback por centroide de código postal
  - P05               Fallback por centroide de municipio
  - P06               Rescate de coordenadas en el mar (distancia <1km → localidad más cercana)
  - P07               Auditoría espacial pasiva (sin modificar datos): OK/MAR/DISCREPANCIA
  - P08               Asignación de localidad por ST_Intersects o proximidad
  - P09               Asignación de isla_id, modalidad_id, tipologia_id, clasificacion_id y microdestino
  - P10               Detección y documentación de duplicados
  - P11               Migración de staging_import a alojamientos (DISTINCT ON + ON CONFLICT)
  - P12               Informe de auditoría final

Los logs se escriben por fecha en importar_gobcan/logs/. Los CSV originales procesados se guardan en importar_gobcan/historico/.

# Pipeline de informes (informes/)

Pipeline activo compuesto por tres scripts PT ejecutados en secuencia:

  PT01-Capturar_datos_base.R
    - Fecha: MAX(fecha_alta) de alojamientos por defecto; acepta parámetro para histórico
    - Elimina registros previos de full_snapshots para esa fecha (deduplicación anticipada)
    - Hace TRUNCATE de base_snapshots y captura datos brutos para los cuatro ámbitos:
      canarias, isla, municipio, localidad
    - Incluye: oferta VV/AR, población, PTE, viviendas, superficie, hogares
    - pte_v usa la media de los últimos 12 meses <= fecha del snapshot (no el dato puntual)
      → elimina la estacionalidad para análisis estructural
    - pte_v_periodo almacena el rango "YYYY-MM/YYYY-MM" de los meses promediados
    - Añade tipo_municipio, tipo_isla y etiqueta_ambito_superior
    - No calcula ratios (solo datos base)

  PT02-Calcular_ratios_dinamicos.R
    - Lee base_snapshots y el diccionario_de_datos (tabla DB) para obtener fórmulas y orden de cálculo
    - Ejecuta fórmulas literales en orden (orden_de_calculo)
    - Calcula benchmarks (avg/max) segmentados por (ambito, tipo_municipio)
    - En ambos tipos de benchmark, si la fórmula contiene "| Excluyendo valores 100",
      filtra x[x < 100] antes de calcular (tanto para avg como para max)
    - Tipado guiado por el campo formato del diccionario (entero vs numérico)
    - Borra registros previos de la misma fecha antes de insertar (deduplicación)
    - Escribe a full_snapshots

  PT03-Exportar_datos.R
    - Reconstruye la vista materializada mv_full_snapshots_dashboard
      (UNION ALL de canarias/isla/municipio con geometría, campos filtrados por en_mv=TRUE)
    - Crea índices GIST + CLUSTER para rendimiento en Raspberry Pi
    - Guarda copia de seguridad de los JSONs anteriores en .../visor/backup/
    - Exporta 3 JSONs a /home/carlos/visor/web/sites/default/files/visor/:
        datos_dashboard.json  → snapshot actual (canarias/isla/municipio)
        series.json           → histórico completo (campos con comparable=TRUE)
        localidades.json      → datos de localidades del snapshot actual
    - Reinicia el contenedor Martin al finalizar para que aplique la nueva MV

Scripts de validación y calidad (no forman parte del pipeline ordinario):

  calidad_datos.R     Informe de calidad: geocodificación, plazas, traspasos municipales
  paranoia_test.R     Verificación independiente: recalcula manualmente con SQL directo
                      para Canarias, Lanzarote (isla), Arrecife, Tías (municipios) y
                      El Hierro (isla sin microdestinos turísticos) y compara contra
                      full_snapshots. Cubre ~70 campos por ámbito incluyendo benchmarks.
  PT-rollback.R       Deshace la última importación: elimina alojamientos con la última
                      fecha_alta, reactiva las bajas de esa importación, borra el snapshot
                      de full_snapshots y regenera PT01→PT02→PT03 sobre la fecha anterior.
                      Acepta --dry-run para ver el efecto sin ejecutar cambios.

# Scripts descartados / archivados

  S01-totalizar_alojamientos.R     Prototipo inicial. Escribía en tabla snapshots (obsoleta),
                                   ratios hardcodeados, sin diccionario. No usar.
  T01, T02                         Versiones intermedias (fórmulas en formulas.csv externo,
                                   pipeline monolítico, sin deduplicación). No usar.
  PT04-importar_historico_islas.R  Movido a carpeta auxiliar. Script de uso único para
                                   cargar datos históricos pre-sistema desde columnas de
                                   la tabla islas. Solo ejecutar si se vacía full_snapshots.

# Descarga y actualización de datos estadísticos (descarga_datos/)

Dos bloques de datos con cadencias distintas:

## Bloque 1 — Registros turísticos (actualización frecuente)
Fuente: Gobierno de Canarias (datos.canarias.es, catálogo CKAN)
  - Viviendas vacacionales (VV)
  - Establecimientos hoteleros
  - Establecimientos extrahoteleros sin VV

Tienen pipeline complejo de importación (P01–P12 en importar_gobcan/).
Scripts de descarga: turismo_download.py (producción), turismo_ckan.py (exploración).

## Bloque 2 — Estadísticas periódicas (actualización ocasional)
Fuente: ISTAC y otros organismos. Pipeline de importación sencillo: descarga + reparto + carga.
Cada dato tiene su propio script en descarga_datos/.

### PTE Vacacional (pte_vacacional)
Fuente: ISTAC, dataset C00065A_000061 "Estadística de Vivienda Vacacional".
Script de descarga: istac_pte_vv.py → tmp/pte_vv_YYYYMMDD.csv
Script de importación: importar_pte_vv.R (en descarga_datos/)

Cobertura: mensual desde 2019-M01. Niveles: canarias, 7 islas, 88 municipios.
Medidas originales: plazas_disponibles, viviendas_disponibles, viviendas_reservadas,
  tasa_vivienda_reservada, estancia_media, ingresos_totales.

Metodología PTEv (Turismo de Islas Canarias, ref. "Presión turística sobre la
población y el territorio"):
  noches_vv = plazas_disponibles × (tasa_vivienda_reservada / 100) × días_mes
  ptev      = noches_vv / días_mes
  → ptev es la media diaria de plazas VV ocupadas en el mes.

Clasificación de territorios por código: ES70=canarias, ES703–ES709=isla,
  35xxx/38xxx=municipio, _U=descartado.
Estrategia de carga: TRUNCATE + reload completo (el ISTAC revisa datos retroactivos).
Registros: 86 canarias + 602 isla + 7.568 municipio = 8.256 total.

Scripts anteriores (raíz del workspace): importar_pte_vv.R y check_importacion_vv.R
  — sustituidos por los nuevos scripts en descarga_datos/. No usar: dependían de
  paquetes istacr/rjstat, URL con versión fija, setwd() y append sin deduplicación.

### PTE Reglada (pte_reglada)
Fuente: ISTAC, dataset C00065A_000042 "Población Turística Equivalente".
Script de descarga: istac_poblacion_turistica.py → tmp/poblacion_turistica_equivalente_YYYYMMDD.csv
Script de importación: importar_pte_reglada.R

El ISTAC organiza la PTE por microdestinos (zonas turísticas con suelo clasificado).
Las plazas fuera de esas zonas acumulan PTE en una "bolsa" por isla que debe repartirse
entre municipios en proporción a sus camas turísticas fuera de microdestinos.

Niveles almacenados en pte_reglada:
  - canarias: código ES70, directo del CSV
  - isla: códigos ES703–ES709, directo del CSV
  - localidad_turistica: códigos ES7XXBy (7 chars, dígito final ≠ 9), join con destinos_turisticos
  - municipio: NO existe en el CSV — se calcula sumando localidades + reparto de bolsas

Tipos de bolsa:
  - Islas CON microdestinos (FV, GC, LZ, TF): código B9 por isla (ej. ES705B9)
  - Islas SIN microdestinos (EH=ES703, LG=ES706, LP=ES707): el código de isla completo es la bolsa

Reparto de bolsas: tabla at_canarias_no_microdestino
  Contiene plazas turísticas fuera de microdestinos por (isla_id, municipio_id).
  ratio_municipio = plazas_municipio / total_plazas_isla
  PTE_municipio = PTE_localidades_propias + (bolsa_isla × ratio)

Estrategia de carga: TRUNCATE + reload completo en cada actualización.
Justificación: el ISTAC revisa valores retroactivamente en cada publicación (confirmado:
los años 2021–2024 tienen diferencias respecto a la carga anterior).

NOTA CONOCIDA — Descuadre ISTAC 2020:
  El ISTAC publicó en 2020 valores de isla/bolsa que no cuadran internamente:
  GC (ES705): isla=23.983,70 pero suma(hijos)=23.999,55 → exceso +15,86
  TF (ES709): isla=27.988,48 pero suma(hijos)=27.972,63 → defecto −15,86
  El ±15,86 aparece simétricamente entre GC y TF en todas las versiones del dataset.
  No es un error nuestro. Se preserva tal cual en la BD y no afecta a ningún otro año.

### Población de Derecho (poblacion)
Fuente dual: ISTAC (niveles canarias/isla y municipios pre-1996) + INE (municipios 1996+).
Un solo origen de verdad por año/ámbito; el campo fuente registra la procedencia.

Estado actual de la tabla:
  - canarias + isla: ISTAC C00025A_000002, años 1986–2024
  - municipio 1986–1995: ISTAC C00025A_000002 (~783 registros)
  - municipio 1996–2025: INE t=29005 (2.541 registros, incluyendo año 2025)

#### Bloque ISTAC (canarias, isla y municipios históricos)
Script de descarga: istac_poblacion.py → tmp/poblacion_YYYYMMDD.csv
Script de importación: importar_poblacion.R
Dataset: C00025A_000002 "Población de derecho (municipios de Canarias)"
Dimensiones API: TERRITORIO × MEDIDAS × TIME_PERIOD. Se extrae MEDIDAS=POBLACION.
Cubre 1986–2024 (37 años; sin datos para 1991 y 1997).
Estrategia de carga: TRUNCATE + reload completo (ON CONFLICT falla con NULLs en clave única).

Caso especial Frontera (El Hierro, codigo_ine=38013):
  El ISTAC usa 38013_1912 (años 1986–2007) y 38013_2007 (años 2008–2024).
  Ambos se reasignan a 38013. No hay solapamiento.

#### Bloque INE (municipios 1996+)
Script de descarga: ine_poblacion.py → tmp/ine_poblacion_YYYYMMDD.csv
Script de importación: importar_poblacion_ine.R
Fuente: INE tabla 29005, Padrón Municipal (https://www.ine.es/jaxiT3/Tabla.htm?t=29005)
Ventaja: publica el año en curso antes que el ISTAC (datos 2025 disponibles desde enero 2026).
Cubre 1996–año actual (sin 1997); solo nivel municipio.

Estrategia de descarga (2 llamadas):
  1. VALORES_VARIABLE/19 → catálogo {nombre_ine: codigo_ine} para Canarias
     (88 municipios: códigos INE que empiezan por 35 o 38, excluyendo 2 entradas
     especiales "Población en municipios desaparecidos de...")
  2. DATOS_TABLA/29005 → 24.414 series (toda España, ~5 MB); filtrar ". Total."
     y cruzar por nombre con el catálogo.

Caso conocido — Moya duplicada (35013):
  Existe otra "Moya" en Cuenca (16135, ~148 hab.). Ambas series tienen el mismo
  nombre en DATOS_TABLA. Se resuelve conservando el valor mayor por (codigo_ine, anyo).

Estrategia de carga: UPSERT (ON CONFLICT DO UPDATE) vía tabla temporal.
Sobreescribe los registros ISTAC de municipios para 1996+. Los años 1986–1995 y
los niveles canarias/isla (solo ISTAC) permanecen intactos.

Secuencia de importación:
  1. Rscript descarga_datos/importar_poblacion.R      # ISTAC: todos los niveles
  2. Rscript descarga_datos/importar_poblacion_ine.R  # INE: municipios 1996+

### Hogares y Tamaño Medio (hogares)
Fuente: ISTAC, dataset C00025A_000001 "Población, hogares y tamaño medio según censos. Municipios".
Script de descarga: istac_hogares.py → tmp/hogares_YYYYMMDD.csv
Script de importación: importar_hogares.R

Cubre 22 ediciones censales (1842–2021) para Canarias, islas y los 88 municipios.
La tabla hogares tiene columnas: ambito, isla_id, municipio_id, hogares, miembros, year.
La columna municipio_id fue añadida al incorporar este script (ALTER TABLE).

Campos extraídos: HOGARES (total hogares) y HOGARES_TAMANIO_MEDIO (tamaño medio).
Se descartan los 13 territorios con sufijos _E##/_N## (entidades históricas sin dato moderno).
Registros cargados: 19 canarias + 133 isla + 1.625 municipio = 1.777 total.
Estrategia de carga: TRUNCATE + reload completo.

El campo year se almacena como DATE (2021-12-31 para todos los registros actuales).
PT01 usa get_hogares_limitado() que consulta hogares por ambito/isla_id/municipio_id;
devuelve personas_por_hogar para canarias, isla Y municipio (no para localidad = NA).
Esto permite calcular viviendas_necesarias y deficit a nivel municipal.

### Viviendas por Municipio (viviendas_municipios)
Fuente: INE, tabla 59531 (Censo de Población y Viviendas 2021).
Script de descarga: ine_viviendas.py → tmp/ine_viviendas_YYYYMMDD.csv
Script de importación: importar_viviendas.R

Snapshot único (sin dimensión temporal). Niveles: canarias, 7 islas, 88 municipios.
Los niveles isla y canarias se calculan por agregación desde los municipios.
Campos: total, vacias, esporadicas, habituales (= total - vacias - esporadicas).
La tabla tiene CHECK: total = vacias + esporadicas + habituales.
Registros: 1 canarias + 7 isla + 88 municipio = 96 total.
Totales 2021: 1.088.700 viviendas (793.529 habituales, 211.452 vacías, 83.719 esporádicas).
Estrategia de carga: TRUNCATE + reload completo.

### Viviendas Iniciadas y Terminadas (vivienda_iniciada_terminada_canarias)
Fuente: ISTAC, dataset E25004A_000001 "Viviendas iniciadas y terminadas en Canarias".
Script de descarga: istac_vivienda_construccion.py → tmp/vivienda_YYYYMMDD.csv
Script de importación: importar_vivienda_construccion.R

Territorios almacenados: ES70 (Canarias total), ES701 (Las Palmas), ES702 (SCT).
Períodos: anuales (YYYY) y mensuales (YYYY-Mxx). ES70 desde 2002; ES701/ES702 desde 2008.
Medidas (6 columnas): terminadas/iniciadas × total/libres/protegidas.

Dimensiones API (orden en observaciones): TIME_PERIOD × MEDIDAS × TERRITORIO.
Registros cargados: 780 (ES70: 312, ES701: 234, ES702: 234).
Estrategia de carga: TRUNCATE + reload completo (el ISTAC revisa valores retroactivos).

Nota: la tabla anterior vivienda_terminada_canarias (solo año + unidades totales ES70)
queda obsoleta y puede eliminarse una vez que PT01 se actualice para leer la nueva tabla.

#### Fuente complementaria no integrada — ISTAC E20001A (licencias municipales)
El ISTAC publica una estadística de construcción basada en licencias municipales de obra mayor
(fuente diferente: ayuntamientos, no Colegios de Arquitectos) con detalle insular y municipal:
  E20001A_000007 → viviendas por tipo de obra × 7 islas + ES70 (1994–2019)
  E20001A_000021 → viviendas por tipo de obra × 88 municipios (2012–2019)
Accesibles via API ISTAC JSON. Corte duro en 2019; no se actualiza desde entonces.
Mide permisos concedidos (proxy de inicio), no terminaciones.
Posible uso: análisis histórico 1994–2019 con desglose insular/municipal si se necesita.

### Histórico de Plazas Regladas (historico_plazas_regladas) y Tasa de Ocupación (historico_tasa_ocupacion_reglada)
Fuente: ISTAC, dataset C00065A_000033 "Encuesta de Ocupación en Alojamientos Turísticos".
Script de descarga: istac_plazas.py → tmp/plazas_YYYYMMDD.csv
Script de importación: importar_plazas.R

El CSV incluye dos medidas del dataset, ambas con ALOJAMIENTO_TURISTICO_CATEGORIA=_T (todas las categorías):
  - PLAZAS               → tabla historico_plazas_regladas (campo plazas, INTEGER)
  - TASA_OCUPACION_PLAZA → tabla historico_tasa_ocupacion_reglada (campo tasa, NUMERIC 5,2)

Cobertura de ambas tablas: anual 2009–año más reciente publicado. Ámbitos: canarias + 7 islas.
Dimensiones API (orden): TIME_PERIOD × TERRITORIO × MEDIDAS × ALOJAMIENTO_TURISTICO_CATEGORIA.
Registros: 17 canarias + 119 isla = 136 total (en cada tabla).
Estrategia de carga: TRUNCATE + reload completo en ambas tablas en el mismo script.
Nota: caída pronunciada en 2020 (plazas 395k→190k; tasa 68%→42%) por cierre COVID.

### Estancia Media Reglada (historico_estancia_media_reglada)
Fuente: ISTAC, dataset C00065A_000039 "Encuesta de Ocupación en Alojamientos Turísticos" (serie anual con desglose por nacionalidad).
Script de descarga: istac_estancia_reglada.py → tmp/estancia_reglada_YYYYMMDD.csv
Script de importación: importar_estancia_reglada.R

El dataset no publica un agregado `_T` de nacionalidades. La estancia media total se calcula como:
  ESTANCIA_MEDIA = PERNOCTACIONES_suma / VIAJEROS_ENTRADOS_suma
sumando las 27 nacionalidades disponibles (pernoctaciones y viajeros son aditivos).

Cobertura: anual 2009–año más reciente publicado. Ámbitos: canarias + 7 islas.
Dimensiones API: MEDIDAS × TERRITORIO × TIME_PERIOD × NACIONALIDAD (orden de iteración real).
Registros: 17 canarias + 119 isla = 136 total.
Estrategia de carga: TRUNCATE + reload completo.
Tendencia: ~8.7 días en 2009 → ~7.2 en 2025. Caída 2020–2021 por COVID.

Nota: C00065A_000033 (fuente de plazas y tasa) no incluye ESTANCIA_MEDIA. C00065A_000039
es el dataset anual específico para pernoctaciones y viajeros (mismo origen, diferente dataset).

### Estancia Media VV (historico_estancia_media_vv)
Fuente: tabla pte_vacacional (ISTAC C00065A_000061), campo estancia_media.
No requiere descarga externa: el script lee directamente de la BD.
Script de importación: importar_estancia_media_vv.R

Cobertura: anual 2019–año en curso. Ámbitos: canarias + 7 islas.
Metodología: media ponderada por viviendas_reservadas.
  estancia_anual = Σ(estancia_media_mes × viviendas_reservadas_mes) / Σ(viviendas_reservadas_mes)
  → Los meses con más actividad turística pesan más que los meses con poca reserva.
  → Los meses con viviendas_reservadas = 0 o NULL se excluyen.
Registros: 8 canarias + 56 isla = 64 total (incluye año en curso con datos parciales).
Estrategia de carga: TRUNCATE + reload completo. Ejecutar tras cada actualización de pte_vacacional.
Nota: el año en curso aparece en la tabla pero con datos incompletos (solo meses disponibles a la fecha).

### Turistas Llegados por Isla (turistas_llegadas)
Fuente: ISTAC, dataset E16028B_000011 "Encuesta de Gasto Turístico".
Script de descarga: istac_turistas.py → tmp/turistas_YYYYMMDD.csv
Script de importación: importar_turistas.R

Cobertura: mensual 2010-M01 hasta el mes más reciente publicado.
Islas: ES704 (Fuerteventura), ES705 (Gran Canaria), ES707 (La Palma),
  ES708 (Lanzarote), ES709 (Tenerife). El dataset ISTAC no incluye
  El Hierro (ES703) ni La Gomera (ES706).
Dato almacenado: TIPO_VIAJERO=TURISTA (total), LUGAR_RESIDENCIA=_T (todos los mercados).
Registros: 960 (192 meses × 5 islas).
Estrategia de carga: TRUNCATE + reload completo (el ISTAC revisa datos retroactivos).

Nota sobre tipos de viajero (no almacenados):
  TURISTA_PRINCIPAL = isla es el destino principal del viaje
  TURISTA_SECUNDARIO = isla visitada como parte de un viaje multidestinoNo existe dato de Canarias total ni de islas menores en este dataset.

Scripts de exploración (no usar en producción):
  istac_explore.py     Explora metadatos y endpoints de la API ISTAC

### Turistas FRONTUR por Isla (frontur_turistas)
Fuente: ISTAC, dataset E16028B_000016 "FRONTUR — Movimientos Turísticos en Fronteras".
Script de descarga: frontur_canarias.py → tmp/frontur_YYYYMMDD.csv
Script de importación: importar_frontur.R

Cobertura: mensual 2010-M01 hasta el mes más reciente publicado.
Territorios: ES70 (Canarias), ES704 (Fuerteventura), ES705 (Gran Canaria),
  ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife).
Complementa turistas_llegadas (EGT) con el enfoque FRONTUR.
Usado para calcular PTEt: FRONTUR × estancia_EGT / 365.
Registros: 1.152 (192 meses × 6 territorios).
Estrategia de carga: TRUNCATE + reload completo (el ISTAC revisa datos retroactivos).

### Estancia Media EGT por Isla (egt_estancia_media)
Fuente: ISTAC, datasets C00028A_000003 (TURISTAS) y C00028A_000004 (NOCHES_PERNOCTADAS).
Script de descarga: istac_egt_estancia.py → tmp/egt_estancia_YYYYMMDD.csv
Script de importación: importar_egt_estancia.R

La estancia media se calcula como NOCHES_PERNOCTADAS / TURISTAS, garantizando
consistencia metodológica. Usada para PTEt según metodología TIC.
Cobertura: anual 2010–año más reciente publicado.
Territorios: ES70 (Canarias), ES704 (Fuerteventura), ES705 (Gran Canaria),
  ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife).
Registros: 96 (16 años × 6 territorios en última carga).
Estrategia de carga: TRUNCATE + reload completo.
Nota: los datos de los últimos años pueden diferir ligeramente del informe TIC
  (el ISTAC revisa retroactivamente).

# Tablas principales de la base de datos

  Datos primarios:
    alojamientos        Tabla de producción con rastro de auditoría completo
    staging_import      Tabla de trabajo durante el pipeline de importación
    at_canarias         Alojamientos turísticos reglados

  Tablas maestras geográficas:
    municipios, localidades, islas
    centroides_localidad, centroides_cp, centroides_municipio
    callejero_portales

  Datos estadísticos de entrada:
    poblacion, pte_reglada, pte_vacacional, hogares
    viviendas_municipios, superficies
    vivienda_iniciada_terminada_canarias   ES70/ES701/ES702, anuales+mensuales 2002–
    vivienda_terminada_canarias            OBSOLETA (solo ES70 anual total) — pendiente de eliminar
    viviendas_no_habituales_censos         Viviendas no hab. por municipio, censos 2001/2011/2021.
                                           2001/2011: "no principales" (vacías+secundarias); 2021: vacías+esporádicas.
                                           Metodologías no comparables directamente. 80/88 municipios.
                                           ine_viviendas_no_hab_historico.py + auxiliares/viviendas_no_habituales.R
    historico_plazas_regladas              Plazas regladas anuales, canarias+7 islas, 2009–
    historico_tasa_ocupacion_reglada       Tasa de ocupación por plaza (%), mismo origen y cobertura
    historico_estancia_media_reglada       Estancia media reglada (días) anual, canarias+7 islas, 2009–
                                           PERNOC/VIAJEROS sumando 27 nacs. Fuente: C00065A_000039.
    historico_estancia_media_vv            Estancia media VV (días) anual, canarias+7 islas, 2019–
                                           Ponderada por viviendas_reservadas. Fuente: pte_vacacional.
    frontur_turistas                       Turistas FRONTUR por territorio y mes, canarias+5 islas, 2010–
                                           Fuente: ISTAC E16028B_000016. Usada para calcular PTEt.
    egt_estancia_media                     Estancia media EGT anual (días), canarias+5 islas, 2010–
                                           NOCHES_PERNOCTADAS/TURISTAS. Fuente: ISTAC C00028A.
                                           Usada para calcular PTEt y el corrector proporcional PTEv.
    ech_tamano_hogar_ccaa                  Tamaño medio del hogar por CCAA y trimestre, Q1 2021–
    nucleos_censales                       Hogares por nº de núcleos familiares, 88 municipios, Censo 2021
                                           Formato ancho: hogares_0..3 + year. Solo nivel municipio.
                                           Integrada en snapshot (base_snapshots / full_snapshots).
    ech_hogares_tipo                       Hogares por tipo de hogar, solo Canarias, 2013-2021.
                                           Solo indicador analítico — NO integrada en snapshot.
                                           Fuentes y cobertura:
                                             ECH  2013-2020  INE op.274, anual, miles de hogares
                                                             ine_ech_tipo_hogar.py + importar_ech_hogares_tipo.R
                                             ECEPOV  2021    INE tabla 56531, quinquenal (próxima ed. ~2026)
                                                             ine_ech_hogares.py + importar_ech_hogares_tipo.R
                                             CENSO   2021    nucleos_censales → hogares_2+hogares_3
                                                             Cubre "Dos o más núcleos" que ECEPOV no desglosa
                                           Hueco sin dato: 2022-2025
    ech_tamano_hogar_ccaa                  Tamaño medio del hogar por CCAA y trimestre (ECH).
                                           Total nacional + 19 CCAA. Solo indicador analítico —
                                           NO integrada en snapshot. Permite comparar Canarias
                                           con el resto de CCAA. Cobertura: Q1 2021–presente.
                                           ccaa_cod "00"=nacional, "05"=Canarias (códigos INE 2 dígitos).
                                           ine_tamano_hogar_ccaa.py + importar_tamano_hogar_ccaa.R

  Tablas de clasificación:
    modalidades, tipologias, clasificaciones, destinos_turisticos
    at_canarias_no_microdestino   Plazas turísticas fuera de microdestinos por municipio
                                  (usada como pesos para el reparto de bolsas en pte_reglada)

  Salida calculada:
    base_snapshots                   Datos brutos por ámbito (sin ratios)
    full_snapshots                   Snapshot completo con todos los campos calculados
    mv_full_snapshots_dashboard      Vista materializada para el visor (con geometría)

  Metadatos:
    diccionario_de_datos             Campos, fórmulas, orden de cálculo, flags de exportación

# Documentación de tablas (pg_description / COMMENT ON TABLE)

Cada tabla y vista materializada de producción tiene un comentario estructurado cargado desde
`sql-init/02_comentarios_tablas.sql` (se aplica automáticamente al inicializar el contenedor
postgis via docker-entrypoint-initdb.d).

## Formato de los comentarios

```
Descripción libre en texto plano.

@fuente: Organismo o sistema de origen de los datos
@dataset: Código o nombre del dataset de origen
@descarga: script.py → tmp/archivo_YYYYMMDD.csv
@importacion: script.R
@cobertura_temporal: rango o descripción del período
@cobertura_geografica: ámbito territorial
@actualizacion: frecuencia o política de actualización
@notas: advertencias, casos especiales, limitaciones conocidas
```

Solo se incluyen los @tags relevantes para cada tabla. Los tags son parseables con la
expresión regular `/^@(\w+):\s*(.+)$/m` sobre el texto completo del comentario.

## Consulta base desde Drupal (o cualquier cliente SQL)

```sql
SELECT
  relname                             AS tabla,
  obj_description(oid, 'pg_class')    AS comentario
FROM pg_class
WHERE relkind IN ('r', 'm')
  AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
  AND relname != 'spatial_ref_sys'
ORDER BY relname;
```

`relkind = 'r'` → tablas ordinarias; `relkind = 'm'` → vistas materializadas.
`spatial_ref_sys` se excluye por ser tabla interna de PostGIS sin comentario de dominio.

## Reaplicar los comentarios (si se recrea el contenedor o se actualiza el fichero)

```bash
docker cp sql-init/02_comentarios_tablas.sql gis-canarias-production:/tmp/
docker exec gis-canarias-production psql -U gis_user -d viviendas_canarias \
  -f /tmp/02_comentarios_tablas.sql
```

# Docker

docker-compose.yml
  services:
    postgis:
      image: imresamu/postgis-arm64:16-3.5
      container_name: gis-canarias-production
      restart: unless-stopped
      ports:
        - "5432:5432"
      environment:
        POSTGRES_USER: gis_user
        POSTGRES_PASSWORD: GIS_Canarias_2024_Prod
        POSTGRES_DB: viviendas_canarias
      volumes:
        - ./postgis_data:/var/lib/postgresql/data
        - ./sql-init:/docker-entrypoint-initdb.d
      networks:
        - gis-network

    martin:
      build:
        context: .
        dockerfile: Dockerfile.martin
      container_name: martin-canarias-production
      restart: unless-stopped
      command: martin --base-path /martin
      environment:
        - DATABASE_URL=postgresql://gis_user:GIS_Canarias_2024_Prod@postgis:5432/viviendas_canarias
        - MAP_CORS="*"
        - WATCH_MODE=true
        - VIRTUAL_HOST=vtp.carlosespino.es
        - VIRTUAL_PORT=3000
        - VIRTUAL_PATH=/martin/
        - MARTIN_BASE_URL=https://vtp.carlosespino.es
      ports:
        - "3000:3000"
      networks:
        - gis-network
        - public-network
      depends_on:
        - postgis

  networks:
    gis-network:
      name: gis-network
      driver: bridge
    public-network:
      external: true
      name: nginx-proxy_default
