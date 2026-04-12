-- ==============================================================================
-- COMENTARIOS DE TABLAS Y VISTAS MATERIALIZADAS
-- Formato @etiqueta para generación de documentación estructurada desde pg_description.
-- Consulta: SELECT relname, obj_description(oid,'pg_class') FROM pg_class
--           WHERE relkind IN ('r','m') AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='public');
-- ==============================================================================

-- ============================================================
-- TABLAS MAESTRAS GEOGRÁFICAS
-- ============================================================

COMMENT ON TABLE canarias IS
'Polígono de contorno del archipiélago canario. Usado como geometría de fondo en el visor y para cálculos de ámbito Canarias en las vistas materializadas.

@fuente: Elaboración propia a partir de datos IGN / CNIG
@cobertura_geografica: Comunidad Autónoma de Canarias (1 registro)
@actualizacion: Estático';

COMMENT ON TABLE islas IS
'Tabla maestra de las 7 islas. Contiene nombre, código INE, geometría y campos auxiliares (tipo_isla). Clave foránea de referencia en la mayor parte de las tablas estadísticas y de alojamiento.

@fuente: Elaboración propia
@cobertura_geografica: 7 islas de Canarias
@actualizacion: Estático';

COMMENT ON TABLE municipios IS
'Tabla maestra de los 88 municipios de Canarias. Contiene nombre, código INE, isla_id, geometría y tipo_municipio. Clave foránea de referencia en tablas estadísticas, alojamientos y snapshots.

@fuente: Elaboración propia a partir de datos INE / IGN
@cobertura_geografica: 88 municipios de Canarias
@actualizacion: Estático (modificaciones puntuales por cambios administrativos)';

COMMENT ON TABLE localidades IS
'Tabla maestra de localidades (entidades de población). Contiene nombre, municipio_id, isla_id y geometría de punto representativo. Nivel más fino de la jerarquía territorial del sistema.

@fuente: Elaboración propia
@cobertura_geografica: Canarias — todas las localidades con actividad turística o poblacional relevante
@actualizacion: Ocasional';

COMMENT ON TABLE recintos_municipales IS
'Límites municipales en formato INSPIRE (Directiva europea de infraestructuras de datos espaciales). Polígonos MultiPolygon con atributos INSPIREID, NATCODE y códigos NUTS. Usados para geocodificación y análisis espacial.

@fuente: IGN — Servicio WFS INSPIRE de límites administrativos
@cobertura_geografica: 88 municipios de Canarias
@actualizacion: Ocasional (alineado con publicaciones IGN)';


COMMENT ON TABLE mapa_municipios IS
'Tabla de equivalencias entre nombres de municipios en formato bruto (texto del GobCan) y su nombre normalizado. Usada durante el pipeline de importación (P00) para corregir variantes tipográficas y de acentuación antes de la ingesta.

@fuente: Elaboración propia — mantenida manualmente al detectar nuevas variantes
@cobertura_geografica: 88 municipios de Canarias
@actualizacion: Manual (cuando el GobCan introduce nuevas variantes en los CSV)';

-- ============================================================
-- GEOCODIFICACIÓN Y CARTOGRAFÍA AUXILIAR
-- ============================================================

COMMENT ON TABLE callejero_portales IS
'Callejero de portales de Canarias con coordenadas. Base de la geocodificación de primer nivel (P02) del pipeline de importación: se busca la dirección más similar mediante distancia de edición (fuzzy match, umbral >0.45 con filtro municipal). Incluye campos normalizados (dir_normalizada, nombre_via_norm, loca_norm) para mejorar la tasa de acierto.

@fuente: Cartociudad (IGN) — descarga estatal filtrada por Canarias
@cobertura_geografica: Canarias — ~526.000 portales geocodificados
@descarga: Proceso manual de descarga desde Cartociudad + carga via script auxiliar
@actualizacion: Ocasional (cada actualización de Cartociudad)';

COMMENT ON TABLE portales_canarias IS
'Tabla auxiliar de portales simplificada, derivada de callejero_portales. Contiene nombre de vía, número, código postal, municipio e isla en texto plano. Usada como caché de consulta rápida en fases del pipeline donde no se necesita la geometría completa.

@fuente: Derivada de callejero_portales
@cobertura_geografica: Canarias — ~526.000 portales
@actualizacion: Sincronizada con callejero_portales';

COMMENT ON TABLE codigos_postales IS
'Relación de códigos postales de Canarias con su municipio e isla. Usada como fallback de geocodificación (P04) cuando no se puede resolver la dirección por callejero ni por centroide de localidad. Contiene num_registros (portales asociados a ese CP en el callejero).

@fuente: Derivada de callejero_portales por agregación
@cobertura_geografica: Canarias — 583 códigos postales
@actualizacion: Sincronizada con callejero_portales';

COMMENT ON TABLE centroides_localidad IS
'Puntos centroide por localidad. Fallback de geocodificación P03: cuando no se resuelve por callejero, se asignan las coordenadas del centroide de la localidad indicada en el registro fuente.

@fuente: Calculada a partir de localidades.geom
@cobertura_geografica: Canarias — una fila por localidad
@actualizacion: Sincronizada con localidades';

COMMENT ON TABLE centroides_cp IS
'Puntos centroide por código postal. Fallback de geocodificación P04.

@fuente: Calculada a partir de codigos_postales y callejero_portales
@cobertura_geografica: Canarias — una fila por código postal
@actualizacion: Sincronizada con codigos_postales';

COMMENT ON TABLE centroides_municipio IS
'Puntos centroide por municipio. Fallback de geocodificación P05: último recurso cuando ningún otro método resuelve la dirección.

@fuente: Calculada a partir de municipios.geom
@cobertura_geografica: 88 municipios de Canarias
@actualizacion: Sincronizada con municipios';

-- ============================================================
-- TABLAS DE CLASIFICACIÓN
-- ============================================================

COMMENT ON TABLE modalidades IS
'Tabla maestra de modalidades de alojamiento (VV = Vivienda Vacacional, AR = Alojamiento Rural, etc.). Clave foránea en alojamientos.

@fuente: Elaboración propia a partir del catálogo del GobCan
@actualizacion: Manual (cuando el GobCan introduce nuevas modalidades)';

COMMENT ON TABLE tipologias IS
'Tabla maestra de tipologías de alojamiento (unifamiliar, apartamento, etc.). Clave foránea en alojamientos.

@fuente: Elaboración propia a partir del catálogo del GobCan
@actualizacion: Manual';

COMMENT ON TABLE clasificaciones IS
'Tabla maestra de clasificaciones/categorías de alojamiento (número de llaves, estrellas, etc.). Clave foránea en alojamientos.

@fuente: Elaboración propia a partir del catálogo del GobCan
@actualizacion: Manual';

COMMENT ON TABLE destinos_turisticos IS
'Catálogo de localidades turísticas (microdestinos) reconocidos por el ISTAC. Se usa en la asignación de PTE reglada: las localidades con código en esta tabla reciben PTE directamente; las que no, reciben la bolsa insular repartida por at_canarias_no_microdestino.

@fuente: ISTAC — Dataset C00065A_000042 (códigos de destino turístico)
@cobertura_geografica: Canarias — destinos con suelo turístico clasificado
@actualizacion: Ocasional (cuando el ISTAC modifica el catálogo de destinos)';

COMMENT ON TABLE at_canarias_no_microdestino IS
'Plazas de alojamiento turístico reglado situadas fuera de microdestinos (suelo residencial o rural), por municipio e isla. Se usa como vector de reparto para distribuir la "bolsa" de PTE insular entre municipios sin destino turístico propio. El ratio_municipio = plazas_municipio / total_plazas_isla.

@fuente: Calculada a partir de alojamientos y destinos_turisticos
@cobertura_geografica: Canarias — municipios con oferta reglada fuera de microdestinos
@actualizacion: Regenerada manualmente cuando cambia la oferta reglada de forma significativa';

-- ============================================================
-- PIPELINE DE IMPORTACIÓN — DATOS PRIMARIOS
-- ============================================================

COMMENT ON TABLE staging_import IS
'Tabla de trabajo temporal del pipeline de importación de alojamientos. Recibe los registros crudos de los CSV del GobCan (P01) y va acumulando resultados de geocodificación (P02–P06), auditoría espacial (P07) y asignación de localidad (P08). Se vacía en cada nueva importación. No usar como fuente de datos de producción.

@fuente: CSV Gobierno de Canarias (datos.canarias.es) — Viviendas Vacacionales + Alojamientos Turísticos
@descarga: turismo_download.py (producción) / turismo_ckan.py (exploración)
@importacion: importar_gobcan/P00.R → P01.R → … → P11.R
@actualizacion: En cada ciclo de importación (frecuencia: según publicación GobCan)';

COMMENT ON TABLE alojamientos IS
'Tabla de producción con el registro auditado de todos los alojamientos turísticos de Canarias (viviendas vacacionales y establecimientos reglados). Cada fila representa un establecimiento con su historial de alta/baja, coordenadas validadas, clasificación territorial y rastro de auditoría de geocodificación. La clave única es establecimiento_id (código oficial GobCan). Los registros de baja conservan fecha_baja pero permanecen en la tabla para el historial.

@fuente: CSV Gobierno de Canarias (datos.canarias.es)
@descarga: turismo_download.py
@importacion: importar_gobcan/P01.R … P11.R (pipeline completo)
@cobertura_temporal: Desde la primera importación del sistema hasta la fecha más reciente procesada
@cobertura_geografica: Canarias — 88 municipios
@actualizacion: Según publicación del GobCan (frecuencia variable, típicamente mensual)';

-- ============================================================
-- DATOS ESTADÍSTICOS DE ENTRADA
-- ============================================================

COMMENT ON TABLE poblacion IS
'Series históricas de población residente por ámbito territorial (Padrón Municipal de Habitantes). Fuente dual: ISTAC para Canarias/isla y municipios históricos (1986–1995); INE para municipios 1996 en adelante. El campo fuente registra el origen de cada registro.

@fuente: ISTAC C00025A_000002 (Canarias, islas, municipios 1986–1995) + INE tabla 29005 (municipios 1996+)
@descarga: istac_poblacion.py → tmp/poblacion_YYYYMMDD.csv / ine_poblacion.py → tmp/ine_poblacion_YYYYMMDD.csv
@importacion: importar_poblacion.R (ISTAC) + importar_poblacion_ine.R (INE, UPSERT sobre municipios)
@cobertura_temporal: 1986–año actual (sin 1991 y 1997)
@cobertura_geografica: Canarias, 7 islas, 88 municipios
@actualizacion: Anual (el INE publica el año en curso antes que el ISTAC)';

COMMENT ON TABLE hogares IS
'Datos censales de hogares y tamaño medio del hogar por ámbito territorial, extraídos de las ediciones del Censo de Población y Viviendas. Cubre 22 ediciones censales desde 1842. PT01 usa el campo miembros (personas_por_hogar) para calcular viviendas_necesarias y déficit.

@fuente: ISTAC C00025A_000001 (Población, hogares y tamaño medio según censos)
@descarga: istac_hogares.py → tmp/hogares_YYYYMMDD.csv
@importacion: importar_hogares.R
@cobertura_temporal: 1842–2021 (22 ediciones censales)
@cobertura_geografica: Canarias, 7 islas, 88 municipios
@actualizacion: Decenal (próxima edición: Censo 2031)';

COMMENT ON TABLE nucleos_censales IS
'Distribución de hogares según número de núcleos familiares por municipio, extraída del Censo 2021 (componente encuesta). Columnas: hogares_0 (sin núcleo = unipersonales o personas sin relación), hogares_1 (un núcleo = familia nuclear estándar), hogares_2 (dos núcleos = plurinuclear), hogares_3 (tres o más núcleos). Alimenta los campos hogares_0..3 y deficit_teorico_viviendas del snapshot. No equivale a viviendas_municipios (metodología distinta: encuesta vs consumo eléctrico).

@fuente: INE — Censo de Población y Viviendas 2021 (API POST Censo2021)
@dataset: tabla="hog", variable ID_NUC_HOG
@descarga: descarga_datos/censo2021_hogares.py → tmp/censo2021_hogares_YYYYMMDD.csv
@importacion: descarga_datos/importar_censo2021_hogares.R
@cobertura_temporal: 2021 (foto fija; próximo censo ~2031)
@cobertura_geografica: 88 municipios de Canarias (3 con secreto estadístico → hogares_* = 0)
@actualizacion: Decenal';

COMMENT ON TABLE ech_hogares_tipo IS
'Serie temporal de hogares según tipo de hogar para Canarias, combinando dos fuentes con distinta metodología y categorías. ECH 2013–2020: encuesta anual, unidad miles de hogares, incluye categoría "Dos o más núcleos familiares" explícita. ECEPOV 2021: encuesta quinquenal (componente del Censo 2021), unidad convertida a miles, la categoría "Otros tipos" agrupa plurinucleares sin desglosalos. Censo 2021 (fuente=CENSO): complementa con el dato explícito de plurinucleares de nucleos_censales. Solo ámbito Canarias — no integrar en el pipeline de snapshots.

@fuente: ECH (INE operación 274) + ECEPOV (INE tabla 56531) + Censo 2021 (nucleos_censales)
@descarga: ine_ech_tipo_hogar.py → tmp/ine_ech_tipo_hogar_YYYYMMDD.csv / ine_ech_hogares.py → tmp/ine_ech_hogares_YYYYMMDD.csv
@importacion: descarga_datos/importar_ech_hogares_tipo.R
@cobertura_temporal: 2013–2021 (ECH: 2013–2020 anual; ECEPOV: 2021; próxima edición ECEPOV ~2026)
@cobertura_geografica: Canarias (solo CCAA — sin desglose provincial ni municipal)
@actualizacion: ECH: descontinuada (sustituida por ECEPOV). ECEPOV: quinquenal. Próxima actualización ~2026.
@notas: La ECH no está disponible en Tempus3/API; se descarga directamente del JAXI como CSV. La ECEPOV es quinquenal, no hay datos 2022–2025.';

COMMENT ON TABLE viviendas_no_habituales_censos IS
'Viviendas no habituales por municipio en los tres últimos censos disponibles.
Permite analizar la evolución del stock no ocupado a nivel municipal (2001, 2011, 2021).

CAUTELA METODOLÓGICA: los tres valores NO son comparables directamente.
  no_hab_2001, no_hab_2011: "viviendas no principales" (vacías + secundarias juntas),
    Censo INE de encuesta de campo. Fuente: PC-Axis nal02.px / 02mun00.px (JAXI p07).
  no_hab_2021: vacías + esporádicas del Censo 2021, metodología de consumo eléctrico.
    La clasificación eléctrica tiende a elevar el número de vacías respecto a la encuesta.
Solo disponible para municipios >2.000 hab en 2001/2011 (80 de 88 en Canarias).

@fuente: INE — Censos de Población y Viviendas 2001/2011/2021
@descarga: ine_viviendas_no_hab_historico.py → tmp/viviendas_no_hab_YYYYMMDD.csv
@importacion: auxiliares/viviendas_no_habituales.R
@cobertura_temporal: Censos 2001, 2011, 2021 (tres puntos)
@cobertura_geografica: 80 municipios de Canarias (>2.000 hab en 2001/2011); 88 con dato 2021
@actualizacion: Estático (censal)';

COMMENT ON TABLE ech_tamano_hogar_ccaa IS
'Tamaño medio del hogar (personas/hogar) por comunidad autónoma y trimestre.
Permite comparar la evolución de Canarias frente al resto de CCAA y al total nacional.
Canarias (ccaa_cod=05) se sitúa por encima de la media nacional de forma estable (~2.60 vs 2.49 en 2026-T1).
Solo indicador analítico — no integrada en el pipeline de snapshots.

@fuente: INE, tabla 60132 (Encuesta Continua de Hogares)
@descarga: ine_tamano_hogar_ccaa.py → tmp/tamano_hogar_ccaa_YYYYMMDD.csv
@importacion: descarga_datos/importar_tamano_hogar_ccaa.R
@cobertura_temporal: Q1 2021 – trimestre más reciente publicado (trimestral)
@cobertura_geografica: Total nacional (ccaa_cod=00) + 19 comunidades y ciudades autónomas
@actualizacion: Trimestral (TRUNCATE + reload; INE revisa datos retroactivos)';

COMMENT ON TABLE viviendas_municipios IS
'Distribución de viviendas según uso (habitual, vacía, esporádica) por municipio, isla y Canarias. Una fila por ámbito y edición censal (campo year). Basada en metodología de consumo eléctrico (>750 kWh/año = vivienda habitual). Los niveles isla y canarias se calculan por agregación desde municipios.

@fuente: INE — Censo de Población y Viviendas 2021, tabla 59531
@descarga: ine_viviendas.py → tmp/ine_viviendas_YYYYMMDD.csv
@importacion: importar_viviendas.R [acepta fecha del censo: Rscript importar_viviendas.R 2031-12-31]
@cobertura_temporal: 2021-12-31 (Censo 2021); próxima edición ~2031
@cobertura_geografica: Canarias, 7 islas, 88 municipios
@actualizacion: Decenal
@notas: Metodología eléctrica → diferente del total de hogares encuestados (nucleos_censales/ECEPOV). El Censo 2021 usó consumos de 2020 (año COVID), por lo que muchas VV clasificaron como no habituales. PT02 aplica corrección automática: si year < 2026-01-01, viviendas_disponibles = viviendas_habituales (sin descontar VV). Totales 2021: 1.088.700 viviendas (793.529 habituales, 211.452 vacías, 83.719 esporádicas).';

COMMENT ON TABLE superficies IS
'Superficies territoriales en hectáreas por ámbito (Canarias, isla, municipio, localidad). Usada en PT01 para calcular superficie_km2 en el snapshot.

@fuente: Elaboración propia a partir de geometrías IGN
@cobertura_geografica: Canarias, islas, municipios, localidades
@actualizacion: Estático';

COMMENT ON TABLE pte_vacacional IS
'Presión Turística Equivalente (PTE) de Vivienda Vacacional por ámbito y mes. La PTEv es la media diaria de plazas VV ocupadas en el mes: ptev = plazas_disponibles × (tasa_reservada/100). PT01 toma la media de los últimos 12 meses para eliminar estacionalidad (campo pte_v en base_snapshots).

@fuente: ISTAC C00065A_000061 (Estadística de Vivienda Vacacional)
@descarga: istac_pte_vv.py → tmp/pte_vv_YYYYMMDD.csv
@importacion: descarga_datos/importar_pte_vv.R
@cobertura_temporal: 2019-M01 hasta el mes más reciente publicado (mensual)
@cobertura_geografica: Canarias (ES70), 7 islas (ES703–ES709), 88 municipios (35xxx/38xxx)
@actualizacion: Mensual (ISTAC revisa datos retroactivos → estrategia TRUNCATE + reload)';

COMMENT ON TABLE pte_reglada IS
'Presión Turística Equivalente (PTE) de alojamiento reglado por localidad turística, municipio (calculado), isla y Canarias. El ISTAC organiza los datos por microdestinos; las plazas fuera de ellos se agrupan en una "bolsa" insular que se reparte entre municipios en proporción a sus camas en at_canarias_no_microdestino.

@fuente: ISTAC C00065A_000042 (Población Turística Equivalente)
@descarga: istac_poblacion_turistica.py → tmp/poblacion_turistica_equivalente_YYYYMMDD.csv
@importacion: descarga_datos/importar_pte_reglada.R
@cobertura_temporal: Serie histórica anual y mensual disponible en el dataset
@cobertura_geografica: Canarias, 7 islas, localidades turísticas, 88 municipios (calculado)
@actualizacion: El ISTAC revisa valores retroactivos → estrategia TRUNCATE + reload
@notas: Descuadre conocido del ISTAC en 2020: GC exceso +15,86 / TF defecto −15,86 (simétrico, no es error nuestro).';

COMMENT ON TABLE historico_plazas_regladas IS
'Plazas turísticas regladas (todas las categorías) por isla y año. Usadas en PT01 para contextualizar la oferta histórica. La caída pronunciada en 2020 refleja los cierres por COVID.

@fuente: ISTAC C00065A_000033 (Encuesta de Ocupación en Alojamientos Turísticos)
@descarga: istac_plazas.py → tmp/plazas_YYYYMMDD.csv
@importacion: importar_plazas.R
@cobertura_temporal: 2009–año más reciente publicado (anual)
@cobertura_geografica: Canarias + 7 islas (sin desglose municipal)
@actualizacion: Anual';

COMMENT ON TABLE historico_tasa_ocupacion_reglada IS
'Tasa de ocupación por plaza (%) del alojamiento turístico reglado por isla y año. Complemento de historico_plazas_regladas: mismo origen, misma cobertura, pero almacena el índice de ocupación en lugar del stock de plazas. La caída pronunciada en 2020 refleja los cierres por COVID (42%); récord hasta 2025 en 73,83%.

@fuente: ISTAC C00065A_000033 (Encuesta de Ocupación en Alojamientos Turísticos), medida TASA_OCUPACION_PLAZA
@descarga: istac_plazas.py → tmp/plazas_YYYYMMDD.csv (columna tasa_ocupacion_plaza)
@importacion: importar_plazas.R
@cobertura_temporal: 2009–año más reciente publicado (anual)
@cobertura_geografica: Canarias + 7 islas (sin desglose municipal)
@actualizacion: Anual (TRUNCATE + reload junto con historico_plazas_regladas)';

COMMENT ON TABLE turistas_llegadas IS
'Turistas llegados por isla y mes. Solo incluye las 5 islas principales (sin El Hierro ni La Gomera, que no están en el dataset ISTAC). Tipo: TURISTA (excluye excursionistas); mercado: todos los orígenes.

@fuente: ISTAC E16028B_000011 (Encuesta de Gasto Turístico)
@descarga: istac_turistas.py → tmp/turistas_YYYYMMDD.csv
@importacion: importar_turistas.R
@cobertura_temporal: 2010-M01 hasta el mes más reciente publicado (mensual)
@cobertura_geografica: ES704 (Fuerteventura), ES705 (Gran Canaria), ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife)
@actualizacion: Mensual (TRUNCATE + reload)';

COMMENT ON TABLE vivienda_iniciada_terminada_canarias IS
'Viviendas iniciadas y terminadas en Canarias (libre y protegida) por territorio y período. Territorios: ES70 (Canarias), ES701 (Las Palmas), ES702 (SCT). Períodos: anuales (YYYY) y mensuales (YYYY-Mxx). Seis campos: iniciadas/terminadas × total/libres/protegidas.

@fuente: ISTAC E25004A_000001 (Viviendas iniciadas y terminadas en Canarias)
@descarga: istac_vivienda_construccion.py → tmp/vivienda_YYYYMMDD.csv
@importacion: importar_vivienda_construccion.R
@cobertura_temporal: ES70: 2002–presente; ES701/ES702: 2008–presente (anual y mensual)
@cobertura_geografica: Canarias (ES70), provincia Las Palmas (ES701), provincia SCT (ES702)
@actualizacion: El ISTAC revisa valores retroactivos → TRUNCATE + reload';

-- ============================================================
-- PIPELINE DE SNAPSHOTS — SALIDA CALCULADA
-- ============================================================

COMMENT ON TABLE base_snapshots IS
'Datos brutos capturados por PT01 para cada fecha de proceso. Contiene una fila por ámbito (canarias, isla, municipio, localidad) con los datos de entrada sin ratios calculados: oferta VV/AT, población, PTE (reglada y vacacional), viviendas, superficies, hogares y núcleos censales. PT02 lee esta tabla para calcular los ratios y escribir full_snapshots. Se hace TRUNCATE en cada ejecución de PT01.

@importacion: informes/PT01-Capturar_datos_base.R
@cobertura_geografica: Canarias, 7 islas, 88 municipios, localidades
@actualizacion: En cada ciclo de importación + cálculo de snapshots';

COMMENT ON TABLE full_snapshots IS
'Snapshot completo con todos los indicadores calculados (ratios, porcentajes, benchmarks avg/max) por ámbito y fecha de proceso. Generado por PT02 a partir de base_snapshots usando las fórmulas del diccionario_de_datos. Es la tabla de origen para la vista materializada mv_full_snapshots_dashboard y para las exportaciones JSON del visor. Se acumulan snapshots históricos (no se trunca).

@importacion: informes/PT02-Calcular_ratios_dinamicos.R (lee diccionario_de_datos para fórmulas)
@cobertura_geografica: Canarias, 7 islas, 88 municipios, localidades
@actualizacion: En cada ciclo; PT02 acepta parámetro --fecha para recalcular snapshots históricos de pipeline';

COMMENT ON TABLE diccionario_de_datos IS
'Metadiccionario de todos los indicadores del dashboard. Cada fila define un campo de full_snapshots: nombre, fórmula R (ejecutada por PT02), orden de cálculo, formato, flag en_mv (¿se exporta a la vista materializada?), comparable (¿aparece en series.json?) y otros metadatos de visualización.

@importacion: Mantenido manualmente + actualizaciones puntuales vía SQL o script
@actualizacion: Manual al añadir nuevos indicadores al pipeline';

-- ============================================================
-- VISTAS MATERIALIZADAS
-- ============================================================

COMMENT ON MATERIALIZED VIEW mv_full_snapshots_dashboard IS
'Vista materializada para el visor cartográfico. Consolida el snapshot más reciente de Canarias, islas y municipios (no localidades) con geometría adjunta, filtrando solo los campos marcados como en_mv=TRUE en diccionario_de_datos. Incluye índices GIST y CLUSTER para rendimiento en Raspberry Pi. Se regenera al final de PT03 y requiere reinicio del contenedor Martin para ser visible vía tile server.

@importacion: informes/PT03-Exportar_datos.R (REFRESH MATERIALIZED VIEW + REINDEX + CLUSTER)
@actualizacion: En cada ciclo de PT03';

COMMENT ON MATERIALIZED VIEW v_mapa_etiquetas IS
'Vista materializada de etiquetas para el visor cartográfico. Una fila por ámbito (canarias, isla, municipio) con el texto de la etiqueta y un punto representativo calculado con ST_PointOnSurface. Alimenta la capa de etiquetas del mapa.

@importacion: Generada a partir de full_snapshots + geometrías de islas/municipios/canarias
@actualizacion: En cada ciclo de PT03';
