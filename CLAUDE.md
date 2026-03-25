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
- Los ficheros estructura_de_la_base_de_datos.csv y diccionario_de_datos.csv documentan el esquema y los campos calculados. El diccionario_de_datos también existe como tabla en la base de datos y es leído en tiempo de ejecución por PT02.

# Pipeline de importación (importar_gobcan/)

Secuencia de 12 scripts (P01–P12) que transforman CSVs del GobCan en datos de producción:

  - helper.R           Utilidades compartidas: conexión DB, logging, normalización de texto
  - P01               Ingesta controlada desde vv.csv y at.csv → staging_import
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
    - Hace TRUNCATE de base_snapshots y captura datos brutos para los cuatro ámbitos:
      canarias, isla, municipio, localidad
    - Incluye: oferta VV/AR, población, PTE, viviendas, superficie, hogares
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
    - Exporta 3 JSONs a /home/carlos/visor/web/sites/default/files/visor/:
        datos_dashboard.json  → snapshot actual (canarias/isla/municipio)
        series.json           → histórico completo (campos con comparable=TRUE)
        localidades.json      → datos de localidades del snapshot actual

Scripts de validación y calidad (no forman parte del pipeline ordinario):

  calidad_datos.R     Informe de calidad: geocodificación, plazas, traspasos municipales
  paranoia_test.R     Verificación independiente: recalcula manualmente con SQL directo
                      para un caso concreto y compara contra full_snapshots

# Scripts descartados / archivados

  S01-totalizar_alojamientos.R     Prototipo inicial. Escribía en tabla snapshots (obsoleta),
                                   ratios hardcodeados, sin diccionario. No usar.
  T01, T02                         Versiones intermedias (fórmulas en formulas.csv externo,
                                   pipeline monolítico, sin deduplicación). No usar.
  PT04-importar_historico_islas.R  Movido a carpeta auxiliar. Script de uso único para
                                   cargar datos históricos pre-sistema desde columnas de
                                   la tabla islas. Solo ejecutar si se vacía full_snapshots.

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

  Tablas de clasificación:
    modalidades, tipologias, clasificaciones, destinos_turisticos

  Salida calculada:
    base_snapshots                   Datos brutos por ámbito (sin ratios)
    full_snapshots                   Snapshot completo con todos los campos calculados
    mv_full_snapshots_dashboard      Vista materializada para el visor (con geometría)

  Metadatos:
    diccionario_de_datos             Campos, fórmulas, orden de cálculo, flags de exportación

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
