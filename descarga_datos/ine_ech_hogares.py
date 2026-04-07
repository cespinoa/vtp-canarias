#!/usr/bin/env python3
"""
ine_ech_hogares.py
Descarga datos de hogares según tipo de hogar para Canarias del INE,
combinando dos fuentes:

  ECEPOV (Encuesta de Características Esenciales de Población y Viviendas)
    — Continuadora de la ECH desde 2021. En Tempus3 cada edición anual
      es una tabla independiente (un snapshot por año, sin dimensión temporal).
      Nivel CCAA (Canarias), sin desglose provincial.
      Categorías de tipo de hogar (5):
        Hogar unipersonal
        Padre/madre sólo/a con hijos que conviven en el hogar
        Pareja sin hijos que conviven en el hogar
        Pareja con hijos que conviven en el hogar
        Otros tipos de hogar   ← incluye plurinucleares y otras formas atípicas

  NOTA sobre la ECH histórica (2013–2020):
    La ECH (operación 274 del JAXI) contiene datos provinciales con categoría
    explícita "Hogares con dos o más núcleos". Sin embargo, NO está disponible
    en el sistema Tempus3 ni via ningún endpoint API público del INE. Solo es
    accesible descargando manualmente las tablas Excel desde la web del INE:
    https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736176952

Estrategia:
  1. Para cada tabla ECEPOV definida en ECEPOV_TABLAS ({id: año}), descarga
     DATOS_TABLA/{id} y extrae las series de Canarias con habitaciones=Total.
  2. Produce un CSV con todas las ediciones disponibles.
  3. Cuando el INE publique una nueva edición ECEPOV, añadir su ID y año
     a ECEPOV_TABLAS y volver a ejecutar el script.

Formato de las series ECEPOV en Tempus3:
  Nombre: "Territorio, Tipo_hogar, Habitaciones"
  Ejemplo: "Canarias, Hogar unipersonal, Total"
  Data: [{"Valor": 214604.0, "Secreto": false}]  ← sin campo Anyo

Salida: tmp/ine_ech_hogares_YYYYMMDD.csv
  anyo | tipo_hogar | hogares

Sin dependencias externas.

Uso:
    python3 ine_ech_hogares.py
    python3 ine_ech_hogares.py --raw   # guarda JSON crudo de cada tabla
"""

import argparse
import csv
import sys
import json
import urllib.request
from datetime import datetime
from pathlib import Path

BASE_URL = "https://servicios.ine.es/wstempus/js/ES"

# Tablas ECEPOV conocidas: {id_tabla_tempus3: año_de_referencia}
# Añadir aquí las ediciones futuras cuando el INE las publique.
# Para descubrir el ID de una nueva edición, buscar en:
#   https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177092
# y consultar: https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{id}
ECEPOV_TABLAS = {
    56531: 2021,
    # 99999: 2022,   # añadir cuando esté disponible
    # 99999: 2023,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

# Categorías de tipo_hogar a extraer (excluye "Total" agregado)
TIPOS_HOGAR_EXCLUIR = {"total", "Total"}


def fetch_json(url: str, retries: int = 3) -> list:
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=120) as resp:
                chunks = []
                while True:
                    chunk = resp.read(1024 * 64)
                    if not chunk:
                        break
                    chunks.append(chunk)
                return json.loads(b"".join(chunks))
        except Exception as e:
            if attempt == retries:
                raise
            print(f"  Intento {attempt} fallido ({e}), reintentando...", flush=True)


def extraer_canarias(datos: list, anyo: int) -> list[dict]:
    """
    Extrae filas de Canarias desde una tabla ECEPOV.

    Formato del nombre: "Territorio, Tipo_hogar, Habitaciones"
    Filtra: territorio=="Canarias", habitaciones=="Total", tipo_hogar!=Total

    Devuelve lista de {anyo, tipo_hogar, hogares}.
    """
    rows = []
    sin_parsear = 0

    for serie in datos:
        nombre = serie.get("Nombre", "")
        partes = [p.strip() for p in nombre.split(", ")]

        if len(partes) != 3:
            sin_parsear += 1
            continue

        territorio, tipo_hogar, habitaciones = partes

        if territorio != "Canarias":
            continue
        if habitaciones != "Total":
            continue
        if tipo_hogar in TIPOS_HOGAR_EXCLUIR:
            continue

        data_pts = serie.get("Data", [])
        if not data_pts:
            continue

        for pt in data_pts:
            if pt.get("Secreto", False):
                continue
            val = pt.get("Valor")
            if val is None:
                continue
            rows.append({
                "anyo":       anyo,
                "tipo_hogar": tipo_hogar,
                "hogares":    int(val),
            })

    if sin_parsear:
        print(f"  AVISO: {sin_parsear} series con nombre no parseable (esperado: 3 partes).",
              file=sys.stderr)

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga ECEPOV (hogares por tipo) para Canarias"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también los JSON crudos en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"ine_ech_hogares_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    all_rows: list[dict] = []

    for tabla_id, anyo in sorted(ECEPOV_TABLAS.items(), key=lambda x: x[1]):
        print(f"Descargando DATOS_TABLA/{tabla_id} (ECEPOV {anyo})...", flush=True)
        try:
            datos = fetch_json(f"{BASE_URL}/DATOS_TABLA/{tabla_id}")
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            continue

        print(f"  Series recibidas: {len(datos):,}")

        if args.raw:
            p = TMP_DIR / f"ine_ecepov_{anyo}_raw_{date_str}.json"
            p.write_text(json.dumps(datos, ensure_ascii=False, indent=2))
            print(f"  JSON crudo → {p}")

        filas = extraer_canarias(datos, anyo)
        print(f"  Filas Canarias extraídas: {len(filas)}")
        all_rows.extend(filas)

    if not all_rows:
        print("ERROR: no se extrajeron filas. "
              "Comprueba los IDs en ECEPOV_TABLAS o ejecuta con --raw.",
              file=sys.stderr)
        sys.exit(1)

    all_rows.sort(key=lambda r: (r["anyo"], r["tipo_hogar"]))

    fieldnames = ["anyo", "tipo_hogar", "hogares"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    kb = csv_path.stat().st_size / 1024
    print(f"\n  ✓ {len(all_rows)} filas → {csv_path} ({kb:.1f} KB)")

    # Resumen
    total_por_anyo: dict[int, int] = {}
    for r in all_rows:
        total_por_anyo[r["anyo"]] = total_por_anyo.get(r["anyo"], 0) + r["hogares"]

    print("\nResumen por año (Canarias, suma de tipos):")
    print(f"  {'AÑO':>5}  {'TOTAL HOGARES':>15}")
    print(f"  {'-----':>5}  {'-'*15}")
    for anyo in sorted(total_por_anyo):
        print(f"  {anyo:>5}  {total_por_anyo[anyo]:>15,}")

    print(f"\nTipos de hogar disponibles ({sorted(ECEPOV_TABLAS.values())[0]}+):")
    tipos = sorted({r["tipo_hogar"] for r in all_rows})
    for t in tipos:
        print(f"  {t}")

    print("\nNOTA: la ECH (2013–2020) con categoría 'plurinuclear' explícita")
    print("  no está disponible via API. Ver docstring del script para más info.")


if __name__ == "__main__":
    main()
