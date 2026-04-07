#!/usr/bin/env python3
"""
censo2021_hogares.py
Descarga del Censo de Población y Viviendas 2021 (INE) el número de
hogares según número de núcleos familiares, a nivel municipal para
los 88 municipios de Canarias.

Endpoint: POST https://www.ine.es/Censo2021/api
  tabla:     hog  (Hogares)
  métrica:   SHOGARES
  variables: ID_RESIDENCIA_N3 (municipio) + ID_NUC_HOG (número de núcleos)

La petición devuelve datos de toda España (~8k municipios × categorías
de núcleos). Se filtran client-side los códigos INE 35xxx y 38xxx.

Salida: tmp/censo2021_hogares_YYYYMMDD.csv
  codigo_ine | nombre | num_nucleos | hogares
  (formato largo: una fila por combinación municipio × categoría)

Categorías esperadas de num_nucleos:
  Sin núcleo familiar | Un núcleo | Dos núcleos | Tres o más núcleos

Sin dependencias externas. Dato de referencia: Censo 2021 (foto fija).

Uso:
    python3 censo2021_hogares.py
    python3 censo2021_hogares.py --raw   # guarda también el JSON crudo
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

API_URL = "https://www.ine.es/Censo2021/api"

HEADERS = {
    "User-Agent":   "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":       "application/json",
    "Content-Type": "application/json",
}

TMP_DIR = Path("./tmp")

PAYLOAD = {
    "tabla":     "hog",
    "idioma":    "ES",
    "metrica":   ["SHOGARES"],
    "variables": ["ID_RESIDENCIA_N3", "ID_NUC_HOG"],
}


def post_json(url: str, payload: dict, retries: int = 3) -> dict:
    data = json.dumps(payload).encode("utf-8")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=180) as resp:
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


def parse_municipio(valor: str) -> tuple[str, str]:
    """
    Extrae (codigo_ine, nombre) de cadenas como '35001 Agaete'.
    El código INE ocupa los primeros 5 caracteres numéricos.
    """
    valor = valor.strip()
    partes = valor.split(" ", 1)
    if len(partes) == 2 and partes[0].isdigit() and len(partes[0]) == 5:
        return partes[0], partes[1].strip()
    return "", valor


def es_canarias(codigo: str) -> bool:
    return len(codigo) == 5 and codigo.startswith(("35", "38"))


def build_rows(data_items: list) -> list[dict]:
    rows = []
    secretos = 0
    for item in data_items:
        mun_str = item.get("ID_RESIDENCIA_N3", "")
        codigo, nombre = parse_municipio(mun_str)
        if not es_canarias(codigo):
            continue

        num_nucleos = str(item.get("ID_NUC_HOG", "")).strip()
        hogares = item.get("SHOGARES")

        if hogares is None:
            secretos += 1
            continue

        rows.append({
            "codigo_ine":  codigo,
            "nombre":      nombre,
            "num_nucleos": num_nucleos,
            "hogares":     int(hogares),
        })

    if secretos:
        print(f"  AVISO: {secretos} registros omitidos por secreto estadístico o valor nulo.",
              file=sys.stderr)

    return sorted(rows, key=lambda r: (r["codigo_ine"], r["num_nucleos"]))


def main():
    parser = argparse.ArgumentParser(
        description="Descarga hogares por núcleos familiares del Censo 2021 — municipios Canarias"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo de la API en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"censo2021_hogares_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Consultando API Censo 2021 (tabla hog, ID_NUC_HOG × municipios España)...",
          flush=True)
    respuesta = post_json(API_URL, PAYLOAD)

    # La API devuelve un error JSON si la consulta es demasiado grande
    if isinstance(respuesta, str) or "error" in str(respuesta).lower():
        print(f"ERROR inesperado de la API:\n{respuesta}", file=sys.stderr)
        sys.exit(1)

    if args.raw:
        raw_path = TMP_DIR / f"censo2021_hogares_raw_{date_str}.json"
        raw_path.write_text(json.dumps(respuesta, ensure_ascii=False, indent=2))
        print(f"  JSON crudo → {raw_path}")

    # La API del Censo 2021 usa "data" en minúscula (distinto a la API Tempus3)
    data_items = respuesta.get("data") or respuesta.get("Data") or []
    if not data_items:
        print("ERROR: la respuesta no contiene 'data'. Ejecuta con --raw para inspeccionar.",
              file=sys.stderr)
        sys.exit(1)

    print(f"  Registros recibidos (España): {len(data_items):,}")

    rows = build_rows(data_items)
    municipios = len({r["codigo_ine"] for r in rows})
    categorias = sorted({r["num_nucleos"] for r in rows})

    print(f"  Municipios Canarias: {municipios}")
    print(f"  Categorías de num_nucleos: {categorias}")

    if municipios == 0:
        print("AVISO: no se encontraron municipios canarios. Comprueba el formato del campo "
              "ID_RESIDENCIA_N3 en el JSON crudo (--raw).", file=sys.stderr)

    fieldnames = ["codigo_ine", "nombre", "num_nucleos", "hogares"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows)} filas → {csv_path} ({kb:.1f} KB)")

    # Resumen Canarias (suma de todos los municipios por categoría)
    from collections import defaultdict
    totales: dict[str, int] = defaultdict(int)
    for r in rows:
        totales[r["num_nucleos"]] += r["hogares"]

    if totales:
        print("\nResumen Canarias (suma de municipios):")
        total_general = sum(totales.values())
        print(f"  {'CATEGORÍA':<35} {'HOGARES':>10}  {'%':>6}")
        print(f"  {'-'*35} {'-'*10}  {'-'*6}")
        for cat in sorted(totales):
            pct = totales[cat] / total_general * 100 if total_general else 0
            print(f"  {cat:<35} {totales[cat]:>10,}  {pct:>5.1f}%")
        print(f"  {'TOTAL':<35} {total_general:>10,}  100.0%")


if __name__ == "__main__":
    main()
