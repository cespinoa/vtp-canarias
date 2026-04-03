#!/usr/bin/env python3
"""
istac_explore.py
Explora la estructura del dataset C00065A_000042 (Población turística equivalente)
del ISTAC a través de la API de recursos estadísticos.

Sin dependencias externas.

Uso:
    python3 istac_explore.py
    python3 istac_explore.py --data       # Muestra también una muestra de los datos
    python3 istac_explore.py --exports    # Muestra formatos de exportación disponibles
"""

import argparse
import json
import sys
import urllib.request
import urllib.parse
from pprint import pformat

ISTAC_BASE = "https://datos.canarias.es/api/estadisticas"
AGENCY     = "ISTAC"
DATASET_ID = "C00065A_000042"
VERSION    = "~latest"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-explore/1.0)",
    "Accept": "application/json",
}


def get(url: str, params: dict = None) -> dict:
    if params:
        url += "?" + urllib.parse.urlencode(params)
    print(f"  GET {url}")
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── Metadatos del dataset ─────────────────────────────────────

def explore_metadata():
    section("Metadatos del dataset")
    url = f"{ISTAC_BASE}/statistical-resources/v1.0/datasets/{AGENCY}/{DATASET_ID}/{VERSION}"
    data = get(url, {"_type": "json"})

    meta = data.get("datasetMetadata") or data.get("dataset") or data
    title = (meta.get("name") or {}).get("text", [{}])
    print(f"\n  Título: {next((t['value'] for t in title if t.get('lang') == 'es'), 'N/D')}")

    # Fechas
    for campo in ("validFrom", "validTo", "nextVersion", "replaces"):
        if meta.get(campo):
            print(f"  {campo}: {meta[campo]}")

    # Dimensiones
    dims = (meta.get("dimensions") or {}).get("dimension", [])
    if not dims and "dimensions" in meta:
        dims = meta["dimensions"]
    print(f"\n  Dimensiones ({len(dims)}):")
    for d in dims:
        did  = d.get("id", "?")
        name = (d.get("name") or {})
        if isinstance(name, dict):
            name = next((t['value'] for t in name.get("text", []) if t.get('lang') == 'es'), did)
        print(f"    - {did}: {name}")
        # Valores de la dimensión
        vals = (d.get("dimensionValues") or {}).get("dimensionValue", [])
        if vals:
            print(f"      {len(vals)} valores, primeros 5:")
            for v in vals[:5]:
                vid   = v.get("id", "?")
                vname = (v.get("name") or {})
                if isinstance(vname, dict):
                    vname = next((t['value'] for t in vname.get("text", []) if t.get('lang') == 'es'), vid)
                print(f"        · {vid}: {vname}")

    # Atributos
    attrs = (meta.get("attributes") or {}).get("attribute", [])
    if attrs:
        print(f"\n  Atributos ({len(attrs)}):")
        for a in attrs[:5]:
            print(f"    - {a.get('id', '?')}")

    # Estructura bruta resumida (primeras claves)
    print(f"\n  Claves raíz de la respuesta: {list(data.keys())}")
    return data


# ── Muestra de datos ─────────────────────────────────────────

def explore_data():
    section("Muestra de datos (primeras observaciones)")
    url = f"{ISTAC_BASE}/statistical-resources/v1.0/datasets/{AGENCY}/{DATASET_ID}/{VERSION}/data"
    # Pedir pocas filas para explorar
    data = get(url, {"_type": "json", "limit": "10", "offset": "0"})

    print(f"\n  Claves raíz: {list(data.keys())}")

    # Intentar distintas estructuras de respuesta ISTAC
    obs = (
        data.get("data", {}).get("observations")
        or data.get("observations")
        or data.get("datasetData", {}).get("observations")
    )
    if obs:
        print(f"\n  Tipo de observaciones: {type(obs)}")
        if isinstance(obs, dict):
            sample = list(obs.items())[:5]
            print(f"  Muestra (clave: valor):")
            for k, v in sample:
                print(f"    {k}: {v}")
        elif isinstance(obs, list):
            print(f"  Primeras 3:")
            for row in obs[:3]:
                print(f"    {row}")
    else:
        print("\n  Estructura completa (truncada a 2000 chars):")
        print(json.dumps(data, ensure_ascii=False, indent=2)[:2000])


# ── Formatos de exportación ───────────────────────────────────

def explore_exports():
    section("Exportaciones disponibles")
    base_export = f"{ISTAC_BASE}/export/v1.0/datasets/{AGENCY}/{DATASET_ID}/{VERSION}"

    formats = {
        "CSV (SDMX)":    f"{base_export}/csv?_type=json",
        "TSV":           f"{base_export}/tsv?_type=json",
        "XLSX":          f"{base_export}/xlsx?_type=json",
        "JSON":          f"{base_export}/json?_type=json",
        "PDF":           f"{base_export}/pdf?_type=json",
    }

    for label, url in formats.items():
        print(f"\n  {label}")
        print(f"    URL: {url}")

    # Probar también la URL de exportación directa sin /export
    alt_csv = f"{ISTAC_BASE}/statistical-resources/v1.0/datasets/{AGENCY}/{DATASET_ID}/{VERSION}.csv"
    print(f"\n  CSV directo (alt): {alt_csv}")

    # Intentar HEAD en CSV para ver si responde
    try:
        req = urllib.request.Request(
            f"{base_export}/csv",
            headers=HEADERS,
            method="HEAD"
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            print(f"\n  HEAD /csv → {resp.status} {resp.reason}")
            print(f"  Content-Type: {resp.headers.get('Content-Type', 'N/D')}")
            print(f"  Content-Length: {resp.headers.get('Content-Length', 'N/D')}")
    except Exception as e:
        print(f"\n  HEAD /csv → {e}")


# ── Operación estadística relacionada ────────────────────────

def explore_operation():
    section("Operación estadística (contexto)")
    # El ID de operación suele ser la primera parte del dataset: C00065A
    op_id = DATASET_ID.split("_")[0]
    url = f"{ISTAC_BASE}/operations/v1.0/operations/ISTAC/{op_id}"
    try:
        data = get(url, {"_type": "json"})
        op = data.get("operation", data)
        name = (op.get("name") or {})
        if isinstance(name, dict):
            name = next((t['value'] for t in name.get("text", []) if t.get('lang') == 'es'), "?")
        print(f"\n  Operación: {op_id}")
        print(f"  Nombre: {name}")
        period = op.get("currentInternalPeriod") or op.get("currentPeriod", "?")
        print(f"  Periodo actual: {period}")
    except Exception as e:
        print(f"\n  No se pudo obtener la operación: {e}")


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Explora el dataset C00065A_000042 del ISTAC")
    parser.add_argument("--data",    action="store_true", help="Incluye muestra de datos")
    parser.add_argument("--exports", action="store_true", help="Prueba endpoints de exportación")
    args = parser.parse_args()

    try:
        explore_metadata()
    except Exception as e:
        print(f"\nERROR en metadatos: {e}", file=sys.stderr)
        print("Volcando respuesta cruda...")
        url = f"{ISTAC_BASE}/statistical-resources/v1.0/datasets/{AGENCY}/{DATASET_ID}/{VERSION}"
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url + "?_type=json", headers=HEADERS), timeout=30
            ).read()
            print(raw.decode()[:3000])
        except Exception as e2:
            print(f"También falló la cruda: {e2}")

    if args.data:
        try:
            explore_data()
        except Exception as e:
            print(f"\nERROR en datos: {e}", file=sys.stderr)

    if args.exports:
        explore_exports()

    explore_operation()
    print("\n")


if __name__ == "__main__":
    main()
