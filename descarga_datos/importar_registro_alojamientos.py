#!/usr/bin/env python3
"""
importar_registro_alojamientos.py
Descarga los tres datasets del Registro General Turístico de Canarias
desde el catálogo CKAN de datos.canarias.es y los guarda en
importar_gobcan/historico/ con la fecha de actualización en el nombre.

Datasets:
  vv  — Viviendas Vacacionales
  ht  — Establecimientos Hoteleros
  ap  — Establecimientos Extrahoteleros (sin VV)

La fecha del fichero se obtiene del campo metadata_modified del catálogo
CKAN, de modo que refleja cuándo actualizó el GobCan el dataset, no cuándo
se ejecuta el script. Si ya existe un fichero con esa fecha se omite la
descarga.

Uso:
    python3 descarga_datos/importar_registro_alojamientos.py
    python3 descarga_datos/importar_registro_alojamientos.py --dataset vv
"""

import argparse
import json
import sys
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

CKAN_BASE = "https://datos.canarias.es/catalogos/general/api/action"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; registro-alojamientos/1.0)",
    "Accept": "*/*",
}

DATASETS = {
    "vv": {
        "label":  "Viviendas Vacacionales",
        "slug":   "establecimientos-extrahoteleros-de-tipologia-vivienda-vacacional-inscritos-en-el-registro",
        "prefix": "vv",
    },
    "ht": {
        "label":  "Establecimientos Hoteleros",
        "slug":   "establecimientos-hoteleros-inscritos-en-el-registro-general-turistico-de-canarias",
        "prefix": "ht",
    },
    "ap": {
        "label":  "Establecimientos Extrahoteleros (sin VV)",
        "slug":   "establecimientos-extrahoteleros-sin-viviendas-vacacionales-inscritos-en-el-registro",
        "prefix": "ap",
    },
}

HISTORICO_DIR = Path("importar_gobcan/historico")


def http_get(url: str, params: dict = None) -> bytes:
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        chunks = []
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)


def get_ckan_package(slug: str) -> dict:
    raw = http_get(f"{CKAN_BASE}/package_show", {"id": slug})
    data = json.loads(raw)
    if not data.get("success"):
        raise RuntimeError(f"CKAN error: {data.get('error')}")
    return data["result"]


def select_csv_resource(resources: list) -> dict | None:
    for r in resources:
        if r.get("format", "").upper() == "CSV":
            return r
    return resources[0] if resources else None


def download(key: str):
    ds = DATASETS[key]
    print(f"\n[{ds['label']}]")

    print("  Consultando CKAN... ", end="", flush=True)
    package = get_ckan_package(ds["slug"])

    modified = package.get("metadata_modified", "")
    date_str = (datetime.fromisoformat(modified).strftime("%Y-%m-%d")
                if modified else datetime.now().strftime("%Y-%m-%d"))
    print(f"última modificación: {date_str}")

    dest = HISTORICO_DIR / f"{ds['prefix']}-{date_str}.csv"
    if dest.exists():
        print(f"  Ya existe: {dest} — omitiendo.")
        return

    resource = select_csv_resource(package.get("resources", []))
    if not resource:
        raise RuntimeError("No se encontró ningún recurso en el dataset.")
    url = resource.get("url", "")
    if not url:
        raise RuntimeError("El recurso no tiene URL de descarga.")

    print(f"  Descargando → {dest.name} ... ", end="", flush=True)
    content = http_get(url)
    dest.write_bytes(content)

    n_registros = content.count(b"\n") - 1  # descontar cabecera
    kb = len(content) / 1024
    print(f"✓ {n_registros:,} registros ({kb:,.1f} KB)")


def main():
    parser = argparse.ArgumentParser(
        description="Descarga los registros de alojamientos turísticos de Canarias (CKAN)"
    )
    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()) + ["all"],
        default="all",
        help="Dataset a descargar (por defecto: all)",
    )
    args = parser.parse_args()

    HISTORICO_DIR.mkdir(parents=True, exist_ok=True)

    keys = list(DATASETS.keys()) if args.dataset == "all" else [args.dataset]

    for key in keys:
        try:
            download(key)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)

    print("\nListo.")


if __name__ == "__main__":
    main()
