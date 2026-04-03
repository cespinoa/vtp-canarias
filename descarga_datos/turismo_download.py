#!/usr/bin/env python3
"""
turismo_download.py
Descarga los CSVs del Registro General Turístico de Canarias y del ISTAC
y los guarda en ./tmp/ con la fecha en el nombre.

Uso:
    python3 turismo_download.py
    python3 turismo_download.py --dataset vv
    python3 turismo_download.py --dataset poblacion_turistica
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
    "User-Agent": "Mozilla/5.0 (compatible; turismo-download/1.0)",
    "Accept": "*/*",
}

DATASETS = {
    "vv": {
        "label":  "Viviendas Vacacionales",
        "slug":   "establecimientos-extrahoteleros-de-tipologia-vivienda-vacacional-inscritos-en-el-registro",
        "url":    "https://datos.canarias.es/catalogos/general/dataset/9f4355a2-d086-4384-ba72-d8c99aa2d544/resource/8ff8cc43-c00b-4513-8f42-a5b961c579e1/download/establecimientos-extrahoteleros-de-tipologia-vivienda-vacacional-inscritos-en-el-registro-genera.csv",
        "prefix": "viviendas_vacacionales",
        "source": "ckan",
    },
    "hoteleros": {
        "label":  "Establecimientos Hoteleros",
        "slug":   "establecimientos-hoteleros-inscritos-en-el-registro-general-turistico-de-canarias",
        "url":    "https://datos.canarias.es/catalogos/general/dataset/429db33d-cbce-4920-b1b6-b4dde9e5f90f/resource/87741d75-2ce2-4a45-8131-ad8263257664/download/establecimientos-hoteleros-inscritos-en-el-registro-general-turistico-de-canarias.csv",
        "prefix": "establecimientos_hoteleros",
        "source": "ckan",
    },
    "extrahoteleros": {
        "label":  "Extrahoteleros sin Viviendas Vacacionales",
        "slug":   "establecimientos-extrahoteleros-sin-viviendas-vacacionales-inscritos-en-el-registro",
        "url":    "https://datos.canarias.es/catalogos/general/dataset/1364104c-b86c-4ab9-8ef5-12fdf399aa01/resource/d98c2617-db26-4d15-8ee4-3b2da1130bd0/download/establecimientos-extrahoteleros-sin-viviendas-vacacionales-inscritos-en-el-registro-general-turi.csv",
        "prefix": "establecimientos_extrahoteleros",
        "source": "ckan",
    },
    "poblacion_turistica": {
        "label":  "Población Turística Equivalente (ISTAC)",
        "url":    "https://datos.canarias.es/api/estadisticas/statistical-resources/v1.0/datasets/ISTAC/C00065A_000042/1.8.csv",
        "prefix": "poblacion_turistica_equivalente",
        "source": "istac_direct",
        # Nota: la versión "1.8" está fija en la URL. Cuando el ISTAC publique
        # una versión nueva aparecerá como 1.9, 2.0, etc. Actualizar aquí.
    },
}


def http_get(url: str, params: dict = None) -> bytes:
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def get_remote_date(slug: str) -> str:
    """Consulta CKAN y devuelve metadata_modified como YYYYMMDD."""
    raw = http_get(f"{CKAN_BASE}/package_show", {"id": slug})
    data = json.loads(raw)
    if not data.get("success"):
        raise RuntimeError(f"CKAN error: {data.get('error')}")
    ts = data["result"].get("metadata_modified", "")
    return datetime.fromisoformat(ts).strftime("%Y%m%d") if ts else datetime.now().strftime("%Y%m%d")


def download(key: str, tmp_dir: Path):
    ds = DATASETS[key]
    print(f"\n[{ds['label']}]")

    # Determinar fecha para el nombre del fichero
    print("  Consultando fecha... ", end="", flush=True)
    if ds.get("source") == "istac_direct":
        # URL con versión fija: usamos fecha de descarga como etiqueta
        date_str = datetime.now().strftime("%Y%m%d")
        print(f"{date_str} (fecha de descarga)")
    else:
        date_str = get_remote_date(ds["slug"])
        print(date_str)

    # Nombre de fichero: prefijo_YYYYMMDD.csv
    filename = tmp_dir / f"{ds['prefix']}_{date_str}.csv"

    if filename.exists():
        print(f"  Ya existe: {filename} — omitiendo.")
        return

    # Descarga
    print(f"  Descargando → {filename.name} ... ", end="", flush=True)
    content = http_get(ds["url"])
    filename.write_bytes(content)
    kb = len(content) / 1024
    print(f"{kb:,.1f} KB")


def main():
    parser = argparse.ArgumentParser(description="Descarga CSVs turísticos de Canarias Datos Abiertos")
    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()) + ["all"],
        default="all",
        help="Dataset a descargar (por defecto: all)",
    )
    args = parser.parse_args()

    keys = list(DATASETS.keys()) if args.dataset == "all" else [args.dataset]

    tmp_dir = Path("./tmp")
    tmp_dir.mkdir(exist_ok=True)
    print(f"Directorio de destino: {tmp_dir.resolve()}")

    for key in keys:
        try:
            download(key, tmp_dir)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)

    print("\nListo.")


if __name__ == "__main__":
    main()
