#!/usr/bin/env python3
"""
turismo_ckan.py
Explora y descarga los tres datasets del Registro General Turístico de Canarias
desde el catálogo CKAN de datos.canarias.es

Uso:
    python3 turismo_ckan.py --info          # Muestra recursos disponibles en los 3 datasets
    python3 turismo_ckan.py --download      # Descarga todos los datasets activos
    python3 turismo_ckan.py --download --dataset vv   # Solo viviendas vacacionales
"""

import argparse
import json
import sys
from pathlib import Path
import urllib.request
import urllib.parse

# ──────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────

CKAN_BASE = "https://datos.canarias.es/catalogos/general/api/action"

DATASETS = {
    "vv": {
        "label": "Viviendas Vacacionales",
        "slug": "establecimientos-extrahoteleros-de-tipologia-vivienda-vacacional-inscritos-en-el-registro",
    },
    "hoteleros": {
        "label": "Establecimientos Hoteleros",
        "slug": "establecimientos-hoteleros-inscritos-en-el-registro-general-turistico-de-canarias",
    },
    "extrahoteleros": {
        "label": "Extrahoteleros sin Viviendas Vacacionales",
        "slug": "establecimientos-extrahoteleros-sin-viviendas-vacacionales-inscritos-en-el-registro",
    },
}

OUTPUT_DIR = Path("./datos_turisticos")


# ──────────────────────────────────────────────
# Utilidades
# ──────────────────────────────────────────────

def ckan_request(action: str, params: dict) -> dict:
    """Llama a la Action API de CKAN y devuelve el resultado."""
    url = f"{CKAN_BASE}/{action}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            if not data.get("success"):
                print(f"  ERROR CKAN: {data.get('error', {})}")
                return {}
            return data.get("result", {})
    except Exception as e:
        print(f"  Error de red: {e}")
        return {}


def download_url(url: str, dest: Path):
    """Descarga una URL a un fichero local con barra de progreso simple."""
    print(f"  Descargando → {dest.name}")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            chunk_size = 65536
            downloaded = 0
            with open(dest, "wb") as f:
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded * 100 // total
                        print(f"\r  {pct}% ({downloaded:,} / {total:,} bytes)", end="", flush=True)
            print(f"\r  ✓ {downloaded:,} bytes guardados en {dest}")
    except Exception as e:
        print(f"\n  Error descargando {url}: {e}")


# ──────────────────────────────────────────────
# Fase 1: Inspección
# ──────────────────────────────────────────────

def inspect_datasets(keys: list):
    """Muestra todos los recursos disponibles en los datasets indicados."""
    for key in keys:
        ds = DATASETS[key]
        print(f"\n{'='*60}")
        print(f"  {ds['label']}")
        print(f"  slug: {ds['slug']}")
        print(f"{'='*60}")

        result = ckan_request("package_show", {"id": ds["slug"]})
        if not result:
            print("  No se pudo obtener información.")
            continue

        print(f"  Título    : {result.get('title', 'N/D')}")
        print(f"  Licencia  : {result.get('license_title', 'N/D')}")
        modified = result.get("metadata_modified", "N/D")
        print(f"  Modificado: {modified}")
        print(f"  Recursos  :")

        resources = result.get("resources", [])
        for i, r in enumerate(resources):
            print(f"\n    [{i}] {r.get('name', 'sin nombre')}")
            print(f"        id      : {r['id']}")
            print(f"        formato : {r.get('format', 'N/D')}")
            print(f"        url     : {r.get('url', 'N/D')}")
            # Comprobar si tiene datastore activo
            ds_info = ckan_request("datastore_info", {"id": r["id"]})
            if ds_info:
                fields = ds_info.get("fields", [])
                print(f"        datastore: SÍ ({len(fields)} campos)")
                print(f"        campos   : {', '.join(f['id'] for f in fields[:10])}" +
                      (" ..." if len(fields) > 10 else ""))
            else:
                print(f"        datastore: NO (solo descarga directa)")


# ──────────────────────────────────────────────
# Fase 2: Descarga
# ──────────────────────────────────────────────

def download_datasets(keys: list):
    """Descarga el recurso CSV/JSON más reciente de cada dataset."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    for key in keys:
        ds = DATASETS[key]
        print(f"\n{'='*60}")
        print(f"  {ds['label']}")
        print(f"{'='*60}")

        result = ckan_request("package_show", {"id": ds["slug"]})
        if not result:
            print("  No se pudo obtener información. Saltando.")
            continue

        resources = result.get("resources", [])

        # Seleccionar el mejor recurso: preferir CSV, luego JSON, luego cualquiera
        preferred = None
        for fmt in ["CSV", "JSON", "XLSX"]:
            for r in resources:
                if r.get("format", "").upper() == fmt:
                    preferred = r
                    break
            if preferred:
                break
        if not preferred and resources:
            preferred = resources[0]

        if not preferred:
            print("  Sin recursos disponibles.")
            continue

        url = preferred.get("url", "")
        fmt = preferred.get("format", "dat").lower()
        filename = OUTPUT_DIR / f"{key}.{fmt}"

        print(f"  Recurso seleccionado : {preferred.get('name', 'N/D')}")
        print(f"  Formato              : {fmt.upper()}")
        print(f"  URL                  : {url}")

        if url:
            download_url(url, filename)
        else:
            print("  Sin URL de descarga directa.")


# ──────────────────────────────────────────────
# Entrada
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Descarga datasets turísticos de Canarias Datos Abiertos")
    parser.add_argument("--info", action="store_true", help="Inspecciona los datasets y muestra recursos disponibles")
    parser.add_argument("--download", action="store_true", help="Descarga los datasets")
    parser.add_argument(
        "--dataset",
        choices=list(DATASETS.keys()) + ["all"],
        default="all",
        help="Dataset a procesar (por defecto: all)"
    )
    args = parser.parse_args()

    if not args.info and not args.download:
        parser.print_help()
        sys.exit(0)

    keys = list(DATASETS.keys()) if args.dataset == "all" else [args.dataset]

    if args.info:
        inspect_datasets(keys)
    if args.download:
        download_datasets(keys)


if __name__ == "__main__":
    main()
