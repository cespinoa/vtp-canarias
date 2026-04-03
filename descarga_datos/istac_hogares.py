#!/usr/bin/env python3
"""
istac_hogares.py
Descarga el dataset C00025A_000001 (Población, hogares y tamaño medio
de los hogares según censos. Municipios de Canarias) del ISTAC y lo
guarda como CSV en ./tmp/hogares_YYYYMMDD.csv

El cubo tiene 3 dimensiones:
  TIME_PERIOD  → 22 ediciones censales (1768–2021)
  TERRITORIO   → Canarias, islas, municipios y entidades históricas
  MEDIDAS      → POBLACION | HOGARES | HOGARES_TAMANIO_MEDIO

Se extraen HOGARES y HOGARES_TAMANIO_MEDIO para todos los territorios
estándar (sin sufijos _E##/_N##) que tengan dato.

Sin dependencias externas.

Uso:
    python3 istac_hogares.py
    python3 istac_hogares.py --raw
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

DATASET_URL = (
    "https://datos.canarias.es/api/estadisticas/statistical-resources"
    "/v1.0/datasets/ISTAC/C00025A_000001/~latest?_type=json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")


def fetch_json(url: str, retries: int = 3) -> dict:
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


def extract_dim_values(data_dims: list) -> dict:
    result = {}
    for dim in data_dims:
        did   = dim["dimensionId"]
        codes = sorted(
            dim["representations"]["representation"],
            key=lambda r: r["index"]
        )
        result[did] = [r["code"] for r in codes]
    return result


def build_csv(raw: dict) -> list[dict]:
    """
    Reconstruye las observaciones y devuelve filas con:
        territorio_codigo, periodo, hogares, hogares_tamanio_medio

    Se descartan:
      - Territorios con sufijo _E## o _N## (entidades históricas)
      - Filas donde ambas medidas son nulas
    """
    data_dims = raw["data"]["dimensions"]["dimension"]
    dim_values = extract_dim_values(data_dims)

    periodos    = dim_values["TIME_PERIOD"]
    territorios = dim_values["TERRITORIO"]
    medidas     = dim_values["MEDIDAS"]

    n_p = len(periodos)
    n_t = len(territorios)
    n_m = len(medidas)

    obs_vals = [v.strip() for v in raw["data"]["observations"].split("|")]

    print(f"  Dimensiones: {n_p} periodos × {n_t} territorios × {n_m} medidas")
    print(f"  Observaciones esperadas: {n_p * n_t * n_m}")
    print(f"  Observaciones recibidas: {len(obs_vals)}")

    def parse_val(v):
        if v in ("", ".", ".."):
            return None
        try:
            return float(v)
        except ValueError:
            return None

    # Índice: TIME_PERIOD × TERRITORIO × MEDIDAS
    idx_medidas = {m: i for i, m in enumerate(medidas)}
    rows = []

    idx = 0
    for periodo in periodos:
        for terr in territorios:
            vals = {}
            for medida in medidas:
                v = obs_vals[idx] if idx < len(obs_vals) else ""
                vals[medida] = parse_val(v)
                idx += 1

            # Descartar entidades históricas (_E## o _N##)
            if "_" in terr:
                continue

            h  = vals.get("HOGARES")
            tm = vals.get("HOGARES_TAMANIO_MEDIO")

            # Solo filas con al menos un valor
            if h is None and tm is None:
                continue

            rows.append({
                "territorio_codigo":    terr,
                "periodo":              periodo,
                "hogares":              int(h) if h is not None else "",
                "hogares_tamanio_medio": tm if tm is not None else "",
            })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga hogares censales del ISTAC (C00025A_000001)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"hogares_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando dataset ISTAC C00025A_000001...")
    raw = fetch_json(DATASET_URL)

    version     = raw.get("metadata", {}).get("version", "")
    last_update = raw.get("metadata", {}).get("lastUpdate", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"hogares_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = ["territorio_codigo", "periodo", "hogares", "hogares_tamanio_medio"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:,.1f} KB)")

    # Resumen por período
    from collections import Counter
    conteo = Counter(r["periodo"] for r in rows)
    ultimos = sorted(conteo.keys())[-5:]
    print("\nÚltimos 5 censos (territorios con dato):")
    for p in ultimos:
        print(f"  {p}: {conteo[p]} territorios")

    # Muestra Canarias + municipios grandes en 2021
    print("\nMuestra 2021:")
    muestra = [r for r in rows if r["periodo"] == "2021"
               and r["territorio_codigo"] in ("ES70", "38038", "35016", "38023")]
    for r in sorted(muestra, key=lambda x: x["territorio_codigo"]):
        print(f"  {r['territorio_codigo']:<8} hogares={r['hogares']:>10}  "
              f"tamanio_medio={r['hogares_tamanio_medio']}")


if __name__ == "__main__":
    main()
