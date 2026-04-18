#!/usr/bin/env python3
"""
istac_egt_estancia.py
Descarga la estancia media EGT (Encuesta sobre el Gasto Turístico — ISTAC C00028A)
para Canarias y las 5 islas principales y guarda los resultados anuales en
./tmp/egt_estancia_YYYYMMDD.csv

La estancia media se calcula como NOCHES_PERNOCTADAS / TURISTAS, ambas del mismo
dataset, lo que garantiza consistencia metodológica.

Territorios: ES70 (Canarias), ES704 (Fuerteventura), ES705 (Gran Canaria),
  ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife).

Fuentes:
  C00028A_000003 — Turistas por isla y año/trimestre
  C00028A_000004 — Noches pernoctadas por isla y año/trimestre

Sin dependencias externas.

Uso:
    python3 istac_egt_estancia.py
    python3 istac_egt_estancia.py --raw
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

BASE_URL = (
    "https://datos.canarias.es/api/estadisticas/statistical-resources"
    "/v1.0/datasets/ISTAC/{code}/~latest?_type=json"
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


def extract_dim(data_dims: list, dim_id: str) -> list[str]:
    for dim in data_dims:
        if dim["dimensionId"] == dim_id:
            return [r["code"] for r in sorted(
                dim["representations"]["representation"],
                key=lambda r: r["index"]
            )]
    return []


def build_series(raw: dict, medida: str) -> dict[tuple, float]:
    """Devuelve dict {(territorio, year): valor} para períodos anuales.
    Calcula los strides a partir del orden real de dimensiones en el JSON."""
    data_dims  = raw["data"]["dimensions"]["dimension"]
    dim_order  = [(dim["dimensionId"], extract_dim(data_dims, dim["dimensionId"]))
                  for dim in data_dims]
    sizes      = [len(vals) for _, vals in dim_order]
    strides    = [1]
    for s in reversed(sizes[1:]):
        strides.insert(0, strides[0] * s)
    dim_dict   = {name: vals for name, vals in dim_order}
    obs_vals   = [v.strip() for v in raw["data"]["observations"].split("|")]

    result = {}
    for p_i, periodo in enumerate(dim_dict["TIME_PERIOD"]):
        if len(periodo) != 4:          # solo años completos
            continue
        year = int(periodo)
        for t_i, terr in enumerate(dim_dict["TERRITORIO"]):
            sel = {name: vals[0] for name, vals in dim_order}  # defaults
            sel["MEDIDAS"]      = medida
            sel["TERRITORIO"]   = terr
            sel["TIME_PERIOD"]  = periodo
            idx = sum(dim_dict[name].index(sel[name]) * stride
                      for (name, _), stride in zip(dim_order, strides))
            v = obs_vals[idx] if idx < len(obs_vals) else ""
            if v not in ("", ".", ".."):
                result[(terr, year)] = float(v)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Descarga estancia media EGT por isla del ISTAC (C00028A)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también los JSON crudos en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"egt_estancia_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando C00028A_000003 (turistas EGT)...")
    raw3 = fetch_json(BASE_URL.format(code="C00028A_000003"))
    print("Descargando C00028A_000004 (noches pernoctadas EGT)...")
    raw4 = fetch_json(BASE_URL.format(code="C00028A_000004"))

    last3 = raw3.get("metadata", {}).get("lastUpdate", "")
    last4 = raw4.get("metadata", {}).get("lastUpdate", "")
    print(f"  Última actualización turistas  : {last3}")
    print(f"  Última actualización noches    : {last4}")

    if args.raw:
        for code, raw in [("000003", raw3), ("000004", raw4)]:
            p = TMP_DIR / f"egt_raw_{code}_{date_str}.json"
            p.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
            print(f"  JSON crudo → {p}")

    turistas = build_series(raw3, "TURISTAS")
    noches   = build_series(raw4, "NOCHES_PERNOCTADAS")

    rows = []
    for (terr, year), t in sorted(turistas.items()):
        n = noches.get((terr, year))
        if n and t > 0:
            rows.append({
                "geo_code":      terr,
                "year":          year,
                "estancia_media": round(n / t, 4),
            })

    fieldnames = ["geo_code", "year", "estancia_media"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"\n  ✓ {len(rows):,} filas → {csv_path} ({kb:.1f} KB)")

    # Resumen: últimos años para Canarias
    canarias = [(r["year"], r["estancia_media"]) for r in rows if r["geo_code"] == "ES70"]
    print("\nEstancia media EGT Canarias (últimos 5 años):")
    for year, est in sorted(canarias)[-5:]:
        print(f"  {year}: {est} días")


if __name__ == "__main__":
    main()
