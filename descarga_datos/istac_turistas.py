#!/usr/bin/env python3
"""
istac_turistas.py
Descarga el dataset E16028B_000011 (Encuesta de Gasto Turístico — ISTAC)
y guarda el total de turistas llegados por isla y mes en
./tmp/turistas_YYYYMMDD.csv

Se extrae únicamente: TIPO_VIAJERO=TURISTA, LUGAR_RESIDENCIA=_T, MEDIDAS=TURISTAS.
Territorios cubiertos: ES704 (Fuerteventura), ES705 (Gran Canaria),
  ES707 (La Palma), ES708 (Lanzarote), ES709 (Tenerife).
  El dataset no incluye El Hierro (ES703), La Gomera (ES706) ni Canarias total.

Sin dependencias externas.

Uso:
    python3 istac_turistas.py
    python3 istac_turistas.py --raw
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
    "/v1.0/datasets/ISTAC/E16028B_000011/~latest?_type=json"
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
    Orden de iteración: TIME_PERIOD × TIPO_VIAJERO × LUGAR_RESIDENCIA × TERRITORIO × MEDIDAS
    Extrae solo TIPO_VIAJERO=TURISTA, LUGAR_RESIDENCIA=_T, MEDIDAS=TURISTAS.
    """
    data_dims  = raw["data"]["dimensions"]["dimension"]
    dim_values = extract_dim_values(data_dims)

    periodos    = dim_values["TIME_PERIOD"]
    tipos       = dim_values["TIPO_VIAJERO"]
    residencias = dim_values["LUGAR_RESIDENCIA"]
    territorios = dim_values["TERRITORIO"]
    medidas     = dim_values["MEDIDAS"]

    n_p  = len(periodos)
    n_tv = len(tipos)
    n_lr = len(residencias)
    n_t  = len(territorios)
    n_m  = len(medidas)

    obs_vals = [v.strip() for v in raw["data"]["observations"].split("|")]

    print(f"  Dimensiones: {n_p} periodos × {n_tv} tipos × {n_lr} residencias × {n_t} territorios × {n_m} medidas")
    print(f"  Observaciones esperadas: {n_p * n_tv * n_lr * n_t * n_m}")
    print(f"  Observaciones recibidas: {len(obs_vals)}")

    tv_idx = tipos.index("TURISTA")
    lr_idx = residencias.index("_T")
    m_idx  = medidas.index("TURISTAS")

    rows = []
    for p_i, periodo in enumerate(periodos):
        year = int(periodo[:4])
        mes  = int(periodo[6:8])
        for t_i, terr in enumerate(territorios):
            idx = (p_i * n_tv * n_lr * n_t * n_m +
                   tv_idx * n_lr * n_t * n_m +
                   lr_idx * n_t * n_m +
                   t_i * n_m +
                   m_idx)
            v = obs_vals[idx] if idx < len(obs_vals) else ""
            if v in ("", ".", ".."):
                continue
            rows.append({
                "territorio_codigo": terr,
                "year":              year,
                "mes":               mes,
                "turistas":          int(float(v)),
            })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga turistas llegados por isla del ISTAC (E16028B_000011)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"turistas_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando dataset ISTAC E16028B_000011...")
    raw = fetch_json(DATASET_URL)

    version     = raw.get("metadata", {}).get("version", "")
    last_update = raw.get("metadata", {}).get("lastUpdate", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"turistas_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = ["territorio_codigo", "year", "mes", "turistas"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:.1f} KB)")

    from collections import Counter
    por_terr = Counter(r["territorio_codigo"] for r in rows)
    print(f"\nFilas por territorio: {dict(sorted(por_terr.items()))}")

    # Muestra: último mes disponible
    ultimo = max(rows, key=lambda r: r["year"] * 100 + r["mes"])
    ultimo_periodo = f"{ultimo['year']}-M{ultimo['mes']:02d}"
    muestra = [r for r in rows if r["year"] == ultimo["year"] and r["mes"] == ultimo["mes"]]
    print(f"\nÚltimo período ({ultimo_periodo}):")
    for r in sorted(muestra, key=lambda r: -r["turistas"]):
        print(f"  {r['territorio_codigo']}: {r['turistas']:>10,}")


if __name__ == "__main__":
    main()
