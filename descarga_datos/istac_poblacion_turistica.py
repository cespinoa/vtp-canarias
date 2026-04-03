#!/usr/bin/env python3
"""
istac_poblacion_turistica.py
Descarga el dataset C00065A_000042 (Población turística equivalente,
islas y microdestinos de Canarias) del ISTAC y lo guarda como CSV
en ./tmp/poblacion_turistica_equivalente_YYYYMMDD.csv

El cubo tiene 3 dimensiones:
  MEDIDAS      → siempre POBLACION_TURISTICA_EQV (una sola medida)
  TIME_PERIOD  → años 2009-2025
  TERRITORIO   → códigos NUTS/LAU (Canarias, islas, microdestinos)

Las observaciones vienen como string "val1 | val2 | ..." indexado
por la combinación posicional (MEDIDAS × TIME_PERIOD × TERRITORIO).

Sin dependencias externas.

Uso:
    python3 istac_poblacion_turistica.py
    python3 istac_poblacion_turistica.py --raw    # guarda también el JSON crudo
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
    "/v1.0/datasets/ISTAC/C00065A_000042/~latest?_type=json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def extract_dim_values(data_dims: list) -> dict:
    """
    Devuelve {dimensionId: [code0, code1, ...]} en orden de índice,
    a partir de data['data']['dimensions']['dimension'].
    """
    result = {}
    for dim in data_dims:
        did   = dim["dimensionId"]
        codes = sorted(
            dim["representations"]["representation"],
            key=lambda r: r["index"]
        )
        result[did] = [r["code"] for r in codes]
    return result


def extract_territory_names(meta_dims: list) -> dict:
    """
    Devuelve {code: nombre_es} para la dimensión TERRITORIO
    a partir de metadata['dimensions']['dimension'].
    """
    names = {}
    for dim in meta_dims:
        if dim["id"] != "TERRITORIO":
            continue
        for val in dim.get("dimensionValues", {}).get("value", []):
            code = val.get("id", "")
            text_list = (val.get("name") or {}).get("text", [])
            name_es = next(
                (t["value"] for t in text_list if t.get("lang") == "es"),
                code
            )
            names[code] = name_es
    return names


def build_csv(raw: dict) -> list[dict]:
    """
    Reconstruye las observaciones en una lista de dicts con columnas:
        territorio_codigo, territorio_nombre, periodo, valor
    """
    data_dims = raw["data"]["dimensions"]["dimension"]
    meta_dims = raw["metadata"]["dimensions"]["dimension"]

    dim_values   = extract_dim_values(data_dims)
    terr_names   = extract_territory_names(meta_dims)

    medidas   = dim_values["MEDIDAS"]        # 1 valor
    periodos  = dim_values["TIME_PERIOD"]    # 17 años
    territorios = dim_values["TERRITORIO"]   # N territorios

    n_medidas     = len(medidas)
    n_periodos    = len(periodos)
    n_territorios = len(territorios)

    obs_str = raw["data"]["observations"]
    valores = [v.strip() for v in obs_str.split("|")]

    print(f"  Dimensiones: {n_medidas} medidas × {n_periodos} periodos × {n_territorios} territorios")
    print(f"  Observaciones esperadas: {n_medidas * n_periodos * n_territorios}")
    print(f"  Observaciones recibidas: {len(valores)}")

    rows = []
    idx  = 0
    for _m in medidas:           # solo POBLACION_TURISTICA_EQV
        for periodo in periodos:
            for terr_code in territorios:
                val_raw = valores[idx] if idx < len(valores) else ""
                idx += 1
                # Valor vacío o punto → NULL
                if val_raw in ("", ".", ".."):
                    val = None
                else:
                    try:
                        val = float(val_raw)
                    except ValueError:
                        val = None

                rows.append({
                    "territorio_codigo":  terr_code,
                    "territorio_nombre":  terr_names.get(terr_code, terr_code),
                    "periodo":            periodo,
                    "poblacion_turistica_equivalente": val,
                })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga población turística equivalente del ISTAC"
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="Guarda también el JSON crudo en tmp/"
    )
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"poblacion_turistica_equivalente_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print(f"Descargando dataset ISTAC C00065A_000042...")
    raw = fetch_json(DATASET_URL)

    # Fecha real de los datos (lastUpdate del metadata)
    last_update = raw.get("metadata", {}).get("lastUpdate", "")
    version     = raw.get("metadata", {}).get("version", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"poblacion_turistica_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = [
        "territorio_codigo",
        "territorio_nombre",
        "periodo",
        "poblacion_turistica_equivalente",
    ]

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:,.1f} KB)")

    # Muestra de las primeras filas
    print("\nPrimeras 6 filas:")
    print(f"  {'CODIGO':<12} {'NOMBRE':<40} {'AÑO':<6} {'VALOR'}")
    print(f"  {'-'*12} {'-'*40} {'-'*6} {'-'*15}")
    for row in rows[:6]:
        val = f"{row['poblacion_turistica_equivalente']:,.2f}" if row['poblacion_turistica_equivalente'] is not None else "NULL"
        print(f"  {row['territorio_codigo']:<12} {row['territorio_nombre'][:40]:<40} {row['periodo']:<6} {val}")


if __name__ == "__main__":
    main()
