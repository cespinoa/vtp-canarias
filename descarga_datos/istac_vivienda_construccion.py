#!/usr/bin/env python3
"""
istac_vivienda.py
Descarga el dataset E25004A_000001 (Viviendas iniciadas y terminadas en Canarias)
del ISTAC y lo guarda como CSV en ./tmp/vivienda_YYYYMMDD.csv

Dimensiones del cubo (orden en observaciones): TIME_PERIOD × MEDIDAS × TERRITORIO
  TIME_PERIOD  → periodos anuales (YYYY) y mensuales (YYYY-Mxx) desde 2002
  MEDIDAS      → 6: VIVIENDAS_TERMINADAS / _LIBRES / _PROTEGIDAS
                    VIVIENDAS_INICIADAS  / _LIBRES / _PROTEGIDAS
  TERRITORIO   → territorios España; se filtran ES70, ES701, ES702

Sin dependencias externas.

Uso:
    python3 istac_vivienda.py
    python3 istac_vivienda.py --raw
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
    "/v1.0/datasets/ISTAC/E25004A_000001/~latest?_type=json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

TERRITORIOS_CANARIAS = {"ES70", "ES701", "ES702"}

MEDIDAS_ORDEN = [
    "VIVIENDAS_TERMINADAS",
    "VIVIENDAS_TERMINADAS_LIBRES",
    "VIVIENDAS_TERMINADAS_PROTEGIDAS",
    "VIVIENDAS_INICIADAS",
    "VIVIENDAS_INICIADAS_LIBRES",
    "VIVIENDAS_INICIADAS_PROTEGIDAS",
]


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


def parse_periodo(periodo: str) -> tuple:
    """
    Devuelve (tipo_periodo, year, mes).
    Formato anual:   "2024"     → ('anual',   2024, None)
    Formato mensual: "2024-M01" → ('mensual', 2024, 1)
    """
    if "-M" in periodo:
        partes = periodo.split("-M")
        return "mensual", int(partes[0]), int(partes[1])
    else:
        return "anual", int(periodo), None


def build_csv(raw: dict) -> list[dict]:
    """
    Reconstruye observaciones y devuelve filas con:
        territorio_codigo, periodo, tipo_periodo, year, mes,
        viviendas_terminadas, viviendas_terminadas_libres, viviendas_terminadas_protegidas,
        viviendas_iniciadas, viviendas_iniciadas_libres, viviendas_iniciadas_protegidas

    Orden de iteración: TIME_PERIOD × MEDIDAS × TERRITORIO
    Se filtran únicamente ES70, ES701, ES702.
    Se descartan filas donde todas las medidas son nulas.
    """
    data_dims  = raw["data"]["dimensions"]["dimension"]
    dim_values = extract_dim_values(data_dims)

    periodos    = dim_values["TIME_PERIOD"]
    medidas     = dim_values["MEDIDAS"]
    territorios = dim_values["TERRITORIO"]

    n_p = len(periodos)
    n_m = len(medidas)
    n_t = len(territorios)

    obs_vals = [v.strip() for v in raw["data"]["observations"].split("|")]

    print(f"  Dimensiones: {n_p} periodos × {n_m} medidas × {n_t} territorios")
    print(f"  Observaciones esperadas: {n_p * n_m * n_t}")
    print(f"  Observaciones recibidas: {len(obs_vals)}")

    def parse_val(v):
        if v in ("", ".", ".."):
            return None
        try:
            return int(float(v))
        except ValueError:
            return None

    # Acumular por (territorio, periodo) → dict de medidas
    acum: dict[tuple, dict] = {}

    idx = 0
    for periodo in periodos:
        for medida in medidas:
            for terr in territorios:
                v = obs_vals[idx] if idx < len(obs_vals) else ""
                idx += 1

                if terr not in TERRITORIOS_CANARIAS:
                    continue
                val = parse_val(v)
                if val is None:
                    continue

                key = (terr, periodo)
                if key not in acum:
                    acum[key] = {m: None for m in MEDIDAS_ORDEN}
                if medida in acum[key]:
                    acum[key][medida] = val

    rows = []
    for (terr, periodo), vals in sorted(acum.items()):
        tipo_periodo, year, mes = parse_periodo(periodo)
        rows.append({
            "territorio_codigo":               terr,
            "periodo":                         periodo,
            "tipo_periodo":                    tipo_periodo,
            "year":                            year,
            "mes":                             mes if mes is not None else "",
            "viviendas_terminadas":            vals["VIVIENDAS_TERMINADAS"]            if vals["VIVIENDAS_TERMINADAS"]            is not None else "",
            "viviendas_terminadas_libres":     vals["VIVIENDAS_TERMINADAS_LIBRES"]     if vals["VIVIENDAS_TERMINADAS_LIBRES"]     is not None else "",
            "viviendas_terminadas_protegidas": vals["VIVIENDAS_TERMINADAS_PROTEGIDAS"] if vals["VIVIENDAS_TERMINADAS_PROTEGIDAS"] is not None else "",
            "viviendas_iniciadas":             vals["VIVIENDAS_INICIADAS"]             if vals["VIVIENDAS_INICIADAS"]             is not None else "",
            "viviendas_iniciadas_libres":      vals["VIVIENDAS_INICIADAS_LIBRES"]      if vals["VIVIENDAS_INICIADAS_LIBRES"]      is not None else "",
            "viviendas_iniciadas_protegidas":  vals["VIVIENDAS_INICIADAS_PROTEGIDAS"]  if vals["VIVIENDAS_INICIADAS_PROTEGIDAS"]  is not None else "",
        })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga viviendas iniciadas/terminadas del ISTAC (E25004A_000001)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"vivienda_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando dataset ISTAC E25004A_000001...")
    raw = fetch_json(DATASET_URL)

    version     = raw.get("metadata", {}).get("version", "")
    last_update = raw.get("metadata", {}).get("lastUpdate", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"vivienda_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = [
        "territorio_codigo", "periodo", "tipo_periodo", "year", "mes",
        "viviendas_terminadas", "viviendas_terminadas_libres", "viviendas_terminadas_protegidas",
        "viviendas_iniciadas",  "viviendas_iniciadas_libres",  "viviendas_iniciadas_protegidas",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:,.1f} KB)")

    # Resumen por tipo de período y territorio
    from collections import Counter
    por_tipo = Counter(r["tipo_periodo"] for r in rows)
    por_terr = Counter(r["territorio_codigo"] for r in rows)
    print(f"\nPor tipo de período: {dict(por_tipo)}")
    print(f"Por territorio:      {dict(sorted(por_terr.items()))}")

    # Muestra: años recientes ES70 (anuales, terminadas)
    muestra = [r for r in rows
               if r["territorio_codigo"] == "ES70"
               and r["tipo_periodo"] == "anual"
               and int(r["year"]) >= 2020]
    muestra.sort(key=lambda r: r["year"])
    print("\nES70 anual — últimos años (terminadas / iniciadas):")
    for r in muestra:
        print(f"  {r['year']}  terminadas={r['viviendas_terminadas']:>6}  "
              f"iniciadas={r['viviendas_iniciadas']:>6}")


if __name__ == "__main__":
    main()
