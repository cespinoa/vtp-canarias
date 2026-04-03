#!/usr/bin/env python3
"""
istac_pte_vv.py
Descarga el dataset C00065A_000061 (Estadística de Vivienda Vacacional — ISTAC)
y lo guarda como CSV en ./tmp/pte_vv_YYYYMMDD.csv

El cubo tiene 4 dimensiones:
  TERRITORIO        → ES70 (Canarias), ES703–ES709 (islas), municipios, _U (desconocido)
  TIME_PERIOD       → mensual, desde 2019-M01
  INTERVALOS_PLAZAS → un solo valor (_T, total)
  MEDIDAS           → 6 indicadores de ocupación VV

Se extraen los 6 indicadores para todos los territorios clasificables
(canarias, isla, municipio). Se descarta _U.

La PTEv se calcula en el script R de importación, no aquí.

Sin dependencias externas.

Uso:
    python3 istac_pte_vv.py
    python3 istac_pte_vv.py --raw
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
    "/v1.0/datasets/ISTAC/C00065A_000061/~latest?_type=json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

# Códigos NUTS de islas canarias
ISLA_CODES = {"ES703", "ES704", "ES705", "ES706", "ES707", "ES708", "ES709"}


def classify_territorio(code: str) -> str | None:
    """
    Devuelve 'canarias', 'isla' o 'municipio' según el código ISTAC.
    Devuelve None para _U (desconocido) u otros códigos no clasificables.
    """
    if code == "ES70":
        return "canarias"
    if code in ISLA_CODES:
        return "isla"
    if code.startswith(("35", "38")) and len(code) == 5:
        return "municipio"
    return None


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
        territorio_codigo, ambito, time_period, year, mes,
        plazas_disponibles, viviendas_disponibles, viviendas_reservadas,
        tasa_vivienda_reservada, estancia_media, ingresos_totales

    Orden de iteración: TERRITORIO × TIME_PERIOD × INTERVALOS_PLAZAS × MEDIDAS
    Se descarta _U y cualquier código no clasificable.
    """
    data_dims  = raw["data"]["dimensions"]["dimension"]
    dim_values = extract_dim_values(data_dims)

    territorios = dim_values["TERRITORIO"]
    periodos    = dim_values["TIME_PERIOD"]
    intervalos  = dim_values["INTERVALOS_PLAZAS"]
    medidas     = dim_values["MEDIDAS"]

    n_t = len(territorios)
    n_p = len(periodos)
    n_i = len(intervalos)
    n_m = len(medidas)

    obs_vals = [v.strip() for v in raw["data"]["observations"].split("|")]

    print(f"  Dimensiones: {n_t} territorios × {n_p} periodos × {n_i} intervalos × {n_m} medidas")
    print(f"  Observaciones esperadas: {n_t * n_p * n_i * n_m}")
    print(f"  Observaciones recibidas: {len(obs_vals)}")

    med_idx = {m: i for i, m in enumerate(medidas)}

    def parse_val(v):
        if v in ("", ".", ".."):
            return None
        try:
            return float(v)
        except ValueError:
            return None

    rows = []
    idx = 0

    for terr in territorios:
        ambito = classify_territorio(terr)
        for periodo in periodos:
            for _intervalo in intervalos:
                vals = {}
                for medida in medidas:
                    v = obs_vals[idx] if idx < len(obs_vals) else ""
                    vals[medida] = parse_val(v)
                    idx += 1

                if ambito is None:
                    continue

                # Saltar si no hay ningún valor útil
                plazas = vals.get("PLAZAS_DISPONIBLES")
                tasa   = vals.get("TASA_VIVIENDA_RESERVADA")
                if plazas is None and tasa is None:
                    continue

                year = int(periodo[:4])
                mes  = int(periodo[6:8])

                rows.append({
                    "territorio_codigo":    terr,
                    "ambito":               ambito,
                    "time_period":          periodo,
                    "year":                 year,
                    "mes":                  mes,
                    "plazas_disponibles":   int(plazas) if plazas is not None else "",
                    "viviendas_disponibles": int(vals.get("VIVIENDAS_VACACIONALES_DISPONIBLES") or 0)
                                             if vals.get("VIVIENDAS_VACACIONALES_DISPONIBLES") is not None else "",
                    "viviendas_reservadas": int(vals.get("VIVIENDAS_VACACIONALES_RESERVADAS") or 0)
                                            if vals.get("VIVIENDAS_VACACIONALES_RESERVADAS") is not None else "",
                    "tasa_vivienda_reservada": tasa if tasa is not None else "",
                    "estancia_media":       vals.get("ESTANCIA_MEDIA_VIVIENDA_VACACIONAL")
                                            if vals.get("ESTANCIA_MEDIA_VIVIENDA_VACACIONAL") is not None else "",
                    "ingresos_totales":     vals.get("INGRESOS_TOTALES")
                                            if vals.get("INGRESOS_TOTALES") is not None else "",
                })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga estadística de VV del ISTAC (C00065A_000061)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"pte_vv_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando dataset ISTAC C00065A_000061...")
    raw = fetch_json(DATASET_URL)

    version     = raw.get("metadata", {}).get("version", "")
    last_update = raw.get("metadata", {}).get("lastUpdate", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"pte_vv_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = [
        "territorio_codigo", "ambito", "time_period", "year", "mes",
        "plazas_disponibles", "viviendas_disponibles", "viviendas_reservadas",
        "tasa_vivienda_reservada", "estancia_media", "ingresos_totales",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:,.1f} KB)")

    # Resumen por ámbito
    from collections import Counter
    por_ambito = Counter(r["ambito"] for r in rows)
    print(f"\nFilas por ámbito: {dict(sorted(por_ambito.items()))}")

    # Muestra: Canarias últimos 3 meses
    canarias = [r for r in rows if r["ambito"] == "canarias"]
    canarias.sort(key=lambda r: (r["year"], r["mes"]))
    print("\nCanarias — últimos 3 períodos:")
    for r in canarias[-3:]:
        print(f"  {r['time_period']}  plazas={r['plazas_disponibles']:>7}  "
              f"tasa={r['tasa_vivienda_reservada']:>6}")


if __name__ == "__main__":
    main()
