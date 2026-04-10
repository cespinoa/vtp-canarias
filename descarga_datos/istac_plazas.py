#!/usr/bin/env python3
"""
istac_plazas.py
Descarga el dataset C00065A_000033 (Encuesta de Ocupación en Alojamientos
Turísticos — ISTAC) y guarda las plazas anuales por isla y Canarias en
./tmp/plazas_YYYYMMDD.csv

Se extrae únicamente: datos anuales (YYYY), MEDIDAS=PLAZAS,
territorios ES70 (Canarias) y ES703–ES709 (7 islas).

Sin dependencias externas.

Uso:
    python3 istac_plazas.py
    python3 istac_plazas.py --raw
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
    "/v1.0/datasets/ISTAC/C00065A_000033/~latest?_type=json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

TERRITORIOS_OBJETIVO = {"ES70", "ES703", "ES704", "ES705", "ES706", "ES707", "ES708", "ES709"}


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
    Orden de iteración: TIME_PERIOD × TERRITORIO × MEDIDAS × ALOJAMIENTO_TURISTICO_CATEGORIA
    Extrae solo periodos anuales (YYYY), MEDIDAS=PLAZAS y TASA_OCUPACION_PLAZA,
    territorios ES70+ES703-ES709.
    """
    data_dims  = raw["data"]["dimensions"]["dimension"]
    dim_values = extract_dim_values(data_dims)

    periodos    = dim_values["TIME_PERIOD"]
    territorios = dim_values["TERRITORIO"]
    medidas     = dim_values["MEDIDAS"]
    cats        = dim_values["ALOJAMIENTO_TURISTICO_CATEGORIA"]

    n_p = len(periodos)
    n_t = len(territorios)
    n_m = len(medidas)
    n_c = len(cats)

    obs_vals = [v.strip() for v in raw["data"]["observations"].split("|")]

    print(f"  Dimensiones: {n_p} periodos × {n_t} territorios × {n_m} medidas × {n_c} categorías")
    print(f"  Observaciones esperadas: {n_p * n_t * n_m * n_c}")
    print(f"  Observaciones recibidas: {len(obs_vals)}")

    m_plazas = medidas.index("PLAZAS")
    m_tasa   = medidas.index("TASA_OCUPACION_PLAZA")

    def get_val(p_i, t_i, m_i):
        idx = p_i * (n_t * n_m * n_c) + t_i * (n_m * n_c) + m_i * n_c + 0
        v = obs_vals[idx] if idx < len(obs_vals) else ""
        return None if v in ("", ".", "..") else v

    rows = []
    for p_i, periodo in enumerate(periodos):
        # Solo periodos anuales (4 dígitos)
        if len(periodo) != 4:
            continue
        ejercicio = int(periodo)

        for t_i, terr in enumerate(territorios):
            if terr not in TERRITORIOS_OBJETIVO:
                continue

            v_plazas = get_val(p_i, t_i, m_plazas)
            v_tasa   = get_val(p_i, t_i, m_tasa)

            if v_plazas is None and v_tasa is None:
                continue

            rows.append({
                "territorio_codigo":  terr,
                "ejercicio":          ejercicio,
                "plazas":             int(float(v_plazas)) if v_plazas is not None else "",
                "tasa_ocupacion_plaza": round(float(v_tasa), 2) if v_tasa is not None else "",
            })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga plazas turísticas anuales del ISTAC (C00065A_000033)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"plazas_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando dataset ISTAC C00065A_000033...")
    raw = fetch_json(DATASET_URL)

    version     = raw.get("metadata", {}).get("version", "")
    last_update = raw.get("metadata", {}).get("lastUpdate", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"plazas_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = ["territorio_codigo", "ejercicio", "plazas", "tasa_ocupacion_plaza"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:.1f} KB)")

    # Resumen: años disponibles y totales recientes
    ejercicios = sorted({r["ejercicio"] for r in rows})
    print(f"\nEjercicios: {ejercicios[0]}–{ejercicios[-1]} ({len(ejercicios)} años)")

    print(f"\nPlazas y tasa de ocupación por territorio en {ejercicios[-1]}:")
    ultimo = sorted([r for r in rows if r["ejercicio"] == ejercicios[-1]],
                    key=lambda r: -(r["plazas"] if r["plazas"] != "" else 0))
    for r in ultimo:
        plazas = f"{r['plazas']:>10,}" if r["plazas"] != "" else f"{'—':>10}"
        tasa   = f"{r['tasa_ocupacion_plaza']:>6}" if r["tasa_ocupacion_plaza"] != "" else f"{'—':>6}"
        print(f"  {r['territorio_codigo']}: {plazas}  tasa={tasa}%")


if __name__ == "__main__":
    main()
