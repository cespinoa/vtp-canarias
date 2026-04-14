#!/usr/bin/env python3
"""
istac_estancia_reglada.py
Descarga el dataset C00065A_000039 (Encuesta de Ocupación en Alojamientos
Turísticos — ISTAC) y calcula la estancia media anual por isla y Canarias.

El dataset no incluye un total (_T) de nacionalidades, por lo que la estancia
media total se calcula como:
  ESTANCIA_MEDIA = PERNOCTACIONES_suma / VIAJEROS_ENTRADOS_suma
sumando las 27 nacionalidades disponibles.

Se extraen solo periodos anuales (YYYY) y territorios ES70 + ES703–ES709.

Sin dependencias externas.

Uso:
    python3 istac_estancia_reglada.py
    python3 istac_estancia_reglada.py --raw
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
    "/v1.0/datasets/ISTAC/C00065A_000039/~latest?_type=json"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; istac-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path(__file__).parent / "tmp"

TERRITORIOS_OBJETIVO = {"ES70", "ES703", "ES704", "ES705", "ES706", "ES707", "ES708", "ES709"}


def fetch_json(url: str, retries: int = 3) -> dict:
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=180) as resp:
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


def extract_dim_values(data_dims: list) -> tuple[dict, list[str]]:
    """Devuelve (dict código→índice por dimensión, orden de dimensiones)."""
    result = {}
    order  = []
    for dim in data_dims:
        did   = dim["dimensionId"]
        order.append(did)
        codes = sorted(
            dim["representations"]["representation"],
            key=lambda r: r["index"]
        )
        result[did] = [r["code"] for r in codes]
    return result, order


def build_csv(raw: dict) -> list[dict]:
    """
    Iteración de observaciones según el orden declarado en las dimensiones.
    Para cada (periodo anual, territorio objetivo) suma PERNOCTACIONES y
    VIAJEROS_ENTRADOS a lo largo de todas las nacionalidades y calcula
    ESTANCIA_MEDIA = pernoc_total / viaj_total.
    """
    data_dims  = raw["data"]["dimensions"]["dimension"]
    dim_values, dim_order = extract_dim_values(data_dims)

    print(f"  Orden de dimensiones: {dim_order}")
    sizes = [len(dim_values[d]) for d in dim_order]
    print(f"  Tamaños: {dict(zip(dim_order, sizes))}")

    obs_vals = [v.strip() for v in raw["data"]["observations"].split("|")]
    expected = 1
    for s in sizes:
        expected *= s
    print(f"  Observaciones esperadas: {expected}")
    print(f"  Observaciones recibidas: {len(obs_vals)}")

    # Índices de medidas necesarias
    medidas    = dim_values["MEDIDAS"]
    m_pernoc   = medidas.index("PERNOCTACIONES")
    m_viajeros = medidas.index("VIAJEROS_ENTRADOS")

    # Función de acceso al vector de observaciones (orden dinámico)
    # idx = Σ (coord_i × Π tamaños de dimensiones posteriores)
    def obs_idx(coords: dict) -> int:
        idx = 0
        stride = 1
        for dim in reversed(dim_order):
            c = coords[dim]
            idx += c * stride
            stride *= sizes[dim_order.index(dim)]
        return idx

    def get_val(coords: dict) -> float | None:
        i = obs_idx(coords)
        v = obs_vals[i] if i < len(obs_vals) else ""
        return None if v in ("", ".", "..") else float(v)

    # Determinar qué dimensiones son TERRITORIO y TIME_PERIOD
    territorios = dim_values["TERRITORIO"]
    periodos    = dim_values["TIME_PERIOD"]

    # Identificar la dimensión de segmentación (NACIONALIDAD o RESIDENCIA)
    seg_dim = None
    for candidate in ("NACIONALIDAD", "RESIDENCIA", "ALOJAMIENTO_TURISTICO_TIPO"):
        if candidate in dim_values:
            seg_dim = candidate
            break
    if seg_dim is None:
        raise ValueError(f"No se encontró dimensión de segmentación entre {dim_order}")

    seg_values = dim_values[seg_dim]
    print(f"  Dimensión de segmentación: {seg_dim} ({len(seg_values)} valores)")

    rows = []
    for p_i, periodo in enumerate(periodos):
        # Solo periodos anuales (4 dígitos)
        if len(periodo) != 4:
            continue
        ejercicio = int(periodo)

        for t_i, terr in enumerate(territorios):
            if terr not in TERRITORIOS_OBJETIVO:
                continue

            # Sumar pernoctaciones y viajeros sobre todos los segmentos
            sum_pernoc = 0.0
            sum_viaj   = 0.0
            n_validos  = 0

            for s_i in range(len(seg_values)):
                coords = {
                    "TIME_PERIOD": p_i,
                    "TERRITORIO":  t_i,
                    "MEDIDAS":     m_pernoc,
                    seg_dim:       s_i,
                }
                v_p = get_val(coords)
                coords["MEDIDAS"] = m_viajeros
                v_v = get_val(coords)

                if v_p is not None and v_v is not None:
                    sum_pernoc += v_p
                    sum_viaj   += v_v
                    n_validos  += 1

            if sum_viaj == 0 or n_validos == 0:
                continue

            estancia = round(sum_pernoc / sum_viaj, 2)
            rows.append({
                "territorio_codigo": terr,
                "ejercicio":         ejercicio,
                "estancia_media":    estancia,
            })

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga estancia media anual reglada del ISTAC (C00065A_000039)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"estancia_reglada_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando dataset ISTAC C00065A_000039...")
    raw = fetch_json(DATASET_URL)

    version     = raw.get("datasetMetadata", {}).get("version", "")
    last_update = raw.get("datasetMetadata", {}).get("lastUpdate", "")
    print(f"  Versión     : {version}")
    print(f"  Última act. : {last_update}")

    if args.raw:
        raw_path = TMP_DIR / f"estancia_reglada_raw_{date_str}.json"
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    print("Construyendo CSV...")
    rows = build_csv(raw)

    fieldnames = ["territorio_codigo", "ejercicio", "estancia_media"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:.1f} KB)")

    ejercicios = sorted({r["ejercicio"] for r in rows})
    print(f"\nEjercicios: {ejercicios[0]}–{ejercicios[-1]} ({len(ejercicios)} años)")

    print(f"\nEstancia media por territorio en {ejercicios[-1]}:")
    ultimo = sorted([r for r in rows if r["ejercicio"] == ejercicios[-1]],
                    key=lambda r: r["territorio_codigo"])
    for r in ultimo:
        print(f"  {r['territorio_codigo']}: {r['estancia_media']} días")


if __name__ == "__main__":
    main()
