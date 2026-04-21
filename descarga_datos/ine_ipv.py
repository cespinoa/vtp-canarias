#!/usr/bin/env python3
"""
ine_ipv.py
Descarga el Índice de Precios de la Vivienda (IPV, base 2015) del INE
(tabla 25171 — trimestral por CCAA) y lo guarda en ./tmp/ipv_YYYYMMDD.csv

Territorios almacenados: Nacional (00) y Canarias (05).
Tipos de vivienda: general, nueva, segunda_mano.
Medidas: indice, variacion_anual, variacion_trimestral.
Cobertura: Q4 2007 – trimestre más reciente publicado.

Sin dependencias externas.

Uso:
    python3 ine_ipv.py
    python3 ine_ipv.py --raw    # guarda también el JSON crudo
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

DATOS_URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/25171?tip=A"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path(__file__).parent / "tmp"

# Territorios de interés: código INE de 2 dígitos → nombre normalizado
TERRITORIOS = {
    "00": "nacional",
    "05": "canarias",
}

# Mapeo de fragmentos del nombre de serie → (tipo_vivienda, medida)
# El nombre tiene forma: "{Territorio}. {Tipo}. {Medida}."
TIPO_MAP = {
    "General":               "general",
    "Vivienda nueva":        "nueva",
    "Vivienda segunda mano": "segunda_mano",
}
MEDIDA_MAP = {
    "Índice":                           "indice",
    "Variación anual":                  "variacion_anual",
    "Variación trimestral":             "variacion_trimestral",
    "Variación en lo que va de año":    None,   # descartada
}

# Trimestre INE (T3_Periodo) → entero
TRIMESTRE_MAP = {"T1": 1, "T2": 2, "T3": 3, "T4": 4}


def fetch_json(url: str, retries: int = 3) -> object:
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


def parse_nombre(nombre: str):
    """
    Extrae (territorio_codigo, tipo_vivienda, medida) del nombre de la serie.
    Devuelve None si no es una serie de interés.
    Formato: "Territorio. Tipo. Medida."
    """
    partes = [p.strip().rstrip(".") for p in nombre.split(".")]
    if len(partes) < 3:
        return None

    territorio_nombre, tipo_raw, medida_raw = partes[0], partes[1], partes[2]

    # Identificar territorio por código
    territorio_codigo = None
    for cod, nom in TERRITORIOS.items():
        if nom == "nacional" and territorio_nombre == "Nacional":
            territorio_codigo = cod
            break
        if nom == "canarias" and territorio_nombre == "Canarias":
            territorio_codigo = cod
            break
    if territorio_codigo is None:
        return None

    tipo = TIPO_MAP.get(tipo_raw)
    if tipo is None:
        return None

    medida = MEDIDA_MAP.get(medida_raw)
    if medida is None:  # incluye las descartadas con valor None
        return None

    return territorio_codigo, tipo, medida


def build_rows(series: list) -> list[dict]:
    rows = []
    for serie in series:
        nombre = serie.get("Nombre", "")
        parsed = parse_nombre(nombre)
        if parsed is None:
            continue

        territorio_codigo, tipo_vivienda, medida = parsed

        for punto in serie.get("Data", []):
            valor = punto.get("Valor")
            if valor is None:
                continue
            trimestre = TRIMESTRE_MAP.get(punto.get("T3_Periodo"))
            if trimestre is None:
                continue
            rows.append({
                "territorio_codigo": territorio_codigo,
                "anyo":              punto["Anyo"],
                "trimestre":         trimestre,
                "tipo_vivienda":     tipo_vivienda,
                "medida":            medida,
                "valor":             valor,
            })

    return rows


def pivot_rows(rows: list[dict]) -> list[dict]:
    """
    Convierte de formato largo (una fila por medida) a ancho
    (una fila por territorio/anyo/trimestre/tipo con columnas indice,
    variacion_anual, variacion_trimestral).
    """
    acum: dict[tuple, dict] = {}
    for r in rows:
        key = (r["territorio_codigo"], r["anyo"], r["trimestre"], r["tipo_vivienda"])
        if key not in acum:
            acum[key] = {
                "territorio_codigo":   r["territorio_codigo"],
                "anyo":                r["anyo"],
                "trimestre":           r["trimestre"],
                "tipo_vivienda":       r["tipo_vivienda"],
                "indice":              None,
                "variacion_anual":     None,
                "variacion_trimestral": None,
            }
        acum[key][r["medida"]] = r["valor"]

    return sorted(acum.values(),
                  key=lambda r: (r["territorio_codigo"], r["anyo"], r["trimestre"], r["tipo_vivienda"]))


def main():
    parser = argparse.ArgumentParser(
        description="Descarga IPV base 2015 del INE (tabla 25171)"
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="Guarda también el JSON crudo en tmp/"
    )
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"ipv_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando tabla 25171 (IPV por CCAA)...", flush=True)
    series = fetch_json(DATOS_URL)
    print(f"  Series recibidas: {len(series):,}")

    if args.raw:
        raw_path = TMP_DIR / f"ipv_raw_{date_str}.json"
        raw_path.write_text(json.dumps(series, ensure_ascii=False, indent=2))
        print(f"  JSON crudo → {raw_path}")

    print("Construyendo CSV...", flush=True)
    rows_largo = build_rows(series)
    rows = pivot_rows(rows_largo)

    if not rows:
        print("ERROR: no se extrajeron filas. Revisa el mapeo de nombres.", file=sys.stderr)
        sys.exit(1)

    anyos = sorted({r["anyo"] for r in rows})
    territorios = sorted({r["territorio_codigo"] for r in rows})
    print(f"  Territorios: {territorios}")
    print(f"  Años: {anyos[0]}–{anyos[-1]}")
    print(f"  Total filas: {len(rows):,}")

    fieldnames = [
        "territorio_codigo", "anyo", "trimestre", "tipo_vivienda",
        "indice", "variacion_anual", "variacion_trimestral",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:,.1f} KB)")

    # Muestra: últimos datos de Canarias
    anyo_max = max(r["anyo"] for r in rows)
    trim_max = max(r["trimestre"] for r in rows if r["anyo"] == anyo_max)
    muestra = [r for r in rows
               if r["territorio_codigo"] == "05"
               and r["anyo"] == anyo_max
               and r["trimestre"] == trim_max]
    print(f"\nCanarias T{trim_max} {anyo_max}:")
    print(f"  {'TIPO':<15} {'ÍNDICE':>8} {'VAR.ANUAL':>10} {'VAR.TRIM.':>10}")
    print(f"  {'-'*15} {'-'*8} {'-'*10} {'-'*10}")
    for r in sorted(muestra, key=lambda x: x["tipo_vivienda"]):
        print(f"  {r['tipo_vivienda']:<15} {r['indice'] or '':>8} "
              f"{r['variacion_anual'] or '':>10} {r['variacion_trimestral'] or '':>10}")


if __name__ == "__main__":
    main()
