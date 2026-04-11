#!/usr/bin/env python3
"""
ine_tamano_hogar_ccaa.py
Descarga la tabla INE 60132 (Encuesta Continua de Hogares — tamaño medio del hogar
por CCAA y trimestre) y guarda un CSV en ./tmp/tamano_hogar_ccaa_YYYYMMDD.csv

Cobertura: Q1 2021 – trimestre más reciente publicado.
Territorios: Total Nacional + 19 comunidades y ciudades autónomas.
FK_Periodo: 19=T1, 20=T2, 21=T3, 22=T4.

Sin dependencias externas.

Uso:
    python3 ine_tamano_hogar_ccaa.py
    python3 ine_tamano_hogar_ccaa.py --raw
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

DATOS_URL = (
    "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/60132?nult=999"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

# FK_Periodo → número de trimestre
PERIODO_TRIMESTRE = {19: 1, 20: 2, 21: 3, 22: 4}

# Mapa nombre INE → código CCAA (2 dígitos, "00" = total nacional)
CCAA_CODIGOS = {
    "Total Nacional":               "00",
    "Andalucía":                    "01",
    "Aragón":                       "02",
    "Asturias, Principado de":      "03",
    "Balears, Illes":               "04",
    "Canarias":                     "05",
    "Cantabria":                    "06",
    "Castilla y León":              "07",
    "Castilla - La Mancha":         "08",
    "Cataluña":                     "09",
    "Comunitat Valenciana":         "10",
    "Extremadura":                  "11",
    "Galicia":                      "12",
    "Madrid, Comunidad de":         "13",
    "Murcia, Región de":            "14",
    "Navarra, Comunidad Foral de":  "15",
    "País Vasco":                   "16",
    "Rioja, La":                    "17",
    "Ceuta":                        "18",
    "Melilla":                      "19",
}


def fetch_json(url: str) -> list:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def extract_nombre(nombre_serie: str) -> str:
    """Extrae el nombre del territorio del campo Nombre de la serie."""
    return nombre_serie.split(". Tamaño")[0].strip()


def build_csv(data: list) -> list[dict]:
    rows = []
    sin_codigo = []

    for serie in data:
        nombre_territorio = extract_nombre(serie["Nombre"])
        ccaa_cod = CCAA_CODIGOS.get(nombre_territorio)
        if ccaa_cod is None:
            sin_codigo.append(nombre_territorio)
            continue

        for obs in serie["Data"]:
            trimestre = PERIODO_TRIMESTRE.get(obs["FK_Periodo"])
            if trimestre is None:
                continue
            if obs["Valor"] is None or obs.get("Secreto", False):
                continue
            rows.append({
                "ccaa_cod":    ccaa_cod,
                "ccaa_nombre": nombre_territorio,
                "anyo":        obs["Anyo"],
                "trimestre":   trimestre,
                "miembros":    round(float(obs["Valor"]), 2),
            })

    if sin_codigo:
        print(f"  ADVERTENCIA — territorios sin código: {sin_codigo}")

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga tamaño medio del hogar por CCAA (INE tabla 60132)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el JSON crudo en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"tamano_hogar_ccaa_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando tabla INE 60132 (tamaño medio del hogar por CCAA)...")
    data = fetch_json(DATOS_URL)
    print(f"  Series recibidas: {len(data)}")

    if args.raw:
        raw_path = TMP_DIR / f"tamano_hogar_ccaa_raw_{date_str}.json"
        raw_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        print(f"  JSON crudo → {raw_path}")

    rows = build_csv(data)

    anyos  = sorted({r["anyo"]      for r in rows})

    print(f"  Cobertura: {anyos[0]}-T{min(r['trimestre'] for r in rows if r['anyo']==anyos[0])}"
          f" → {anyos[-1]}-T{max(r['trimestre'] for r in rows if r['anyo']==anyos[-1])}")

    fieldnames = ["ccaa_cod", "ccaa_nombre", "anyo", "trimestre", "miembros"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["ccaa_cod"], r["anyo"], r["trimestre"])))

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:.1f} KB)")

    # Resumen: último trimestre disponible
    anyo_max = anyos[-1]
    trim_max = max(r["trimestre"] for r in rows if r["anyo"] == anyo_max)
    print(f"\nValores {anyo_max}-T{trim_max}:")
    ultimo = [r for r in rows if r["anyo"] == anyo_max and r["trimestre"] == trim_max]
    for r in sorted(ultimo, key=lambda x: x["ccaa_cod"]):
        print(f"  [{r['ccaa_cod']}] {r['ccaa_nombre']:<35} {r['miembros']:.2f}")


if __name__ == "__main__":
    main()
