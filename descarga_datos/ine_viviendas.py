#!/usr/bin/env python3
"""
ine_viviendas.py
Descarga la Estadística de Viviendas por municipios de Canarias del INE
(tabla 59531) y la guarda como CSV en ./tmp/ine_viviendas_YYYYMMDD.csv

La tabla 59531 es un snapshot (sin dimensión temporal): un único valor por
municipio para cada medida. Basada en el Censo de Población y Viviendas 2021.

Medidas extraídas:
  - Viviendas totales
  - Viviendas vacías
  - Viviendas de uso esporádico
  (habituales = total - vacías - esporádicas, calculado en el script R)

Estrategia:
  1. DATOS_TABLA/59531 → todas las series (~58k, toda España)
  2. Filtra códigos INE de Canarias (35xxx, 38xxx, 5 dígitos)
  3. Guarda los 3 indicadores por municipio

Sin dependencias externas.

Uso:
    python3 ine_viviendas.py
"""

import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

DATOS_URL = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/59531"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

MEDIDAS = {
    "Viviendas totales":          "total",
    "Viviendas vacías":           "vacias",
    "Viviendas de uso esporádico": "esporadicas",
}


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


def build_csv(datos: list) -> list[dict]:
    """
    Extrae los 3 indicadores para los 88 municipios canarios.
    Formato del nombre: 'NNNNN nombre_municipio, medida'
    El código INE de 5 dígitos empieza por 35 (Las Palmas) o 38 (SCT).
    """
    acum: dict[str, dict] = {}

    for serie in datos:
        nombre = serie.get("Nombre", "")
        partes = nombre.split(", ", 1)
        if len(partes) < 2:
            continue

        medida = partes[1]
        if medida not in MEDIDAS:
            continue

        codigo_nombre = partes[0]
        code = codigo_nombre[:5]
        if not (code.startswith(("35", "38")) and code[2:].isdigit()):
            continue

        data_pts = serie.get("Data", [])
        if not data_pts or data_pts[0].get("Secreto", False):
            continue

        valor = data_pts[0].get("Valor")
        if valor is None:
            continue

        if code not in acum:
            nombre_mun = codigo_nombre[6:].strip().rstrip(" *")
            acum[code] = {"codigo_ine": code, "nombre": nombre_mun}
        acum[code][MEDIDAS[medida]] = int(valor)

    rows = list(acum.values())

    # Verificar que los 3 indicadores están presentes en cada municipio
    incompletos = [r for r in rows if any(k not in r for k in ("total", "vacias", "esporadicas"))]
    if incompletos:
        import sys
        print(f"  ADVERTENCIA — municipios con datos incompletos: {len(incompletos)}",
              file=sys.stderr)
        for r in incompletos:
            print(f"    {r['codigo_ine']} {r['nombre']}: {list(r.keys())}", file=sys.stderr)

    return sorted(rows, key=lambda r: r["codigo_ine"])


def main():
    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"ine_viviendas_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando tabla INE 59531 (toda España, ~58k series)...", flush=True)
    datos = fetch_json(DATOS_URL)
    print(f"  Series recibidas: {len(datos):,}")

    print("Construyendo CSV...")
    rows = build_csv(datos)
    print(f"  Municipios Canarias: {len(rows)}")

    fieldnames = ["codigo_ine", "nombre", "total", "vacias", "esporadicas"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows)} filas → {csv_path} ({kb:.1f} KB)")

    # Muestra municipios grandes
    top = sorted(rows, key=lambda r: -r["total"])[:6]
    print(f"\nTop 6 por viviendas totales:")
    print(f"  {'CÓDIGO':<6} {'MUNICIPIO':<35} {'TOTAL':>8} {'VACÍAS':>8} {'ESPORÁD.':>9}")
    print(f"  {'-'*6} {'-'*35} {'-'*8} {'-'*8} {'-'*9}")
    for r in top:
        print(f"  {r['codigo_ine']:<6} {r['nombre'][:35]:<35} "
              f"{r['total']:>8,} {r['vacias']:>8,} {r['esporadicas']:>9,}")


if __name__ == "__main__":
    main()
