#!/usr/bin/env python3
"""
ine_poblacion.py
Descarga la población municipal de Canarias del INE (Padrón Municipal,
tabla 29005) y la guarda como CSV en ./tmp/ine_poblacion_YYYYMMDD.csv

Ventaja frente al ISTAC: el INE publica el dato del año en curso antes
de que el ISTAC lo incorpore (ej. datos 2025 disponibles desde enero 2026).
Cobertura: 1996–año actual (vs ISTAC que cubre 1986–año anterior).

Estrategia de descarga:
  1. VALORES_VARIABLE/19 → catálogo de municipios con código INE (5 dígitos)
     Filtra los de Canarias (código empieza por 35 o 38).
     Descarta las dos entradas especiales "Población en municipios
     desaparecidos de..." que no corresponden a municipios reales.
  2. DATOS_TABLA/29005 → datos de toda España (24.414 series, ~5 MB)
     Filtra las series "Total" cuyos nombres coinciden con los 88
     municipios canarios del catálogo.

Sin dependencias externas.

Uso:
    python3 ine_poblacion.py
    python3 ine_poblacion.py --raw    # guarda también el JSON crudo
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

CATALOGO_URL = (
    "https://servicios.ine.es/wstempus/js/ES/VALORES_VARIABLE/19"
)
DATOS_URL = (
    "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/29005"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "application/json",
}

TMP_DIR = Path("./tmp")

# Entradas del catálogo que no son municipios reales
NOMBRES_ESPECIALES = {
    "Población en municipios desaparecidos de Palmas (Las)",
    "Población en municipios desaparecidos de Santa Cruz de Tenerife",
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


def get_canarias_catalog() -> dict:
    """
    Devuelve {nombre_ine: codigo_ine} para los municipios de Canarias.
    Los códigos INE de Canarias empiezan por 35 (Las Palmas) o 38 (SCT).
    """
    print("  Descargando catálogo de municipios...", flush=True)
    municipios = fetch_json(CATALOGO_URL)
    catalogo = {
        m["Nombre"]: m["Codigo"]
        for m in municipios
        if str(m.get("Codigo", "")).startswith(("35", "38"))
        and m["Nombre"] not in NOMBRES_ESPECIALES
    }
    print(f"  Municipios Canarias en catálogo: {len(catalogo)}")
    return catalogo


def build_csv(datos: list, catalogo: dict) -> list[dict]:
    """
    Filtra las series de Canarias y construye la lista de filas con:
        codigo_ine, nombre, anyo, poblacion

    Puede haber dos series con el mismo nombre de municipio si existe otro
    municipio homónimo en España (caso conocido: Moya, 35013). En ese caso
    se conserva el valor mayor por (codigo_ine, anyo), que corresponde al
    municipio canario real.
    """
    # Acumular por (codigo_ine, anyo) conservando el máximo
    acum: dict[tuple, dict] = {}
    no_encontrados = set(catalogo.keys())

    for serie in datos:
        nombre_serie = serie.get("Nombre", "")
        if ". Total." not in nombre_serie:
            continue

        nombre_mun = nombre_serie.split(". Total.")[0]
        if nombre_mun not in catalogo:
            continue

        codigo_ine = catalogo[nombre_mun]
        no_encontrados.discard(nombre_mun)

        for punto in serie["Data"]:
            val = punto.get("Valor")
            secreto = punto.get("Secreto", False)
            if secreto or val is None:
                continue
            key = (codigo_ine, punto["Anyo"])
            pob = int(val)
            if key not in acum or pob > acum[key]["poblacion"]:
                acum[key] = {
                    "codigo_ine": codigo_ine,
                    "nombre":     nombre_mun,
                    "anyo":       punto["Anyo"],
                    "poblacion":  pob,
                }

    rows = list(acum.values())

    if no_encontrados:
        print(f"  ADVERTENCIA — municipios del catálogo sin datos: {no_encontrados}",
              file=sys.stderr)

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga población municipal de Canarias del INE (t=29005)"
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="Guarda también el JSON crudo de DATOS_TABLA en tmp/"
    )
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"ine_poblacion_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    # Paso 1: catálogo
    catalogo = get_canarias_catalog()

    # Paso 2: datos
    print("  Descargando tabla 29005 (toda España, ~5 MB)...", flush=True)
    datos = fetch_json(DATOS_URL)
    print(f"  Series recibidas: {len(datos):,}")

    if args.raw:
        raw_path = TMP_DIR / f"ine_poblacion_raw_{date_str}.json"
        raw_path.write_text(json.dumps(datos, ensure_ascii=False, indent=2))
        print(f"  JSON crudo  → {raw_path}")

    # Paso 3: filtrar y construir CSV
    print("Construyendo CSV...")
    rows = build_csv(datos, catalogo)

    anyos = sorted({r["anyo"] for r in rows})
    print(f"  Municipios con datos: {len({r['codigo_ine'] for r in rows})}")
    print(f"  Años: {anyos[0]}–{anyos[-1]}")
    print(f"  Total filas: {len(rows):,}")

    fieldnames = ["codigo_ine", "nombre", "anyo", "poblacion"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows):,} filas → {csv_path} ({kb:,.1f} KB)")

    # Muestra: valores más recientes para municipios grandes
    anyo_max = max(r["anyo"] for r in rows)
    muestra = sorted(
        [r for r in rows if r["anyo"] == anyo_max],
        key=lambda r: -r["poblacion"]
    )[:6]
    print(f"\nTop 6 por población ({anyo_max}):")
    print(f"  {'CODIGO':<8} {'NOMBRE':<40} {'POBLACIÓN':>12}")
    print(f"  {'-'*8} {'-'*40} {'-'*12}")
    for r in muestra:
        print(f"  {r['codigo_ine']:<8} {r['nombre'][:40]:<40} {r['poblacion']:>12,}")


if __name__ == "__main__":
    main()
