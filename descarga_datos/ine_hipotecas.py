#!/usr/bin/env python3
"""
ine_hipotecas.py
Descarga la Estadística de Hipotecas del INE y produce un CSV con las
variables necesarias para calcular la cuota media hipotecaria mensual.

Fuentes (tres tablas INE):
  24457 → tipo de interés medio (total/fijo/variable), nacional, viviendas
  24458 → plazo medio (años), nacional, viviendas
  13896 → número e importe de hipotecas (miles €), CCAA + nacional, viviendas

Territorios en el CSV: nacional / canarias.
Cobertura temporal: desde enero 2009 (inicio de 24457/24458) hasta el mes
más reciente publicado.

El importe medio por hipoteca (importe_medio_viv) se calcula en este script
como (importe_miles_viv × 1000) / n_hipotecas_viv. El plazo y el tipo de
interés solo existen a nivel nacional y se asignan con valor NULL para las
filas de Canarias; el cálculo de la cuota se realiza en importar_hipotecas.R.

Sin dependencias externas.

Uso:
    python3 ine_hipotecas.py
    python3 ine_hipotecas.py --raw
"""

import argparse
import csv
import json
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

TMP_DIR = Path(__file__).parent / "tmp"

URLS = {
    "tipo_interes": "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/24457?tip=A",
    "plazo":        "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/24458?tip=A",
    "ccaa":         "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/13896?tip=A",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "application/json",
}

MES_MAP = {f"M{i:02d}": i for i in range(1, 13)}


def fetch_json(url: str, retries: int = 3) -> list:
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


def puntos(serie: list) -> dict[tuple, dict]:
    """Convierte la lista Data de una serie en {(anyo, mes): punto}."""
    out = {}
    for p in serie:
        mes = MES_MAP.get(p.get("T3_Periodo"))
        if mes is None or p.get("Valor") is None:
            continue
        out[(p["Anyo"], mes)] = p
    return out


def parse_tipo_interes(series: list) -> dict[tuple, dict]:
    """
    Extrae de la tabla 24457 los tipos de interés para viviendas (nacional).
    Devuelve {(anyo, mes): {total, fijo, variable, tipo_dato}}.
    """
    acum: dict[tuple, dict] = {}
    for serie in series:
        nombre = serie.get("Nombre", "")
        partes = [p.strip().rstrip(".") for p in nombre.split(".")]
        if len(partes) < 6:
            continue
        if partes[0] != "Viviendas" or partes[1] != "Tipo de interés medio":
            continue
        subtipo = partes[5]  # Total / Fijo / Variable
        if subtipo not in ("Total", "Fijo", "Variable"):
            continue

        for (anyo, mes), punto in puntos(serie["Data"]).items():
            key = (anyo, mes)
            if key not in acum:
                acum[key] = {"tipo_interes_total": None, "tipo_interes_fijo": None,
                              "tipo_interes_variable": None, "tipo_dato_interes": None}
            col = {"Total": "tipo_interes_total", "Fijo": "tipo_interes_fijo",
                   "Variable": "tipo_interes_variable"}[subtipo]
            acum[key][col] = punto["Valor"]
            acum[key]["tipo_dato_interes"] = punto.get("T3_TipoDato")
    return acum


def parse_plazo(series: list) -> dict[tuple, float]:
    """
    Extrae de la tabla 24458 el plazo medio en años para viviendas (nacional).
    Devuelve {(anyo, mes): plazo_anios}.
    """
    acum = {}
    for serie in series:
        nombre = serie.get("Nombre", "")
        partes = [p.strip().rstrip(".") for p in nombre.split(".")]
        if len(partes) < 2:
            continue
        if partes[0] != "Viviendas" or partes[1] != "Plazo medio":
            continue
        for (anyo, mes), punto in puntos(serie["Data"]).items():
            acum[(anyo, mes)] = punto["Valor"]
    return acum


def parse_ccaa(series: list) -> dict[tuple, dict]:
    """
    Extrae de la tabla 13896 número e importe de hipotecas de viviendas
    para Nacional y Canarias.
    Devuelve {(territorio, anyo, mes): {n_hipotecas, importe_miles, tipo_dato}}.

    Formato series:
      Nacional: 'Viviendas. Número de hipotecas. Total Nacional. ...'
      CCAA:     'Viviendas. {CCAA}. Número de hipotecas. ...'
    """
    territorios_interes = {
        "Total Nacional": "nacional",
        "Canarias":       "canarias",
    }
    acum: dict[tuple, dict] = {}

    for serie in series:
        nombre = serie.get("Nombre", "")
        partes = [p.strip().rstrip(".") for p in nombre.split(".")]
        if len(partes) < 3 or partes[0] != "Viviendas":
            continue

        # Detectar territorio y variable según posición
        if partes[2] == "Total Nacional":
            # Formato: Viviendas. {variable}. Total Nacional. ...
            territorio_raw = "Total Nacional"
            variable_raw   = partes[1]
        elif partes[1] in territorios_interes:
            # Formato: Viviendas. {CCAA}. {variable}. ...
            territorio_raw = partes[1]
            variable_raw   = partes[2]
        else:
            continue

        territorio = territorios_interes.get(territorio_raw)
        if territorio is None:
            continue

        if variable_raw == "Número de hipotecas":
            campo = "n_hipotecas"
        elif variable_raw == "Importe de hipotecas":
            campo = "importe_miles"
        else:
            continue

        for (anyo, mes), punto in puntos(serie["Data"]).items():
            key = (territorio, anyo, mes)
            if key not in acum:
                acum[key] = {"n_hipotecas": None, "importe_miles": None,
                              "tipo_dato_ccaa": None}
            acum[key][campo] = int(punto["Valor"]) if campo == "n_hipotecas" \
                                else punto["Valor"]
            acum[key]["tipo_dato_ccaa"] = punto.get("T3_TipoDato")

    return acum


def combinar(tipos: dict, plazos: dict, ccaa: dict) -> list[dict]:
    """
    Une las tres fuentes en una lista de filas, una por (territorio, anyo, mes).
    Las filas de Canarias tienen plazo y tipo de interés en NULL.
    """
    rows = []
    for (territorio, anyo, mes), datos in sorted(ccaa.items()):
        n    = datos.get("n_hipotecas")
        imp  = datos.get("importe_miles")
        medio = round(imp * 1000 / n, 0) if (n and imp and n > 0) else None

        tipo_key  = (anyo, mes)
        ti = tipos.get(tipo_key, {})
        pl = plazos.get(tipo_key)

        rows.append({
            "territorio":            territorio,
            "anyo":                  anyo,
            "mes":                   mes,
            "tipo_dato":             datos.get("tipo_dato_ccaa"),
            "n_hipotecas_viv":       n,
            "importe_miles_viv":     imp,
            "importe_medio_viv":     int(medio) if medio is not None else None,
            "plazo_anios":           pl if territorio == "nacional" else None,
            "tipo_interes_total":    ti.get("tipo_interes_total")    if territorio == "nacional" else None,
            "tipo_interes_fijo":     ti.get("tipo_interes_fijo")     if territorio == "nacional" else None,
            "tipo_interes_variable": ti.get("tipo_interes_variable") if territorio == "nacional" else None,
        })
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Descarga Estadística de Hipotecas INE (tablas 24457, 24458, 13896)"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también los JSON crudos en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str  = datetime.now().strftime("%Y%m%d")
    csv_path  = TMP_DIR / f"hipotecas_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    raw_data = {}
    for clave, url in URLS.items():
        print(f"Descargando tabla {url.split('/')[7].split('?')[0]} ({clave})...", flush=True)
        raw_data[clave] = fetch_json(url)
        print(f"  {len(raw_data[clave])} series recibidas")
        if args.raw:
            p = TMP_DIR / f"hipotecas_{clave}_raw_{date_str}.json"
            p.write_text(json.dumps(raw_data[clave], ensure_ascii=False, indent=2))
            print(f"  JSON crudo -> {p}")

    print("\nProcesando...", flush=True)
    tipos  = parse_tipo_interes(raw_data["tipo_interes"])
    plazos = parse_plazo(raw_data["plazo"])
    ccaa   = parse_ccaa(raw_data["ccaa"])
    rows   = combinar(tipos, plazos, ccaa)

    if not rows:
        print("ERROR: no se extrajeron filas.", file=sys.stderr)
        sys.exit(1)

    anyos = sorted({r["anyo"] for r in rows})
    print(f"  Territorios: {sorted({r['territorio'] for r in rows})}")
    print(f"  Cobertura:   {anyos[0]}-M{min(r['mes'] for r in rows if r['anyo']==anyos[0]):02d} "
          f"hasta {anyos[-1]}-M{max(r['mes'] for r in rows if r['anyo']==anyos[-1]):02d}")
    print(f"  Total filas: {len(rows)}")

    fieldnames = [
        "territorio", "anyo", "mes", "tipo_dato",
        "n_hipotecas_viv", "importe_miles_viv", "importe_medio_viv",
        "plazo_anios", "tipo_interes_total", "tipo_interes_fijo", "tipo_interes_variable",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"\n  -> {csv_path} ({kb:.1f} KB)")

    # Muestra del último mes disponible
    anyo_max = max(r["anyo"] for r in rows)
    mes_max  = max(r["mes"] for r in rows if r["anyo"] == anyo_max)
    muestra  = [r for r in rows if r["anyo"] == anyo_max and r["mes"] == mes_max]
    print(f"\nUltimo mes disponible: {anyo_max}-M{mes_max:02d}")
    print(f"  {'TERRITORIO':<12} {'N.HIP':>7} {'IMP.MEDIO':>10} {'PLAZO':>7} {'TI.TOTAL':>9} {'TI.FIJO':>8} {'TI.VAR':>8}")
    print(f"  {'-'*12} {'-'*7} {'-'*10} {'-'*7} {'-'*9} {'-'*8} {'-'*8}")
    for r in sorted(muestra, key=lambda x: x["territorio"]):
        print(f"  {r['territorio']:<12} {r['n_hipotecas_viv'] or '':>7} "
              f"{r['importe_medio_viv'] or '':>10} "
              f"{r['plazo_anios'] or '':>7} "
              f"{r['tipo_interes_total'] or '':>9} "
              f"{r['tipo_interes_fijo'] or '':>8} "
              f"{r['tipo_interes_variable'] or '':>8}")


if __name__ == "__main__":
    main()
