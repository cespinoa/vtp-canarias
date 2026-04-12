#!/usr/bin/env python3
"""
ine_viviendas_no_hab_historico.py
Descarga el fichero comparativo 2001-2011 del Censo de Viviendas (INE)
y extrae las viviendas no principales (no habituales) para los municipios
de Canarias con más de 2.000 habitantes.

Fuente: /t20/e244/viviendas/p07/02mun00.px
Formato PC-Axis (JAXI INE).

Salida: tmp/viviendas_no_hab_YYYYMMDD.csv
  codigo_ine, nombre, no_hab_2011, no_hab_2001

Sin dependencias externas.
"""

import csv
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

PX_URL = (
    "https://www.ine.es/jaxi/files/_px/es/px/t20/e244/viviendas/p07/02mun00.px"
)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)"}
TMP_DIR = Path("./tmp")


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("latin-1")


def parse_values(block: str) -> list[str]:
    return re.findall(r'"([^"]+)"', block)


def parse_px(content: str) -> list[dict]:
    """
    Extrae viviendas no principales 2001 y 2011 para municipios de Canarias.

    Estructura PC-Axis:
      STUB  = "Municipios"          (N_mun)
      HEADING = "Tipo", "Periodo"   (2 tipos × 2 periodos)
    Orden DATA: por cada municipio → [princ_2011, princ_2001, no_princ_2011, no_princ_2001]
    """
    # Extraer dimensiones
    stub_key = re.search(r'STUB="([^"]+)"', content).group(1)
    mun_block = re.search(
        rf'VALUES\("{re.escape(stub_key)}"\)=(.+?);', content, re.DOTALL
    ).group(1)
    tipo_block = re.search(
        r'VALUES\("Tipo de vivienda"\)=(.+?);', content, re.DOTALL
    ).group(1)
    per_block = re.search(
        r'VALUES\("Periodo"\)=(.+?);', content, re.DOTALL
    ).group(1)

    municipios = parse_values(mun_block)
    tipos      = parse_values(tipo_block)
    periodos   = parse_values(per_block)

    n_tipo = len(tipos)
    n_per  = len(periodos)

    # Parsear DATA
    data_raw = content[content.find("DATA=") + 5:]
    data_vals = []
    for v in re.split(r'[\s;]+', data_raw.rstrip()):
        v = v.strip('"\' ')
        if not v or v in (".", ".."):
            data_vals.append(None)
        else:
            try:
                data_vals.append(float(v))
            except ValueError:
                data_vals.append(None)

    # Índices de "no principales" dentro de los n_tipo tipos
    idx_no_princ = next(
        i for i, t in enumerate(tipos) if "no principal" in t.lower()
    )
    # Índices de periodos
    idx_2011 = periodos.index("2011")
    idx_2001 = periodos.index("2001")

    rows = []
    for i, mun_raw in enumerate(municipios):
        # Solo Canarias
        codigo = mun_raw[:5].strip()
        if codigo[:2] not in ("35", "38"):
            continue

        nombre = mun_raw[5:].strip().lstrip()

        base = i * n_tipo * n_per
        v_2011 = data_vals[base + idx_no_princ * n_per + idx_2011]
        v_2001 = data_vals[base + idx_no_princ * n_per + idx_2001]

        rows.append({
            "codigo_ine": codigo,
            "nombre":     nombre,
            "no_hab_2011": int(round(v_2011)) if v_2011 is not None else "",
            "no_hab_2001": int(round(v_2001)) if v_2001 is not None else "",
        })

    return rows


def main():
    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"viviendas_no_hab_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando 02mun00.px (Censo viviendas 2001-2011)...")
    content = fetch(PX_URL)
    print(f"  {len(content):,} bytes descargados.")

    rows = parse_px(content)
    print(f"  Municipios Canarias extraídos: {len(rows)}")

    fieldnames = ["codigo_ine", "nombre", "no_hab_2011", "no_hab_2001"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: r["codigo_ine"]))

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows)} filas → {csv_path} ({kb:.1f} KB)")

    # Verificación: total Canarias
    total_2011 = sum(r["no_hab_2011"] for r in rows if r["no_hab_2011"] != "")
    total_2001 = sum(r["no_hab_2001"] for r in rows if r["no_hab_2001"] != "")
    print(f"\nTotal no habituales Canarias (municipios >2.000 hab):")
    print(f"  2001: {total_2001:,}  (referencia: ~117.617 total)")
    print(f"  2011: {total_2011:,}  (referencia: ~138.252 total)")


if __name__ == "__main__":
    main()
