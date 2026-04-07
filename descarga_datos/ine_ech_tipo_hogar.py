#!/usr/bin/env python3
"""
ine_ech_tipo_hogar.py
Descarga la serie anual 2013–2020 de hogares según tipo de hogar para Canarias
de la Encuesta Continua de Hogares (ECH) del INE, operación 274.

Fuente: tabla 02001 "Hogares según nacionalidad y tipo de hogar"
  Nivel: Comunidades Autónomas (p02)
  URL: https://www.ine.es/jaxi/files/_px/es/csv_bdsc/t20/p274/serie/prov/p02/l0/02001.csv_bdsc

Filtro aplicado: Canarias + Nacionalidad == "Total"
Unidades: miles de hogares (tal como publica el INE)

Categorías disponibles (excluye el agregado "Total (tipo de hogar)"):
  Hogar unipersonal
  Hogar monoparental
  Pareja sin hijos que convivan en el hogar
  Pareja con hijos que convivan en el hogar: Total
  Pareja con hijos que convivan en el hogar: 1 hijo
  Pareja con hijos que convivan en el hogar: 2 hijos
  Pareja con hijos que convivan en el hogar: 3 o más hijos
  Núcleo familiar con otras personas que no forman núcleo familiar
  Personas que no forman ningún núcleo familiar entre sí
  Dos o más núcleos familiares

Salida: tmp/ine_ech_tipo_hogar_YYYYMMDD.csv
  anyo | tipo_hogar | hogares_miles

Sin dependencias externas.

Uso:
    python3 ine_ech_tipo_hogar.py
    python3 ine_ech_tipo_hogar.py --raw   # guarda también el CSV crudo del INE
"""

import argparse
import csv
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

URL = ("https://www.ine.es/jaxi/files/_px/es/csv_bdsc/t20/p274"
       "/serie/prov/p02/l0/02001.csv_bdsc")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ine-download/1.0)",
    "Accept":     "*/*",
}

TMP_DIR = Path("./tmp")

TIPO_EXCLUIR = {"Total (tipo de hogar)"}


def descargar(url: str) -> bytes:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        chunks = []
        while True:
            chunk = resp.read(1024 * 64)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks)


def parsear_valor(s: str) -> float | None:
    """Convierte '1.234,5' → 1234.5. Devuelve None si no es numérico."""
    s = s.strip()
    if not s or s == ".":
        return None
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Descarga ECH 2013-2020 hogares por tipo para Canarias"
    )
    parser.add_argument("--raw", action="store_true",
                        help="Guarda también el CSV crudo del INE en tmp/")
    args = parser.parse_args()

    TMP_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d")
    csv_path = TMP_DIR / f"ine_ech_tipo_hogar_{date_str}.csv"

    if csv_path.exists():
        print(f"Ya existe: {csv_path} — omitiendo.")
        sys.exit(0)

    print("Descargando ECH tabla 02001 (hogares por tipo, CCAA)...", flush=True)
    raw = descargar(URL)
    print(f"  Descargado: {len(raw):,} bytes")

    if args.raw:
        p = TMP_DIR / f"ine_ech_tipo_hogar_raw_{date_str}.csv"
        p.write_bytes(raw)
        print(f"  CSV crudo → {p}")

    # Decodificar: el INE usa UTF-8 con BOM
    texto = raw.decode("utf-8-sig")
    lineas = texto.splitlines()

    rows = []
    sin_valor = 0

    reader = csv.reader(lineas, delimiter=";")
    cabecera = next(reader)  # saltar cabecera

    for fila in reader:
        if len(fila) < 6:
            continue

        # Columnas: Total Nacional ; CCAA ; Tipo de hogar ; Nacionalidad ; periodo ; Total
        ccaa        = fila[1].strip()
        tipo_hogar  = fila[2].strip()
        nacionalidad = fila[3].strip()
        periodo     = fila[4].strip()
        valor_str   = fila[5].strip()

        if ccaa != "Canarias":
            continue
        if nacionalidad != "Total":
            continue
        if tipo_hogar in TIPO_EXCLUIR:
            continue

        valor = parsear_valor(valor_str)
        if valor is None:
            sin_valor += 1
            continue

        try:
            anyo = int(periodo)
        except ValueError:
            continue

        rows.append({
            "anyo":         anyo,
            "tipo_hogar":   tipo_hogar,
            "hogares_miles": valor,
        })

    if sin_valor:
        print(f"  AVISO: {sin_valor} filas sin valor numérico omitidas.",
              file=sys.stderr)

    if not rows:
        print("ERROR: no se extrajeron filas. Ejecuta con --raw para inspeccionar.",
              file=sys.stderr)
        sys.exit(1)

    rows.sort(key=lambda r: (r["anyo"], r["tipo_hogar"]))

    fieldnames = ["anyo", "tipo_hogar", "hogares_miles"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    kb = csv_path.stat().st_size / 1024
    print(f"  ✓ {len(rows)} filas → {csv_path} ({kb:.1f} KB)")

    # Resumen
    anyos = sorted({r["anyo"] for r in rows})
    tipos = sorted({r["tipo_hogar"] for r in rows})
    print(f"\nAños disponibles: {anyos[0]}–{anyos[-1]}")
    print(f"\nTipos de hogar ({len(tipos)}):")
    for t in tipos:
        print(f"  {t}")

    print(f"\nCanarias — hogares por tipo (miles):")
    print(f"  {'TIPO':<55} " + "  ".join(f"{a}" for a in anyos))
    print(f"  {'-'*55} " + "  ".join("-" * 6 for _ in anyos))
    for t in tipos:
        vals = {r["anyo"]: r["hogares_miles"] for r in rows if r["tipo_hogar"] == t}
        fila_vals = "  ".join(f"{vals.get(a, 0):6.1f}" for a in anyos)
        print(f"  {t:<55} {fila_vals}")


if __name__ == "__main__":
    main()
