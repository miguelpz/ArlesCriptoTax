import pandas as pd
from decimal import Decimal


# ============================
#   UTILIDADES BÁSICAS
# ============================

def extraer_anio(utc_time_str: str) -> int:
    """Extrae el año de la columna UTC_Time."""
    return int(str(utc_time_str)[:4])


def obtener_comision_eur(row) -> Decimal:
    """Devuelve la comisión en EUR si existe, si no 0."""
    if row.get("Comision_Valor_EUR") not in (None, "", 0):
        return Decimal(str(row["Comision_Valor_EUR"]))
    return Decimal("0")


def es_compra(row) -> bool:
    return row.get("Tipo") == "COMPRA"


def es_permuta_o_venta(row) -> bool:
    return row.get("Tipo") in ("PERMUTA","VENTA")


# ============================
#   CÁLCULO DE TOTALES
# ============================

def calcular_totales_base_ahorro(df: pd.DataFrame) -> dict:
    """
    Calcula por año y moneda recibida:
      - nº de operaciones
      - valor total de transmisión
      - valor total de adquisición
    siguiendo los criterios fiscales definidos por Arles.
    """

    # Filtrar solo filas declarables
    df = df[df["Declarable"] == "S"]

    totales = {}

    for _, row in df.iterrows():

        moneda = row.get("Emitido_Moneda")
        cantidad = row.get("Emitido_Cantidad")

        # Solo filas donde realmente se recibe una moneda
        if moneda in (None, "", 0) or cantidad in (None, 0, "0"):
            continue

        anio = extraer_anio(row.get("UTC_Time"))
        key = (anio, moneda)

        if key not in totales:
            totales[key] = {
                "n_ops_recibida": 0,
                "transmision": Decimal("0"),
                "adquisicion": Decimal("0"),
            }

        # Contar operación
        totales[key]["n_ops_recibida"] += 1

        # ============================
        #   TRANSMISIÓN
        # ============================
        # Valor de transmisión = Valor Transmision
        valor_tr = row.get("Valor Transmision")
        if valor_tr not in (None, ""):
            totales[key]["transmision"] += Decimal(str(valor_tr))            
        # ============================
        #   ADQUISICIÓN
        # ============================
        comision_eur = obtener_comision_eur(row)
        valor_adq = row.get("Valor Adquisicion")
        valor_adq_dec = Decimal(str(valor_adq)) if valor_adq not in (None, "") else Decimal("0")
        print(f"Un valor sumado a comision {moneda} cantidad {cantidad} y comision {str(comision_eur)}")
        # A) PERMUTAS → adquisición = Valor Adquisicion + comisión EUR
        if es_permuta_o_venta(row):
            totales[key]["adquisicion"] += valor_adq_dec + comision_eur

        # B) COMPRAS → adquisición = comisión EUR
        if es_compra(row) and comision_eur != Decimal("0"):
            totales[key]["adquisicion"] += comision_eur

    return totales


# ============================
#   GENERACIÓN DEL INFORME TXT
# ============================

def generar_informe_txt_base_ahorro(totales: dict) -> str:
    """
    Genera un informe en texto plano, explicando cada concepto.
    """

    lineas = []
    claves_ordenadas = sorted(totales.keys(), key=lambda x: (x[0], x[1]))

    anio_actual = None

    for (anio, moneda) in claves_ordenadas:
        datos = totales[(anio, moneda)]
        n_ops = datos["n_ops_recibida"]
        tr = datos["transmision"]
        adq = datos["adquisicion"]

        if anio != anio_actual:
            if anio_actual is not None:
                lineas.append("")
            lineas.append(f"===== AÑO {anio} =====")
            lineas.append("")
            anio_actual = anio

        lineas.append(f"Moneda recibida: {moneda}")
        lineas.append(f"  Número de operaciones en las que se recibe: {n_ops}")
        lineas.append("")
        lineas.append("  Valor total de TRANSMISIÓN (EUR):")
        lineas.append("    = sumatorio de 'Valor Transmision' en filas declarables donde se recibe esta moneda.")
        lineas.append(f"    => {tr}")
        lineas.append("")
        lineas.append("  Valor total de ADQUISICIÓN (EUR):")
        lineas.append("    = PERMUTAS: Valor Adquisicion + Comision_Valor_EUR")
        lineas.append("    + COMPRAS: solo Comision_Valor_EUR")
        lineas.append(f"    => {adq}")
        gp = tr - adq
        lineas.append("  Ganancia/Pérdida patrimonial (EUR):")
        lineas.append("    = Transmisión - Adquisición")
        lineas.append(f"    => {gp}")

        lineas.append("")

    return "\n".join(lineas)


# ============================
#   ORQUESTADOR PRINCIPAL
# ============================

def generar_informe_fiscal_base_ahorro_txt(df: pd.DataFrame) -> str:
    """
    Orquesta el cálculo y devuelve el informe final en texto plano.
    """
    totales = calcular_totales_base_ahorro(df)
    informe = generar_informe_txt_base_ahorro(totales)
    return informe