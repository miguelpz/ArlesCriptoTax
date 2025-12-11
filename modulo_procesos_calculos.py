from decimal import Decimal
from pila_fifo import CryptoFIFO
from decimal import Decimal, InvalidOperation
import math

def safe_decimal(value):
    """
    Convierte cualquier valor a Decimal de forma segura.
    Si es None, NaN, vacío o inválido → devuelve Decimal("0").
    """
    if value is None:
        return Decimal("0")

    # pandas NaN
    if isinstance(value, float) and math.isnan(value):
        return Decimal("0")

    # cadenas vacías o basura
    value_str = str(value).strip()
    if value_str == "" or value_str.lower() in ("nan", "none"):
        return Decimal("0")

    try:
        return Decimal(value_str)
    except InvalidOperation:
        return Decimal("0")


def procesar_df_con_fifo(df, fifo: CryptoFIFO):
    """
    Recorre el DataFrame normalizado y aplica FIFO usando la clase CryptoFIFO.
    Añade columnas:
        - Valor Adquisicion
        - Valor Transmision
        - Detalle FIFO
    Solo procesa filas con Declarable == "S".
    """

    # Crear columnas nuevas
    df["Valor Adquisicion"] = Decimal("0")
    df["Valor Transmision"] = Decimal("0")
    df["Detalle FIFO"] = ""

    # Recorrer filas en orden cronológico
    for idx, row in df.iterrows():

        if row["Declarable"] != "S":
            continue

        tipo = row["Tipo"]
        fecha = row["UTC_Time"]

        emitido_moneda = row["Emitido_Moneda"]
        emitido_cantidad = safe_decimal(row["Emitido_Cantidad"])
        emitido_valor = safe_decimal(row["Emitido_Valor_EUR"])

        recibido_moneda = row["Recibido_Moneda"]
        recibido_cantidad = safe_decimal(row["Recibido_Cantidad"])
        recibido_valor = safe_decimal(row["Recibido_Valor_EUR"])

        # ----------------------------------------------------
        # ✅ COMPRA
        # ----------------------------------------------------
        if tipo == "COMPRA":

            if recibido_cantidad > 0:
                precio_unitario = recibido_valor / recibido_cantidad
                fifo.add(
                    fecha=fecha,
                    cripto=recibido_moneda,
                    cantidad=abs(recibido_cantidad),
                    precio_unitario=abs(precio_unitario)
                )

                df.at[idx, "Valor Adquisicion"] = recibido_valor
                df.at[idx, "Valor Transmision"] = Decimal("0")
                df.at[idx, "Detalle FIFO"] = (
                    f"Entrada lote: {recibido_cantidad} {recibido_moneda} "
                    f"a {precio_unitario} EUR/u"
                )

        # ----------------------------------------------------
        # ✅ VENTA
        # ----------------------------------------------------
        elif tipo == "VENTA":

            coste_total, detalle = fifo.consume(
                cripto=emitido_moneda,
                cantidad=abs(emitido_cantidad)
            )
            print("Imprimo debug " + str(detalle))

            df.at[idx, "Valor Adquisicion"] = coste_total
            df.at[idx, "Valor Transmision"] = recibido_valor
            df.at[idx, "Detalle FIFO"] = str(detalle)

        # ----------------------------------------------------
        # ✅ PERMUTA
        # ----------------------------------------------------
        elif tipo == "PERMUTA":

            # 1) Consumir el activo emitido
            coste_total, detalle_salida = fifo.consume(
                cripto=emitido_moneda,
                cantidad=abs(emitido_cantidad)
            )

            print(f"Debug del coste en permuta {emitido_moneda}  y {emitido_cantidad}= " +  str(coste_total))

            # 2) Valor de transmisión = mayor de los dos valores
            valor_transmision = max(emitido_valor, recibido_valor)

            df.at[idx, "Valor Adquisicion"] = coste_total
            df.at[idx, "Valor Transmision"] = valor_transmision

            # 3) Determinar precio unitario del lote que entra
            precio_unitario = valor_transmision / recibido_cantidad

            # 4) Insertar lote del activo recibido
            fifo.add(
                fecha=fecha,
                cripto=recibido_moneda,
                cantidad=abs(recibido_cantidad),
                precio_unitario=abs(precio_unitario)
            )

            # 5) Registrar detalle FIFO completo (salida + entrada)
            detalle_entrada = {
                 f"Entrada lote: {recibido_cantidad} {recibido_moneda} "
                f"a {precio_unitario} EUR/u"
            }

            df.at[idx, "Detalle FIFO"] = str({
                "salida": detalle_salida,
                "entrada": detalle_entrada
            })


    return df