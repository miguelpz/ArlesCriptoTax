# parseador_binance_excel.py
import csv
import pandas as pd
from dataclasses import dataclass
from typing import List, Optional
from collections import defaultdict
from decimal import Decimal, InvalidOperation, getcontext
from datetime import datetime, timezone
from bce_api import convert_stables_in_df, translate_eur_values, convert_no_stables_in_df
from pila_fifo import CryptoFIFO
from modulo_procesos_calculos import procesar_df_con_fifo
from generador_informes import generar_informe_fiscal_base_ahorro_txt



FIAT_CURRENCIES = {"EUR", "USD"}  # Solo estas son fiduciarias

def to_decimal(x: str) -> Decimal:
    try:
        return Decimal(x)
    except InvalidOperation:
        return Decimal(str(float(x)))

def parse_utc(ts: str) -> datetime:
    ts = ts.strip()

    if ts.endswith("Z"):
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    elif ts.endswith(" UTC"):
        # Ejemplo: "2025-09-03 23:19:36 UTC"
        ts = ts[:-4].strip()  # quitar " UTC"
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

    elif "T" in ts:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)

    else:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def remove_negative_signs(df):
    cols = [
        "Emitido_Cantidad",
        "Emitido_Valor_EUR",
        "Comision_Cantidad",
        "Comision_Valor_EUR",
    ]

    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: str(x).lstrip("-") if isinstance(x, str) else x
            )

    return df

       

@dataclass
class RawRow:
    user_id: str
    utc_time: str
    account: str
    op_raw: str
    coin: str
    change: Decimal
    remark: str

@dataclass
class NormalizedRow:
    utc_time: str
    tracker: str                # siempre "binance"
    tipo: str                   # COMPRA / VENTA / PERMUTA / INTERNAL / DEPOSIT / WITHDRAW
    emitido_moneda: str
    emitido_cantidad: Decimal
    emitido_valor_eur: str      # vacío
    recibido_moneda: str
    recibido_cantidad: Decimal
    recibido_valor_eur: str     # vacío
    comision_moneda: Optional[str]
    comision_cantidad: Optional[Decimal]
    comision_valor_eur: str     # vacío
    declarable: str             # "Sí" / "No"

def classify_tipo(emitido_moneda: str, recibido_moneda: str, tipo_defecto: str) -> str:
    emit_fiat = emitido_moneda in FIAT_CURRENCIES
    rec_fiat = recibido_moneda in FIAT_CURRENCIES
    if (tipo_defecto=='COMPRA' and not emit_fiat) or (tipo_defecto=='VENTA' and not rec_fiat):
        return "PERMUTA"
    return tipo_defecto

def split_batch_by_proportionality(rows: List[RawRow]) -> List[List[RawRow]]:
    solds = [r for r in rows if r.op_raw == "Transaction Sold"]
    revenues = [r for r in rows if r.op_raw == "Transaction Revenue"]
    buys = [r for r in rows if r.op_raw == "Transaction Buy"]
    spends = [r for r in rows if r.op_raw == "Transaction Spend"]
    fees = [r for r in rows if r.op_raw == "Transaction Fee"]

    # Ordenar todos por magnitud descendente
    solds.sort(key=lambda r: abs(r.change), reverse=True)
    revenues.sort(key=lambda r: abs(r.change), reverse=True)
    buys.sort(key=lambda r: abs(r.change), reverse=True)
    spends.sort(key=lambda r: abs(r.change), reverse=True)
    fees.sort(key=lambda r: abs(r.change), reverse=True)

    sub_ops = []
    if (solds and revenues):
        for i in range(min(len(solds), len(revenues), len(fees))):
            block = [solds[i], revenues[i], fees[i]]
            sub_ops.append(block)
    else:
        for i in range(min(len(buys), len(spends), len(fees))):
            block = [buys[i], spends[i], fees[i]]
            sub_ops.append(block)

    return sub_ops


def parse_group(ts: str, rows: List[RawRow]) -> List[NormalizedRow]:
    normalized = []
    subops = split_batch_by_proportionality(rows)
    for block in subops:       
        sold = next((r for r in block if r.op_raw == "Transaction Sold"), None)
        rev = next((r for r in block if r.op_raw == "Transaction Revenue"), None)
        buy    = next((r for r in block if r.op_raw == "Transaction Buy"), None)
        spend  = next((r for r in block if r.op_raw == "Transaction Spend"), None)
        fee = next((r for r in block if r.op_raw == "Transaction Fee"), None)
        
       
        if sold and rev:
            tipo = "VENTA"
            tipo = classify_tipo(sold.coin, rev.coin, tipo)
            normalized.append(
                NormalizedRow(
                    utc_time=ts,
                    tracker="BINANCE",
                    tipo=tipo,
                    emitido_moneda=sold.coin,
                    emitido_cantidad=sold.change,
                    emitido_valor_eur="",
                    recibido_moneda=rev.coin,
                    recibido_cantidad=rev.change,
                    recibido_valor_eur="",
                    comision_moneda=(fee.coin if fee else ""),
                    comision_cantidad=(fee.change if fee else Decimal("0")),
                    comision_valor_eur="",
                    declarable="S" if tipo in {"VENTA", "PERMUTA"} else "N",
                ))
        elif buy and spend:
            tipo = "COMPRA"
            tipo = classify_tipo(spend.coin, buy.coin,tipo)
            normalized.append(
                NormalizedRow(
                    utc_time=ts,
                    tracker="BINANCE",
                    tipo=tipo,
                    emitido_moneda=spend.coin,
                    emitido_cantidad=spend.change,
                    emitido_valor_eur="",
                    recibido_moneda=buy.coin,
                    recibido_cantidad=buy.change,
                    recibido_valor_eur="",
                    comision_moneda=(fee.coin if fee else ""),
                    comision_cantidad=(fee.change if fee else Decimal("0")),
                    comision_valor_eur="",
                    declarable="S",
                ))
    if len(rows) == 2:
        if rows[0].op_raw == "Binance Convert":
            print('LLEGAMOS A LAS 2')
            print (rows)         
            convert_from = next((r for r in rows if r.change < 0), None)
            convert_to   = next((r for r in rows if r.change > 0), None)
            normalized.append(NormalizedRow(
                utc_time=ts,
                tracker="BINANCE",
                tipo="COMPRA" if convert_to.coin not in FIAT_CURRENCIES else "VENTA",
                emitido_moneda=convert_from.coin,
                emitido_cantidad=convert_from.change,
                emitido_valor_eur="",
                recibido_moneda=convert_to.coin,
                recibido_cantidad=convert_to.change,
                recibido_valor_eur="",
                comision_moneda="",
                comision_cantidad=Decimal("0"),
                comision_valor_eur="",
                declarable="S" if convert_to.coin not in FIAT_CURRENCIES else "N",
            ))
    if len(rows) == 1:
        if rows[0].op_raw == 'Deposit':
            send_operacion = rows[0]
            normalized.append(NormalizedRow(
                utc_time=ts,
                tracker="BINANCE",
                tipo="DEPOSIT",
                emitido_moneda="",
                emitido_cantidad="",
                emitido_valor_eur="",
                recibido_moneda=send_operacion.coin,
                recibido_cantidad=send_operacion.change,
                recibido_valor_eur="",
                comision_moneda="",
                comision_cantidad=Decimal("0"),
                comision_valor_eur="",
                declarable="N",
            ))    
    return normalized

def check_integrity(raw_rows, normalized_rows, tolerance=Decimal("1e-8")):
    sums_original = defaultdict(Decimal)
    for r in raw_rows:
        sums_original[r.coin] += r.change

    sums_normalized = defaultdict(Decimal)
    for n in normalized_rows:
        if n.emitido_moneda:
            sums_normalized[n.emitido_moneda] += n.emitido_cantidad
        if n.recibido_moneda:
            sums_normalized[n.recibido_moneda] += n.recibido_cantidad
        if n.comision_moneda:
            sums_normalized[n.comision_moneda] += n.comision_cantidad

    print("\nPrueba de integridad:")
    for coin in sorted(set(sums_original.keys()) | set(sums_normalized.keys())):
        orig = sums_original[coin]
        norm = sums_normalized[coin]
        if abs(orig - norm) <= tolerance:
            print(f"{coin} → OK (original {orig}, normalizado {norm})")
        else:
            print(f"{coin} → ERROR (original {orig}, normalizado {norm})")	

def check_coin_amounts_absolute(df):
    totals_emitido = defaultdict(Decimal)
    totals_recibido = defaultdict(Decimal)
    totals_comision = defaultdict(Decimal)

    for _, row in df.iterrows():
        if row["Emitido_Moneda"]:
            totals_emitido[row["Emitido_Moneda"]] += abs(Decimal(row["Emitido_Cantidad"]))
        if row["Recibido_Moneda"]:
            totals_recibido[row["Recibido_Moneda"]] += abs(Decimal(row["Recibido_Cantidad"]))
        if row["Comision_Moneda"]:
            totals_comision[row["Comision_Moneda"]] += abs(Decimal(row["Comision_Cantidad"]))

    print("\nTotales emitido (valores absolutos):")
    for coin in sorted(totals_emitido.keys()):
        print(f"{coin}: {totals_emitido[coin]}")

    print("\nTotales recibido (valores absolutos):")
    for coin in sorted(totals_recibido.keys()):
        print(f"{coin}: {totals_recibido[coin]}")

    print("\nTotales comisión (valores absolutos):")
    for coin in sorted(totals_comision.keys()):
        print(f"{coin}: {totals_comision[coin]}")


    print("\nPrueba de integridad de cantidades (valores absolutos):")
    for coin in set(list(totals_emitido.keys()) + list(totals_recibido.keys()) + list(totals_comision.keys())):
        emit = totals_emitido[coin]
        rec = totals_recibido[coin]
        fee = totals_comision[coin]
        balance = rec - (emit + fee)
        if abs(balance) < Decimal("1e-8"):
            print(f"{coin} → OK (emitido {emit}, recibido {rec}, comisión {fee})")
        else:
            print(f"{coin} → DESCUADRE (emitido {emit}, recibido {rec}, comisión {fee}, balance {balance})")

def clean_number(value: str) -> str:
    if value is None:
        return None
    return value.replace("€", "").replace(",", "").strip()


def parse_coinbase_csv(input_path: str) -> List[NormalizedRow]:
    normalized = []
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            ts = r["Timestamp"]
            op = r["Transaction Type"]
            asset = r["Asset"]
            qty = Decimal(str(r["Quantity Transacted"])) if r["Quantity Transacted"] else Decimal("0")
            spot_currency = r["Price Currency"]
            spot_price = Decimal(str(clean_number(r["Price at Transaction"]))) if r["Price at Transaction"] else Decimal("0")
            subtotal = r["Subtotal"]
            subtotal_val = to_decimal(clean_number(subtotal)) if subtotal else None
            fees = Decimal(str(clean_number(r["Fees and/or Spread"]))) if r["Fees and/or Spread"] else Decimal("0")

            # ⚠️ Ajuste: solo calcular Subtotal si está vacío (None), no si es cero
            if subtotal_val is None and qty and spot_price:
                subtotal_val = qty * spot_price

            if op in ["Advanced Trade Buy","Pro Withdrawal"]:
                normalized.append(NormalizedRow(
                    utc_time=ts,
                    tracker="COINBASE",
                    tipo="COMPRA",
                    emitido_moneda=spot_currency,
                    emitido_cantidad=subtotal_val,
                    emitido_valor_eur="",
                    recibido_moneda=asset,
                    recibido_cantidad=qty,
                    recibido_valor_eur="",
                    comision_moneda=spot_currency,
                    comision_cantidad=fees,
                    comision_valor_eur="",
                    declarable="S"
                ))
            elif op == "Advanced Trade Sell":
                normalized.append(NormalizedRow(
                    utc_time=ts,
                    tracker="COINBASE",
                    tipo="VENTA",
                    emitido_moneda=asset,
                    emitido_cantidad=qty,
                    emitido_valor_eur="",
                    recibido_moneda=spot_currency,
                    recibido_cantidad=subtotal_val,
                    recibido_valor_eur="",
                    comision_moneda=spot_currency,
                    comision_cantidad=fees,
                    comision_valor_eur="",
                    declarable="S"
                ))
            elif op == "Reward Income":
                normalized.append(NormalizedRow(
                    utc_time=ts,
                    tracker="COINBASE",
                    tipo="REWARDS",
                    emitido_moneda="",
                    emitido_cantidad=Decimal("0"),
                    emitido_valor_eur="",
                    recibido_moneda=asset,
                    recibido_cantidad=qty,
                    recibido_valor_eur=subtotal_val,
                    comision_moneda=spot_currency,
                    comision_cantidad=fees,
                    comision_valor_eur="",
                    declarable="S"
                ))
            elif op == "Receive":
                normalized.append(NormalizedRow(
                    utc_time=ts,
                    tracker="COINBASE",
                    tipo="AIRDROP",
                    emitido_moneda="",
                    emitido_cantidad=Decimal("0"),
                    emitido_valor_eur="",
                    recibido_moneda=asset,
                    recibido_cantidad=qty,
                    recibido_valor_eur=subtotal_val,
                    comision_moneda=spot_currency,
                    comision_cantidad=fees,
                    comision_valor_eur="",
                    declarable="S"
                ))
            elif op == "Staking Income":
                normalized.append(NormalizedRow(
                    utc_time=ts,
                    tracker="COINBASE",
                    tipo="STAKING",
                    emitido_moneda="",
                    emitido_cantidad=Decimal("0"),
                    emitido_valor_eur="",
                    recibido_moneda=asset,
                    recibido_cantidad=qty,
                    recibido_valor_eur=subtotal_val,
                    comision_moneda=spot_currency,
                    comision_cantidad=fees,
                    comision_valor_eur="",
                    declarable="S"
                ))    
            elif op == "Send":
                emit_val_eur = ""
                if spot_currency == "EUR":
                    emit_val_eur = str(qty * spot_price)
                normalized.append(NormalizedRow(
                    utc_time=ts,
                    tracker="COINBASE",
                    tipo="SEND",
                    emitido_moneda=asset,
                    emitido_cantidad=qty,
                    emitido_valor_eur=emit_val_eur,
                    recibido_moneda="",
                    recibido_cantidad=Decimal("0"),
                    recibido_valor_eur="",
                    comision_moneda="",
                    comision_cantidad=Decimal("0"),
                    comision_valor_eur="",
                    declarable="N"
                ))
                
    return normalized


def main():
    getcontext().prec = 18
    import sys
    if len(sys.argv) < 4:
        print("Uso: python parseador_binance_excel.py binance.csv coinbase.csv output.xlsx")
        sys.exit(1)

    binance_input = sys.argv[1]
    coinbase_input = sys.argv[2]
    output_path = sys.argv[3]

    # Procesar BINANCE   

    raw_rows = []
    with open(binance_input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            raw_rows.append(
                RawRow(
                    user_id=r["User_ID"],
                    utc_time=r["UTC_Time"],
                    account=r["Account"],
                    op_raw=r["Operation"],
                    coin=r["Coin"],
                    change=to_decimal(r["Change"]),
                    remark=r.get("Remark", "")
                )
            )      

    grouped = defaultdict(list)
    for row in raw_rows:
        grouped[row.utc_time].append(row)
    print('Hay total de grupos',len(grouped))    

    normalized = []
    for ts, grp in grouped.items():
        normalized.extend(parse_group(ts, grp))

    check_integrity(raw_rows, normalized)

    normalized.extend(parse_coinbase_csv(coinbase_input))

    # Ordenar por UTC_Time
    normalized.sort(key=lambda r: parse_utc(r.utc_time))
    

    # Convertir a DataFrame y exportar a Excel
    df = pd.DataFrame([{
        "UTC_Time": r.utc_time,
        "Tracker": r.tracker,
        "Tipo": r.tipo,
        "Emitido_Moneda": r.emitido_moneda,
        "Emitido_Cantidad": str(r.emitido_cantidad),
        "Emitido_Valor_EUR": r.emitido_valor_eur,
        "Recibido_Moneda": r.recibido_moneda,
        "Recibido_Cantidad": str(r.recibido_cantidad),
        "Recibido_Valor_EUR": r.recibido_valor_eur,
        "Comision_Moneda": r.comision_moneda,
        "Comision_Cantidad": str(r.comision_cantidad),
        "Comision_Valor_EUR": r.comision_valor_eur,
        "Declarable": r.declarable,
    } for r in normalized])
	
    remove_negative_signs(df)
    check_coin_amounts_absolute(df)
    #convert_stables_in_df(df)
    translate_eur_values(df)
    convert_no_stables_in_df(df)
    procesar_df_con_fifo(df,CryptoFIFO())
    	
    df.to_excel(output_path, index=False)
    print(f"Procesadas {len(raw_rows)} filas -> {len(normalized)} operaciones normalizadas")
    print(f"Excel escrito en: {output_path}")

    informe_txt = generar_informe_fiscal_base_ahorro_txt(df)

    with open("informe_base_ahorro.txt", "w", encoding="utf-8") as f:
        f.write(informe_txt)


if __name__ == "__main__":
    main()