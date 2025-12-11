# parseador_binance_excel.py
import csv
import pandas as pd
from dataclasses import dataclass
from typing import List, Optional
from collections import defaultdict
from decimal import Decimal, InvalidOperation

FIAT_CURRENCIES = {"EUR", "USD"}  # Solo estas son fiduciarias

def to_decimal(x: str) -> Decimal:
    try:
        return Decimal(x)
    except InvalidOperation:
        return Decimal(str(float(x)))

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
                    declarable="S" if tipo in {"PERMUTA"} else "N",
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

def main():
    import sys
    if len(sys.argv) < 3:
        print("Uso: python parseador_binance_excel.py input.csv output.xlsx")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    raw_rows = []
    with open(input_path, newline="", encoding="utf-8") as f:
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
	
    check_integrity(raw_rows, normalized)
	
    df.to_excel(output_path, index=False)
    print(f"Procesadas {len(raw_rows)} filas -> {len(normalized)} operaciones normalizadas")
    print(f"Excel escrito en: {output_path}")

if __name__ == "__main__":
    main()