from decimal import Decimal
import urllib.request, json
import os
import shutil
from datetime import date, timedelta, datetime
import pandas as pd

import requests


def get_usd_to_eur_rate(query_date: str) -> Decimal:
    """
    Consulta el tipo de cambio USD/EUR del BCE para una fecha dada (YYYY-MM-DD)
    usando la Frankfurter API (datos oficiales del BCE).
    """

    today_str = date.today().isoformat()
    cache_file = f"usd_eur_rates_{today_str}.json"

    if not os.path.exists(cache_file):
        # Buscar ficheros anteriores
        backups = [f for f in os.listdir(".") if f.startswith("usd_eur_rates_") and f.endswith(".json")]
        if backups:
            # Ordenar por fecha descendente
            backups.sort(reverse=True)
            last_backup = backups[0]
            shutil.copy(last_backup, cache_file)
            print(f"Copiado {last_backup} como base para {cache_file}")

    # Cargar caché actual (si existe)
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    # Si ya está en caché, devolverlo
    if query_date in cache:
        return Decimal(str(cache[query_date]))
    
    # Consultar API

    url = f"https://api.frankfurter.app/{query_date}?from=USD&to=EUR"
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())

    # La API devuelve algo como:
    # {"amount":1.0,"base":"USD","date":"2022-12-23","rates":{"EUR":0.943}}
    if "rates" in data and "EUR" in data["rates"]:
        rate = Decimal(str(data["rates"]["EUR"]))
        print("Cambio: " + str(rate))

        cache[query_date] = str(rate)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, sort_keys=True)
        return rate
    else:
        raise ValueError(f"No se encontró tipo USD/EUR para {query_date}")


def convert_stables_in_df(df):
    """
    Recorre el DataFrame y convierte USDC/USDT a EUR en las columnas
    Emitido_Valor_EUR, Recibido_Valor_EUR y Comision_Valor_EUR.
    """
    for idx, row in df.iterrows():
        # Fecha en formato YYYY-MM-DD
        date_str = str(row["UTC_Time"])[:10]
        rate = get_usd_to_eur_rate(date_str)

        # Emitido
        if row["Emitido_Moneda"] in ("USDC", "USDT"):
            df.at[idx, "Emitido_Valor_EUR"] = Decimal(str(row["Emitido_Cantidad"])) * rate

        # Recibido
        if row["Recibido_Moneda"] in ("USDC", "USDT"):
            df.at[idx, "Recibido_Valor_EUR"] = Decimal(str(row["Recibido_Cantidad"])) * rate

        # Comisión
        if row["Comision_Moneda"] in ("USDC", "USDT"):
            df.at[idx, "Comision_Valor_EUR"] = Decimal(str(row["Comision_Cantidad"])) * rate

    return df

def translate_eur_values(df):
    """
    Recorre el DataFrame y, si la moneda es EUR, copia la cantidad
    directamente a la columna *_Valor_EUR correspondiente.
    """
    for idx, row in df.iterrows():
        # Emitido
        if row["Emitido_Moneda"] == "EUR":
            df.at[idx, "Emitido_Valor_EUR"] = Decimal(str(row["Emitido_Cantidad"]))

        # Recibido
        if row["Recibido_Moneda"] == "EUR":
            df.at[idx, "Recibido_Valor_EUR"] = Decimal(str(row["Recibido_Cantidad"]))

        # Comisión
        if row["Comision_Moneda"] == "EUR":
            df.at[idx, "Comision_Valor_EUR"] = Decimal(str(row["Comision_Cantidad"]))

        # Recibido no EUR pero Emitido si, coversión con valor en euros tomado
        if row["Recibido_Moneda"] != "EUR" and row["Emitido_Moneda"] == "EUR":
            df.at[idx, "Recibido_Valor_EUR"] = Decimal(str(row["Emitido_Cantidad"]))

    return df


def convert_no_stables_in_df(df):
    """
    Recorre el DataFrame y convierte no estables ni fiat a EUR en las columnas
    Emitido_Valor_EUR, Recibido_Valor_EUR y Comision_Valor_EUR.
    """
    for idx, row in df.iterrows():
        # Fecha en formato YYYY-MM-DD
        date_str = pd.to_datetime(row["UTC_Time"], errors="coerce").strftime("%Y-%m-%d %H:%M")
            
        # Emitido
        if row["Emitido_Moneda"] and row["Emitido_Moneda"] and row["Emitido_Moneda"] not in ("EUR", "USD"):            
            if row["Emitido_Cantidad"] is not None and (pd.isna(row["Emitido_Valor_EUR"]) or row["Emitido_Valor_EUR"] == ""):
                if row["Recibido_Moneda"] and row["Recibido_Moneda"] and row["Recibido_Moneda"] not in ("EUR", "USD"): 
                    price = get_price_binance(row["Emitido_Moneda"],date_str)                                
                    df.at[idx, "Emitido_Valor_EUR"] = price * Decimal(str(row["Emitido_Cantidad"]))
                else:
                    print (f"VALOR DIRECTO --->  {str(idx)}")
                    price =  Decimal(str(row["Recibido_Valor_EUR"]))
                    df.at[idx, "Emitido_Valor_EUR"] = price   

        # Recibido
        if row["Recibido_Moneda"] and row["Recibido_Moneda"] and row["Recibido_Moneda"] not in ("EUR", "USD"):           
            if row["Recibido_Cantidad"] is not None and (pd.isna(row["Recibido_Valor_EUR"]) or row["Recibido_Valor_EUR"] == ""):
                price = get_price_binance(row["Recibido_Moneda"],date_str)
                df.at[idx, "Recibido_Valor_EUR"] = price * Decimal(str(row["Recibido_Cantidad"]))

        # Comisión
        if row["Comision_Moneda"] and row["Comision_Moneda"] and row["Comision_Moneda"] not in ("EUR", "USD"):            
            if row["Comision_Cantidad"] is not None and (pd.isna(row["Comision_Valor_EUR"]) or row["Comision_Valor_EUR"] == ""):
                price = get_price_binance(row["Comision_Moneda"],date_str)
                df.at[idx, "Comision_Valor_EUR"] = price * Decimal(str(row["Comision_Cantidad"]))

    return df    

def get_price_coingecko(asset_id: str, date_str: str, vs_currency: str = "usd") -> Decimal:
    """
    Obtiene el precio histórico de un cripto en CoinGecko.
    - asset_id: id de CoinGecko (ej. 'bitcoin', 'ethereum')
    - date_str: fecha en formato 'DD-MM-YYYY'
    - vs_currency: divisa de referencia ('usd', 'eur', etc.)
    """
    
    url = f"https://api.coingecko.com/api/v3/coins/{asset_id}/history?date={date_str}"
    print("llamada coingecko:" + str(url))
    r = requests.get(url)
    data = r.json()
    try:
        price = data["market_data"]["current_price"][vs_currency]
        return Decimal(str(price))
    except Exception:
        raise ValueError(f"No se encontró precio para {asset_id} en {date_str}")


# def get_price_binance(symbol: str, date_query: str, vs_currency: str = "EUR", interval: str = "1d") -> Decimal:
#     """
#     Obtiene el precio histórico de una cripto en Binance contra EUR en una fecha concreta.
    
#     Args:
#         symbol (str): Ticker de la cripto (ej. 'BTC', 'ETH').
#         date (str): Fecha en formato 'YYYY-MM-DD'.
#         vs_currency (str): Moneda fiat, por defecto 'EUR'.
#         interval (str): Intervalo de vela ('1m', '1h', '1d').
    
#     Returns:
#         Decimal Precio de cierre en EUR.
#     """

#     today_str = date.today().isoformat()
#     cache_file = f"binance_prices_{today_str}.json"
 
#     # --- 1. Si no existe cache_file, copiar el último backup ---
     
#     if not os.path.exists(cache_file):
#         backups = [f for f in os.listdir(".") if f.startswith("binance_prices_") and f.endswith(".json")]
#         if backups:
#             backups.sort(reverse=True)
#             last_backup = backups[0]
#             shutil.copy(last_backup, cache_file)
#             print(f"Copiado {last_backup} como base para {cache_file}")
    
#     # --- 2. Cargar caché actual ---

#     if os.path.exists(cache_file):
#         with open(cache_file, "r", encoding="utf-8") as f:
#             cache = json.load(f)
#     else:
#         cache = {}

#     key = f"{symbol.upper()}_{vs_currency.upper()}_{date_query}"

#     # --- 3. Si ya está en caché, devolverlo ---

#     if key in cache:
#         return Decimal(str(cache[key]))

#     # --- 4. Consultar Binance ---
    
#     pair = f"{symbol.upper()}{vs_currency.upper()}"
    
#     # Convertir fecha a timestamp en milisegundos (UTC)
#     dt = datetime.strptime(date_query, "%Y-%m-%d")
#     start_ts = int(dt.timestamp() * 1000)
#     end_ts = int((dt + timedelta(days=1)).timestamp() * 1000)
    
#     url = f"https://api.binance.com/api/v3/klines?symbol={pair}&interval={interval}&startTime={start_ts}&endTime={end_ts}"
#     print("llamda a binance-> " + str(url))
     
#     response = requests.get(url, timeout=10)
#     response.raise_for_status()
#     data = response.json()
#     if not data:
#         raise ValueError(f"No hay datos para {pair} en {date}")
#     # Cada vela: [open_time, open, high, low, close, volume, ...]
#     close_price = Decimal(data[0][4])
#     print("Precio: " + str(close_price))

#     # --- 5. Guardar en caché ---

#     cache[key] = str(close_price)
#     with open(cache_file, "w", encoding="utf-8") as f:
#         json.dump(cache, f, indent=2, sort_keys=True)

#     return close_price


def get_price_binance(symbol: str, datetime_query: str, vs_currency: str = "EUR") -> Decimal:  
    print(f'PARAMETROS CONVERSION!!  SYMBOL{symbol} DATETIME {datetime_query} VSCURRENCY {vs_currency}')
    if symbol in ("USDC", "USDT"): 
        return convert_stable_to_fiat(symbol, datetime_query, vs_currency,)
    return get_binance_close_price(symbol, datetime_query, vs_currency)   



def convert_stable_to_fiat(symbol_stable: str, datetime_query: str, fiat: str) -> Decimal:
    """
    Convierte una stablecoin (USDT, USDC) a EUR o USD usando BTC como puente.
    """
    # Precios minuto
    btc_stable = get_binance_close_price("BTC", datetime_query, vs_currency=symbol_stable)
    btc_fiat   = get_binance_close_price("BTC", datetime_query, vs_currency=fiat)

    return btc_fiat / btc_stable





def get_binance_close_price(symbol: str, datetime_query: str, vs_currency: str) -> Decimal:
    """
    Obtiene el precio histórico de una cripto en Binance contra EUR en un minuto concreto.

    Args:
        symbol (str): Ticker de la cripto (ej. 'BTC', 'ETH').
        datetime_query (str): Fecha y hora en formato 'YYYY-MM-DD HH:MM'.
        vs_currency (str): Moneda fiat, por defecto 'EUR'.

    Returns:
        Decimal: Precio de cierre del minuto solicitado.
    """

    # --- 1. Preparar caché ---
    today_str = date.today().isoformat()
    cache_file = f"binance_prices_{today_str}.json"

    if not os.path.exists(cache_file):
        backups = [f for f in os.listdir(".") if f.startswith("binance_prices_") and f.endswith(".json")]
        if backups:
            backups.sort(reverse=True)
            shutil.copy(backups[0], cache_file)

    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    # Clave de caché por minuto exacto
    key = f"{symbol.upper()}_{vs_currency.upper()}_{datetime_query}"

    if key in cache:
        return Decimal(str(cache[key]))

    # --- 2. Preparar llamada a Binance ---
    pair = f"{symbol.upper()}{vs_currency.upper()}"

    # Convertir fecha+hora a timestamp
    dt = datetime.strptime(datetime_query, "%Y-%m-%d %H:%M")
    start_ts = int(dt.timestamp() * 1000)
    end_ts = start_ts + 60_000  # 1 minuto después

    url = (
        f"https://api.binance.com/api/v3/klines?"
        f"symbol={pair}&interval=1m&startTime={start_ts}&endTime={end_ts}"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    if not data:
        raise ValueError(f"No hay datos para {pair} en {datetime_query}")

    # Vela de 1 minuto: [open_time, open, high, low, close, volume, ...]
    close_price = Decimal(data[0][4])

    # --- 3. Guardar en caché ---
    cache[key] = str(close_price)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, sort_keys=True)

    return close_price
    
