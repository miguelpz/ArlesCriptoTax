from collections import defaultdict, deque
from decimal import Decimal

TOL = Decimal("0.00000002")


class CryptoFIFO:
    def __init__(self):
        # Cada cripto tiene su cola FIFO de lotes
        self.lotes = defaultdict(deque)

    def add(self, fecha: str, cripto: str, cantidad: Decimal, precio_unitario: Decimal):
        """
        Añade un lote con fecha, cripto, cantidad y precio unitario en euros.
        """
        lote = {
            "fecha": fecha,
            "cripto": cripto.upper(),
            "cantidad": cantidad,
            "precio_unitario": precio_unitario
        }
        self.lotes[cripto.upper()].append(lote)

    def consume(self, cripto: str, cantidad: Decimal):
        """
        Consume una cantidad de la cripto siguiendo FIFO.
        Devuelve el coste total en euros y el detalle de lotes consumidos.
        """
        cripto = cripto.upper()
        if cripto not in self.lotes:
            raise ValueError(f"No hay registros para {cripto}")

        coste_total = Decimal("0")
        restante = cantidad
        detalle = []

        while restante > 0 and self.lotes[cripto]:
            lote = self.lotes[cripto][0]
            cantidad = lote["cantidad"]
            precio_unitario = lote["precio_unitario"]

            if lote["cantidad"] <= restante:
                # Consumimos todo el lote
                coste = lote["cantidad"] * lote["precio_unitario"]
                coste_total += coste
                
                detalle.append({
                    f"Salida lote: {cantidad} {cripto} a {precio_unitario} EUR/u total: {coste}"
                })
                restante -= lote["cantidad"]
                self.lotes[cripto].popleft()
            else: 
                # Consumimos parte del lote
                coste = restante * lote["precio_unitario"]
                coste_total += coste
                detalle.append({
                    f"Salida lote: {restante} {cripto} a {precio_unitario} EUR/u total: {coste}"
                })
                lote["cantidad"] -= restante
                restante = Decimal("0")
 

        if restante > 0:
            print(f'Han faltado {restante} para recuperar{cantidad}')
            if restante > TOL: 
                raise ValueError(f"No hay suficiente {cripto} para consumir {cantidad} restante a {restante}")
 
        return coste_total, detalle

# -------------------------
# Bloque de prueba rápida
# -------------------------
if __name__ == "__main__":
    fifo = CryptoFIFO()

    fifo.add("2023-01-01", "BTC", Decimal("0.5"), Decimal("20000"))
    fifo.add("2023-02-01", "BTC", Decimal("0.5"), Decimal("25000"))
    fifo.add("2023-03-01", "ETH", Decimal("10"), Decimal("1500"))

    coste, detalle = fifo.consume("BTC", Decimal("0.7"))
    print("Coste total:", coste)
    print("Detalle de lotes consumidos:")
    for d in detalle:
        print(d)

