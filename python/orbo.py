import csv
import itertools
from operator import itemgetter

file_name = "../mes-july2020.csv"
reader = csv.DictReader(open(file_name, "r"))
data = [x for x in reader]

SLIPPAGE = 0.25

class Portfolio:
    def __init__(self, cash=100000):
        self.balance = float(cash)
        self._positions = 0
        # self._orders = []
    
    def buy(self, qty, price):
        self.balance = self.balance - (1.0 * price * qty)
        self._positions += qty
        print(f"bought {qty} @ {price}. Positions = {self.positions}")
    
    def sell(self, qty, price):
        self.balance = self.balance + (1.0 * price * qty)
        self._positions -= qty
        print(f"sold {qty} @ {price}. Positions = {self.positions}")

    # @property
    # def orders(self):
    #     return self._orders

    @property
    def positions(self):
        return self._positions

    @property
    def can_go_long(self):
        return self.positions < 2

    @property
    def can_go_short(self):
        return -2 < self.positions

    @property
    def can_trade(self):
        return self.can_go_long or self.can_go_short

def log(msg):
    print(msg)


portfolio = Portfolio()

for date, group in itertools.groupby(data, lambda item: item["date"]):
    rows = [x for x in group]

    range_high, range_low = float(rows[0]["high"]), float(rows[0]["low"])

    log(f"{date}. {range_high}-{range_low}")

    for row in rows[1:]:
        close = float(row['close'])
        high = float(row['high'])
        low = float(row['low'])

        if portfolio.can_trade:
            buy_price = float(row["high"]) + SLIPPAGE
            sell_price = float(row["low"]) - SLIPPAGE

            if close > range_high and portfolio.can_go_long:
                portfolio.buy(1, buy_price)

            if close < range_low and portfolio.can_go_short:
                portfolio.sell(1, sell_price)
    
    break


