import csv
import itertools
from operator import itemgetter
from typing import List

file_name = "../mes-july2020.csv"
reader = csv.DictReader(open(file_name, "r"))
data = [x for x in reader]

SLIPPAGE = 0.25

class Portfolio:
    def __init__(self, cash=100000):
        self.balance = float(cash)
        self._positions = 0
        self._orders = []

    def buy(self, qty, price, exits: List = []):
        self.balance = self.balance - (1.0 * price * qty)
        self._positions += qty
        print(f"bought {qty} @ {price}. Positions={self.positions}. Balance={self.balance}")
        [self._orders.append(x) for x in exits]

    def sell(self, qty, price, exits: List = []):
        self.balance = self.balance + (1.0 * price * qty)
        self._positions -= qty
        print(f"sold {qty} @ {price}. Positions={self.positions}. Balance={self.balance}")
        [self._orders.append(x) for x in exits]

    def actionable_orders(self, t, bid, offer) -> List:
        for x in self._orders:
            #
            # NEXT - handle limit orders 
            #
            if x['kind'] == -1 and x['type'] == 'stop' and bid <= x['price']:
                msg = f"- {t}"
                log(msg)
                self.sell(1, bid)
            if x['kind'] == 1 and x['type'] == 'stop' and offer >= x['price']:
                msg = f"- {t}"
                log(msg)
                self.buy(1, offer)

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

def gen_exits(kind, stop, profit):
    return [
        {"kind": kind, "type": "stop", "price": stop},
        {"kind": kind, "type": "limit", "price": profit}
    ]

portfolio = Portfolio()

for date, group in itertools.groupby(data, lambda item: item["date"]):
    rows = [x for x in group]

    range_high, range_low = float(rows[0]["high"]), float(rows[0]["low"])

    log(f"{date}. {range_high}-{range_low}")

    for row in list(filter(lambda x: x['rth'] == 'TRUE', rows[1:])):
        close = float(row['close'])

        bid = close - SLIPPAGE
        offer = close + SLIPPAGE

        log(row['time'])

        if portfolio.can_trade:
            if close < range_low and portfolio.can_go_short:
                qty = 1
                stop_price = bid + 2.0
                profit_price = bid - 4.0
                exits = gen_exits(qty, stop_price, profit_price)
                portfolio.sell(qty, bid)

            if close > range_high and portfolio.can_go_long:
                qty = 1
                stop_price = offer - 2.0
                profit_price = offer + 4.0
                exits = gen_exits(-qty, stop_price, profit_price)
                portfolio.buy(qty, offer, exits)

        portfolio.actionable_orders(row['time'], bid, offer)


    # todos
    # - determine of exists need to be exercised (stop or limit orders, both long and short)
    # - exit positions 10 mins before market close
    # - exit - do OCO for orders
    # - portfolio, move position changes out of balance
    # - instrument - handle conversion of /MES -> 5.0 per pt

    break
