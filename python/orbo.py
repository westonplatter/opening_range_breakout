import csv
import itertools
import uuid
from operator import itemgetter
from typing import List

def get_instrument_data() -> List:
    file_name = "../mes-july2020.csv"
    reader = csv.DictReader(open(file_name, "r"))
    data = [x for x in reader]
    return data


SLIPPAGE = 0.25

DEFAULT_QTY = 1

STOP_PTS = 2.0
PROFIT_PTS = 4.0


class Portfolio:
    def __init__(self, cash=100000):
        self.balance = float(cash)
        self._positions = 0
        self._orders = []
        self._trades = []

    def buy(self, qty, price, exits: List = []):
        self.balance = self.balance - (1.0 * price * qty)
        self._positions += qty
        log(f"bought {qty} @ {price}. Positions={self.positions}. Balance={self.balance}")
        [self._orders.append(x) for x in exits]

    def sell(self, qty, price, exits: List = []):
        self.balance = self.balance + (1.0 * price * qty)
        self._positions -= qty
        log(f"sold {qty} @ {price}. Positions={self.positions}. Balance={self.balance}")
        [self._orders.append(x) for x in exits]

    def execute_actionable_orders(self, t, bid, offer) -> List:
        for x in self._orders:
            if x['kind'] < 0:
                if x['type'] == 'stop' and offer <= x['price']:
                    self.sell(DEFAULT_QTY, bid)
                    self.remove_order(x)
                if x['type'] == 'limit' and x['price'] <= bid:
                    self.sell(DEFAULT_QTY, bid)
                    self.remove_order(x)

            if x['kind'] > 0:
                if x['type'] == 'stop' and bid >= x['price']:
                    self.buy(DEFAULT_QTY, offer)
                    self.remove_order(x)
                if x['type'] == 'limit' and x['price'] >= offer:
                    self.buy(DEFAULT_QTY, offer)
                    self.remove_order(x)


    def remove_order(self, order: object) -> None:
        # TODO instrospect order - if oco, handle that. else just remove order
        if 'oco_uuid' in order:
            for x in list(filter(lambda xx: xx['oco_uuid'] == order['oco_uuid'], self.orders)):
                self.remove_order_by_object(x)

    def remove_order_by_object(self, order: object) -> None:
        self._orders.remove(order)

    @property
    def positions(self):
        return self._positions

    @property
    def orders(self):
        return self._orders

    @property
    def can_go_long(self):
        return self.positions < 2

    @property
    def can_go_short(self):
        return -2 < self.positions

    @property
    def can_trade(self):
        return self.can_go_long or self.can_go_short


def gen_exits(kind, stop, profit, oco: bool=False):
    orders = []

    _id1, _id2 = str(uuid.uuid4()), str(uuid.uuid4())
    orders.append({"id": _id1, "kind": kind, "type": "stop", "price": stop, "oco_uuid": order_uuid})
    orders.append({"id": _id2, "kind": kind, "type": "limit", "price": profit, "oco_uuid": order_uuid})

    if oco:
        order_uuid = str(uuid.uuid4())
        [x['oco_uuid'] = order_uuid for x in orders]

    return orders


def log(msg):
    print(msg)


portfolio = Portfolio()
data = get_instrument_data()

for date, group in itertools.groupby(data, lambda item: item["date"]):
    rows = [x for x in group]
    rth_rows = list(filter(lambda x: x['rth'] == 'TRUE', rows))

    if len(rth_rows) == 0:
        continue

    range_high, range_low = float(rth_rows[0]["high"]), float(rth_rows[0]["low"])

    available = True
    end_of_day = False

    log(f"{date}. {range_high}-{range_low}")

    for row in rth_rows[1:]:
        if end_of_day:
            continue

        close = float(row['close'])

        if available == False:
            if range_low <= close and close <= range_high:
                available = True

        bid = close - SLIPPAGE
        offer = close + SLIPPAGE

        if portfolio.can_trade:
            if close < range_low and portfolio.can_go_short and available:
                qty = DEFAULT_QTY
                stop_price = bid + STOP_PTS
                profit_price = bid - PROFIT_PTS
                exits = gen_exits(qty, stop_price, profit_price, True)
                portfolio.sell(qty, bid, exits)
                available = False

            if close > range_high and portfolio.can_go_long and available:
                qty = DEFAULT_QTY
                stop_price = offer - STOP_PTS
                profit_price = offer + PROFIT_PTS
                exits = gen_exits(-qty, stop_price, profit_price, True)
                portfolio.buy(qty, offer, exits)
                available = False

        portfolio.execute_actionable_orders(row['time'], bid, offer)

        if row['time'] == '16:50':
            end_of_day = True

            if portfolio.positions > 0:
                portfolio.sell(portfolio.positions, bid)
            elif portfolio.positions < 0:
                portfolio.buy(-portfolio.positions, offer)

            print(f"end of day liquidations. Positions = {portfolio.positions}")


# todo
# - portfolio, move position changes out of balance
# - instrument - handle conversion of /MES -> 5.0 per pt
