from enum import Enum 
import csv
import itertools
import uuid
from operator import itemgetter
from typing import List
import pandas as pd
import numpy as np
import math


LOAD_LAST_ROWS = 50_000 # 400_000
SKIP_ROWS = 2891488 - LOAD_LAST_ROWS  # TODO, dynaically calculate 2891488
NROWS = LOAD_LAST_ROWS

class OrderType(Enum):
    STOP = "STOP"
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


lot_size = 1
payline_points = 5.0
risk_points = 4.0


def get_instrument_data() -> List:
    fn = "../MES-202009-GLOBEX-USD.csv"
    headers = pd.read_csv(fn, nrows=0).columns
    df = pd.read_csv(fn, skiprows=SKIP_ROWS, nrows=NROWS, names=headers)
    column_names = {
        "Date": "date",
        " Time": "time",
        " Open": "open",
        " High": "high",
        " Low": "low",
        " Last": "close",
        " Volume": "volume",
        " NumberOfTrades": "number_of_trades",
        " BidVolume": "bid_volume",
        " AskVolume": "ask_volume",
    }
    df.rename(columns=column_names, inplace=True)
    df["ts"] = df["date"] + df["time"]
    df["ts"] = pd.to_datetime(df["ts"], format="%Y/%m/%d  %H:%M:%S")
    df["ts"] = df["ts"].dt.tz_localize("UTC")
    df.set_index("ts", inplace=True)
    return df


def determine_or(df):
    opening_range = df.between_time(
        "12:30:00", "12:30:30"
    )  # TODO, make this work with/without DST
    high, low = opening_range.close.max(), opening_range.close.min()
    return (high, low)


class Order:
    def __init__(self, buy_or_sell, order_type: OrderType, price, quantity, oco_id, executed_at):
        self.buy_or_sell = buy_or_sell
        self.order_type = order_type
        self.price = price
        self.quantity = quantity
        self.oco_id = oco_id
        self.executed_at = executed_at
    
    def __repr__(self):
        return f"<Order buy_or_sell={self.buy_or_sell} order_type={self.order_type} price={self.price} quantity={self.quantity} oco_id={self.oco_id}>"
    
    def to_dict(self):
        return dict(
            buy_or_sell=self.buy_or_sell,
            order_type=self.order_type.value,
            price=self.price,
            quantity=self.quantity,
            oco_id=self.oco_id,
            executed_at=self.executed_at
        )



class Agent:
    def __init__(self, params):
        self.params = params
        self.positions = 0
        self.orders = []
        self.pts = 0.0
        self.able_to_reload = True
        self.filled_orders = []

    def log(self, msg):
        print(msg)

    def status(self):
        return {"positions": self.positions, "orders": self.orders, "pts": self.pts}

    def log_status(self):
        status = self.status()
        self.log(f"pts = {status['pts']}")

    def flatten_all(self, row):
        if self.positions >= 0:
            market_order = Order("sell", OrderType.MARKET, row.close, self.positions, None, None)
            self.fill_order(market_order, row)
        if self.positions <= 0:
            market_order = Order("buy", OrderType.MARKET, row.close, -1 * self.positions, None, None)
            self.fill_order(market_order, row)

    def create_stop_order(self, buy_or_sell, stop_price, quantity, oco_id=None):
        order = Order(buy_or_sell, OrderType.STOP, stop_price, quantity, oco_id, None)
        self.orders.append(order)

    def create_limit_order(self, buy_or_sell, limit_price, quantity, oco_id=None):
        order = Order(buy_or_sell, OrderType.LIMIT, limit_price, quantity, oco_id, None)
        self.orders.append(order)

    def create_oco_bracket(self, buy_or_sell, stop_price, limit_price, quantity):
        oco_id = str(uuid.uuid4())
        # import ipdb; ipdb.set_trace()
        self.create_stop_order(buy_or_sell, stop_price, quantity, oco_id)
        self.create_limit_order(buy_or_sell, limit_price, quantity, oco_id)

    def remove_order_and_related_orders(self, order):
        if order in self.orders:
            self.orders.remove(order)

        # todo, do this via python filter
        if order.oco_id:
            for __order in self.orders:
                if __order.oco_id:
                    if order.oco_id == __order.oco_id:
                        self.orders.remove(__order)

    def fill_order(self, order, row):
        if order.buy_or_sell == "buy":
            self.positions = self.positions + order.quantity
            self.pts += order.quantity * row.close
            self.log(f"BOT {order.quantity} @ {row.close}. Type={order.order_type.value}")

        if order.buy_or_sell == "sell":
            self.positions = self.positions - order.quantity
            self.pts -= order.quantity * row.close
            self.log(f"SOLD {order.quantity} @ {row.close}. Type={order.order_type.value}")
    
        order.executed_at = row.name
        self.filled_orders.append(order.to_dict())
        
        self.remove_order_and_related_orders(order)
        
        self.log_status()


    def evaluate_and_trigger_orders(self, row):
        for order in self.orders:
            if order.buy_or_sell == "sell":
                if order.order_type == OrderType.STOP and row.close <= order.price:
                    self.fill_order(order, row)
                    
                if order.order_type == OrderType.LIMIT and row.close >= order.price:
                    self.fill_order(order, row)

            if order.buy_or_sell == "buy":
                if order.order_type == OrderType.STOP and row.close >= order.price:
                    self.fill_order(order, row)

                if order.order_type == OrderType.LIMIT and row.close <= order.price:
                    self.fill_order(order, row)

    def enter(self, row, buy_or_sell, quantity):
        if buy_or_sell == "buy":
            market_order = Order("buy", OrderType.MARKET, row.close, quantity, None, None)
            stop_price = row.close - risk_points
            limit_price = row.close + payline_points
            self.create_oco_bracket("sell", stop_price, limit_price, quantity)

        if buy_or_sell == "sell":
            market_order = Order(buy_or_sell, OrderType.MARKET, row.close, quantity, None, None)
            stop_price = row.close + risk_points
            limit_price = row.close - payline_points
            self.create_oco_bracket("buy", stop_price, limit_price, quantity)
            
        self.fill_order(market_order, row)
        self.able_to_reload = False

    def next_row(self, i, row):
        if self.positions == 0 and self.able_to_reload:
            if row.close > self.params["or_high"]:
                self.enter(row, "buy", lot_size)

            if row.close < self.params["or_low"]:
                self.enter(row, "sell", lot_size)

        if self.positions != 0:
            self.evaluate_and_trigger_orders(row)
        
        if self.able_to_reload == False and self.positions == 0:
            if self.params["or_low"] >= row.close and row.close <= self.params["or_high"]:
                print("able to reload")
                self.able_to_reload = True


def run_simulation(df, agent):
    date = df.index.values[0]
    high, low = determine_or(df)
    or_range = high - low
    or_range = (high, low)
    msg = f"------------ running sim for {date}. {or_range}"
    print(msg)

    agent.params = {"or_high": high, "or_low": low}

    # end 15 mins before the close to end flat each day
    for i, row in df.between_time("12:30:31", "19:45:00").iterrows():
        agent.next_row(i, row)

    agent.flatten_all(row)

    # TODO,
    # create plot showing
    # 1. asset price
    # 2. horizontal line with OR all the way across
    # 3. Buy and Sells

    # TODO handle new days

    df.to_csv("asset_prices.csv")
    fills = pd.DataFrame(agent.filled_orders)
    fills.to_csv("filled_orders.csv")

    import ipdb; ipdb.set_trace()


df = get_instrument_data()

skip_first_date = True

for k, v in df.groupby(df.index.date):
    if skip_first_date:
        # skip first date. it will have a partial set of data
        skip_first_date = False
        continue

    if k.weekday() == 6:
        # skip sundays
        continue

    run_simulation(v, Agent(params={}))
