from enum import Enum
import csv
import itertools
import uuid
from operator import itemgetter
from typing import List
import pandas as pd
import numpy as np
import math
from typing import Dict, List

from graph_results import generate_plot


LOAD_LAST_ROWS = 1_000_000  # 400_000
SKIP_ROWS = 2891488 - LOAD_LAST_ROWS  # TODO, dynaically calculate 2891488
NROWS = LOAD_LAST_ROWS

# TODO dynamically get last N days


class OrderType(Enum):
    STOP = "STOP"
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    TRAILING_STOP = "TRAILING_STOP"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


lot_size = 1
payline_points = 4.0
risk_points = 2.0
reset_extension_pts = 1.50
tiered_limit_increase = 6.5
trailing_stop = 4.0
slippage = 0.25
trailing_stop_max_profit = 34.0

# TODO
max_positions = 6


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
    def __init__(
        self,
        buy_or_sell: OrderSide,
        order_type: OrderType,
        price,
        quantity: int,
        oco_id: str,
        **kwargs,
    ):
        self.buy_or_sell = buy_or_sell
        self.order_type = order_type
        self.price = price
        self.quantity = quantity
        self.oco_id = oco_id
        self.executed_at = None
        self.kwargs = kwargs

    def evaluate_update_stop_price(self, row):
        if self.buy_or_sell == OrderSide.BUY:
            diff = row.close - self.price
            if diff > self.kwargs["trailing_pts"]:
                self.price = row.close - self.kwargs["trailing_pts"]

        if self.buy_or_sell == OrderSide.SELL:
            diff = self.price - row.close
            if diff > self.kwargs["trailing_pts"]:
                self.price = row.close + self.kwargs["trailing_pts"]

    def __repr__(self):
        return f"<Order buy_or_sell={self.buy_or_sell.value} order_type={self.order_type} price={self.price} quantity={self.quantity} oco_id={self.oco_id} kwargs={self.kwargs}>"

    def to_dict(self):
        return dict(
            buy_or_sell=self.buy_or_sell.value,
            order_type=self.order_type.value,
            price=self.price,
            quantity=self.quantity,
            oco_id=self.oco_id,
            executed_at=self.executed_at,
            kwargs=self.kwargs,
        )


class Observation:
    def __init__(self, observed_at, note: str):
        self.observed_at = observed_at
        self.note = note

    def to_dict(self):
        return dict(
            observed_at=self.observed_at,
            note=self.note,
        )


class Agent:
    def __init__(self, params: Dict):
        self.params = params
        self.positions = 0
        self.orders = []
        self.pts = 0.0
        self.able_to_reload = True
        self.filled_orders = []
        self.observations = []
        self.cum_pts = []

    def or_mid_point(self):
        return (self.params["or_high"] + self.params["or_low"]) / 2.0

    def log(self, msg):
        pass
        # print(msg)

    def status(self):
        return {"positions": self.positions, "orders": self.orders, "pts": self.pts}

    def log_status(self):
        status = self.status()
        # self.log(f"pts = {status['pts']}")

    def flatten_all(self, row):
        if self.positions != 0:
            if self.positions > 0:
                buy_or_sell = OrderSide.SELL
            if self.positions < 0:
                buy_or_sell = OrderSide.BUY
            market_order = Order(buy_or_sell, OrderType.MARKET, row.close, abs(self.positions), None)
            self.fill_order(market_order, row)
        
        


    def create_stop_order(self, buy_or_sell, stop_price, quantity, oco_id=None):
        order = Order(buy_or_sell, OrderType.STOP, stop_price, quantity, oco_id)
        self.orders.append(order)

    def create_tailing_stop_order(
        self, buy_or_sell: OrderSide, stop_price, trail_pts, quantity, oco_id=None
    ):
        order = Order(
            buy_or_sell,
            OrderType.TRAILING_STOP,
            stop_price,
            quantity,
            oco_id,
            trailing_pts=trail_pts,
        )
        self.orders.append(order)

    def create_limit_order(self, buy_or_sell, limit_price, quantity, oco_id=None):
        order = Order(buy_or_sell, OrderType.LIMIT, limit_price, quantity, oco_id)
        self.orders.append(order)

    def create_oco_bracket(self, buy_or_sell, stop_price, limit_price, quantity):
        oco_id = str(uuid.uuid4())
        self.create_stop_order(buy_or_sell, stop_price, quantity, oco_id)
        self.create_limit_order(buy_or_sell, limit_price, quantity, oco_id)

    def create_oco_bracket_trailing(self, buy_or_sell, stop_price, trailing_pts, limit_price, quantity):
        oco_id = str(uuid.uuid4())
        self.create_tailing_stop_order(buy_or_sell, stop_price, trailing_pts, quantity, oco_id)
        self.create_limit_order(buy_or_sell, limit_price, quantity, oco_id)

    def create_oco_bracket_tiered(
        self, buy_or_sell, stop_price, limit_price_and_quantities: List
    ):
        for limit_price, qty in limit_price_and_quantities:
            oco_id = str(uuid.uuid4())
            self.create_stop_order(buy_or_sell, stop_price, qty, oco_id)
            self.create_limit_order(buy_or_sell, limit_price, qty, oco_id)

    def remove_order_and_related_orders(self, order):
        if order in self.orders:
            self.orders.remove(order)

        if order.oco_id:
            # https://stackoverflow.com/questions/1207406/how-to-remove-items-from-a-list-while-iterating
            # remove items from a list while iterating through them
            self.orders[:] = [x for x in self.orders if order.oco_id != x.oco_id]

    def fill_order(self, order, row):
        if order.buy_or_sell == OrderSide.BUY:
            self.positions = self.positions + order.quantity
            self.pts += order.quantity * row.close
            self.log(
                f"BOT {order.quantity} @ {row.close}. Type={order.order_type.value}. Resulting positions = {self.positions}"
            )

        if order.buy_or_sell == OrderSide.SELL:
            self.positions = self.positions - order.quantity
            self.pts -= order.quantity * row.close
            self.log(
                f"SOLD {order.quantity} @ {row.close}. Type={order.order_type.value}. Resulting positions = {self.positions}"
            )

        order.executed_at = row.name
        self.filled_orders.append(order.to_dict())
        self.remove_order_and_related_orders(order)

    def evaluate_and_trigger_orders(self, row):
        for order in self.orders:
            # TODO, move this evaluation into the order class/object
            if order.buy_or_sell == OrderSide.SELL:
                if order.order_type == OrderType.STOP and row.close <= order.price:
                    self.fill_order(order, row)
                
                if order.order_type == OrderType.TRAILING_STOP and row.close <= order.price:
                    self.fill_order(order, row)

                if order.order_type == OrderType.LIMIT and row.close >= order.price:
                    self.fill_order(order, row)

            if order.buy_or_sell == OrderSide.BUY:
                if order.order_type == OrderType.STOP and row.close >= order.price:
                    self.fill_order(order, row)
                if order.order_type == OrderType.TRAILING_STOP and row.close >= order.price:
                    self.fill_order(order, row)

                if order.order_type == OrderType.LIMIT and row.close <= order.price:
                    self.fill_order(order, row)

            if order.order_type == OrderType.TRAILING_STOP:
                order.evaluate_update_stop_price(row)

    def enter(self, row, buy_or_sell, quantity):
        # enter long
        if buy_or_sell == OrderSide.BUY:
            stop_price = max((row.close - risk_points), self.or_mid_point()+0.5)
            limit_price1 = row.close + payline_points
            limit_price2 = row.close + payline_points + tiered_limit_increase
            limit_price_and_quantities = [(limit_price1, 1), (limit_price2, 1)]

            market_order = Order(
                OrderSide.BUY,
                OrderType.MARKET,
                row.close,
                quantity * len(limit_price_and_quantities),
                None,
            )
            self.fill_order(market_order, row)
            self.create_oco_bracket_tiered(
                OrderSide.SELL, stop_price, limit_price_and_quantities
            )
            mo = Order(OrderSide.BUY, OrderType.MARKET, row.close, 1, None)
            self.fill_order(mo, row)
            self.create_oco_bracket_trailing(OrderSide.SELL, stop_price, trailing_stop, row.close+trailing_stop_max_profit, 1)


        # enter short
        if buy_or_sell == OrderSide.SELL:
            stop_price = min((row.close + risk_points), self.or_mid_point()-0.5)
            limit_price1 = row.close - payline_points
            limit_price2 = row.close - payline_points - tiered_limit_increase
            limit_price_and_quantities = [(limit_price1, 1), (limit_price2, 1)]

            market_order = Order(
                buy_or_sell,
                OrderType.MARKET,
                row.close,
                quantity * len(limit_price_and_quantities),
                None,
            )
            self.fill_order(market_order, row)
            self.create_oco_bracket_tiered(
                OrderSide.BUY, stop_price, limit_price_and_quantities
            )
            mo = Order(OrderSide.SELL, OrderType.MARKET, row.close, 1, None)
            self.fill_order(mo, row)
            self.create_oco_bracket_trailing(OrderSide.BUY, stop_price, trailing_stop, row.close-trailing_stop_max_profit, 1)

        self.able_to_reload = False

    def next_row(self, i, row):
        if self.positions == 0 and self.able_to_reload:
            if row.close >= self.params["or_high"]:
                self.enter(row, OrderSide.BUY, lot_size)

            if row.close <= self.params["or_low"]:
                self.enter(row, OrderSide.SELL, lot_size)

        if len(self.orders) > 0:
            self.evaluate_and_trigger_orders(row)

        if self.able_to_reload == False and self.positions == 0:
            if row.close >= (
                self.params["or_low"] - reset_extension_pts
            ) and row.close <= (self.params["or_high"] + reset_extension_pts):
                self.observations.append(Observation(row.name, "crossed_or").to_dict())
                self.log("--- able to reload")
                self.able_to_reload = True


def run_simulation(df, agent):
    date = pd.to_datetime(df.index.values[0]).date()

    high, low = determine_or(df)
    or_range = high - low
    or_range_delta = or_range
    or_range = (high, low)
    # msg = f"------------ running sim for {date}. {or_range}"
    # print(msg)

    # TODO - create snythetic OR when the real OR is
    # if or_range_delta <= 1.0:
    #     mid_point = (or_range_delta/2.0) - high
    #     high = mid_point + 0.25
    #     low = mid_point - 0.25

    agent.params = {"or_high": high, "or_low": low}

    # end 15 mins before the close to end flat each day
    for i, row in df.between_time("12:30:31", "19:45:00").iterrows():
        agent.next_row(i, row)
    
    try:
        agent.flatten_all(row)

        # TODO handle new days
        filled_orders_df = pd.DataFrame(agent.filled_orders)
        filled_orders_df.to_csv("filled_orders.csv")
        observations_df = pd.DataFrame(agent.observations)
        generate_plot(date, df, filled_orders_df, observations_df)
        # msg = f"Filled Orders = {len(agent.filled_orders)}. Profit = {agent.status()['pts']}"
        msg = f"{date}, {len(agent.filled_orders)}, {agent.status()['pts']}"
        print(msg)
    except UnboundLocalError as e:
        pass


df = get_instrument_data()

skip_first_date = True

print("date, attempts, profits")

for k, v in df.groupby(df.index.date):
    if skip_first_date:
        # skip first date. it will have a partial set of data
        skip_first_date = False
        continue

    if k.weekday() == 6:
        # skip sundays
        continue

    run_simulation(v, Agent(params={}))
