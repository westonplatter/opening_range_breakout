import datetime
import pandas as pd
import numpy as np
import matplotlib

matplotlib.use("agg")
import matplotlib.pyplot as plt
from matplotlib import gridspec


def condense_assset_df(ddf, agg_interval):
    agg_col_definition = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
        "number_of_trades": "sum",
        "bid_volume": "sum",
        "ask_volume": "sum",
    }
    return ddf.resample(agg_interval).agg(agg_col_definition)


def prepare_asset_data(ddf, agg_interval):
    return condense_assset_df(ddf, agg_interval)


def prepare_filled_orders_data(ddf):
    cols = ["buy_or_sell", "order_type", "price", "quantity", "oco_id", "executed_at"]
    ddf = ddf[cols].copy()
    ddf.executed_at = pd.to_datetime(ddf.executed_at)
    return ddf


def get_opening_range(ddf):
    opening_range = ddf.between_time(
        "12:30:00", "12:30:30"
    )  # TODO, make this work with/without DST
    high, low = opening_range.close.max(), opening_range.close.min()
    return (high, low)


def get_opening_range_last_ts(ddf):
    return ddf.between_time("12:30:00", "12:30:30").index.values[-1]


def only_rth_data(ddf):
    return ddf.between_time("10:30:00", "20:00:00")


def dot_annotation_for_order(order_row):
    if order_row.buy_or_sell == "buy":
        # return "go"
        return "bo"
    if order_row.buy_or_sell == "sell":
        return "ro"


def plot_filled_orders(date, ddf, ddf_filled_orders, observations_ddf):
    # plt.figure(figsize=(15, 8))
    # plt.grid(True)
    fig = plt.figure(figsize=(20, 15))
    # gs = gridspec.GridSpec(2, 1, height_ratios=[5, 2])
    gs = gridspec.GridSpec(1, 1, height_ratios=[1])
    ax1 = plt.subplot(gs[0])
    # ax2 = plt.subplot(gs[1]) # TODO add MACD(26, 12, 9) on 30 second data

    ax1.grid(True)
    # ax2.grid(True)

    title = f"date = {date}"
    plt.title(title)

    or_high, or_low = get_opening_range(ddf)

    ax1.plot(only_rth_data(ddf).close)

    # OR high, low, & middle lines
    ax1.axhline(y=or_high, color="r", lw=0.8)
    ax1.axhline(y=or_low, color="r", lw=0.8)
    ax1.axhline(y=((or_high + or_low)) / 2, color="b", lw=0.4)

    ax1.axvline(x=get_opening_range_last_ts(ddf), color="r", lw=0.6)

    for i, order_row in ddf_filled_orders.iterrows():
        annotation = dot_annotation_for_order(order_row)
        ax1.plot(order_row.executed_at, order_row.price, annotation)

    for i, observation_row in observations_ddf.iterrows():
        ax1.axvline(x=observation_row.observed_at, color="g", lw=0.6)

    plt.savefig(f"chart_{date}.png")


def generate_plot(
    date: datetime.date,
    raw_asset_price_by_date: pd.DataFrame,
    raw_filled_orders_by_date: pd.DataFrame,
    observations_df: pd.DataFrame,
):
    interval = "30S"
    asset_prices = prepare_asset_data(raw_asset_price_by_date, interval)
    filled_orders = prepare_filled_orders_data(raw_filled_orders_by_date)
    plot_filled_orders(date, asset_prices, filled_orders, observations_df)
