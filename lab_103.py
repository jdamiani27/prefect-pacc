from typing import Any, Dict

import requests
from pandas import DataFrame
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret


@task
def get_time_series(symbol: str, secret_block: Any):
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = "https://www.alphavantage.co/query"
    r = requests.get(
        url,
        params={
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "5min",
            "apikey": secret_block,
        },
    )
    return r.json()


@task
def analyze_prices(prices: Dict[str, Any]):
    logger = get_run_logger()
    prices_df = DataFrame.from_dict(
        prices["Time Series (5min)"],
        orient="index",
    )
    prices_df.columns = ["open", "high", "low", "close", "volume"]
    analysis = f"Average open price: {prices_df['open'].astype('float').mean()}"
    logger.info(analysis)
    return analysis


@flow
def alphavantage(ticker: str = "PENN"):
    secret_block = Secret.load("alphavantage")
    prices = get_time_series(ticker, secret_block)
    analyze_prices(prices)


if __name__ == "__main__":
    alphavantage()
