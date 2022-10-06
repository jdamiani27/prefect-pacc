from datetime import timedelta
from typing import Optional

import yfinance as yf
from pandas import DataFrame
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1), retries=2)
def get_ticker(
    ticker: str, start_date: Optional[str] = None, end_date: Optional[str] = None
):
    logger = get_run_logger()
    logger.info("Not using cache")
    if start_date and end_date:
        return yf.download(ticker, start=start_date, end=end_date)
    else:
        return yf.download(ticker)


@task
def display_prices(prices: DataFrame):
    logger = get_run_logger()
    logger.info(prices.head())


@flow
def stocks_for_range(ticker: str, start_date: str, end_date: str):
    prices = get_ticker(ticker, start_date, end_date)
    display_prices(prices)


@flow
def stocks():
    prices = get_ticker("PENN")
    display_prices(prices)
    stocks_for_range("PENN", "2022-10-03", "2022-10-04")


if __name__ == "__main__":
    stocks()
