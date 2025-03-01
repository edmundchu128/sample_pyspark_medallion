import logging
import requests
import datetime as dt
import time
import json

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType
import os
import logging

logging.basicConfig(
     filename=f'{dt.datetime.now()}_pipeline_log.log',
     level=logging.INFO, 
     format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
)


## Retrieve POLYGON_API_KEY from environmental variables
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"]

## Initiate constants
POLYGON_BASE_URL = "https://api.polygon.io/v1"
DATE_TODAY = dt.datetime.now().strftime("%Y-%m-%d")

## Read in list of stocks
def get_list_of_stocks(spark:SparkSession, path:str) -> DataFrame:
    try:
        stocks_df = spark.read.csv(path, header= True)
        if not stocks_df.head(1):
            raise Exception("Empty dataframe!")
        
        stocks_df = stocks_df.select('symbol').distinct()
        return stocks_df
    except Exception as e:
        logging.error(str(e))
        raise Exception(str(e))

## Pyspark UDF for parallel API calls
def fetch_ticker_data_by_day(ticker: str
                            , base_url:str = POLYGON_BASE_URL
                            , start_date:str = "2023-01-01"
                            , end_date:str = "2023-12-31"
                            , API_KEY:str = POLYGON_API_KEY) -> str:
    """
    Call Polygon ticker API to retrieve data of a given stock.

    Keyword arguments:
    ticker -- Required parameter. ticker code as shown in Polygon (i.e. AAPL)
    base_url -- Polygon's API base url, defaults to constant POLYGON_BASE_URL
    start_date -- Fetch dataset starting from this date, defaults to "2023-01-01", inclusive.
    end_date -- Fetch dataset up to this date, defaults to "2023-12-31", inclusive.

    """

    if dt.datetime.strptime(start_date, "%Y-%m-%d")>dt.datetime.strptime(end_date,"%Y-%m-%d"):
        logging.error(f"Start date ({start_date}) is larger than end date ({end_date}). Please revise parameters.")
        raise ValueError(f"Start date ({start_date}) is larger than end date ({end_date}). Please revise parameters.")
    # if ticker == "symbol":
    #     logging.error("ticker == symbol")
    #     raise Exception("Still wrong!")
    

    url = f"{base_url}/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}"
    params = {"apiKey": API_KEY}

    try:
        response = requests.get(url=url, params=params)
        if response.status_code == 200:
                    return response.json().get("result", "No Data")
        elif response.status_code == 429:  # Rate limit exceeded
            time.sleep(delay)
            delay *= 2  # Exponential backoff\
    except Exception as e:
         return str(e)

    dataset = response.json()

    output_path = f"../buckets/bronze/{ticker}.json"

    os.makedirs(os.path.dirname(f"../buckets/bronze/{DATE_TODAY}"), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(dataset, f)

    return dataset

def spark_client()-> SparkSession:
    spark = SparkSession.builder \
    .appName("BronzePipeline") \
    .master("local[*]") \
    .getOrCreate()

    return spark

def assert_all_files_called(dir):
    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]
    assert len(files) == 100

    


if __name__ == "__main__":
    logging.info("Submitted spark job...")
    spark = spark_client()

    logging.info("Spark client initiated.")

    logging.info("Getting list of stocks...")

    tickers_df = get_list_of_stocks(spark, "../seeds/stocks.csv")

    logging.info(tickers_df.columns)

    logging.info("Stocks retrieved.")

    api_udf = F.udf(fetch_ticker_data_by_day, StringType())

    api_df = tickers_df.withColumn("response", api_udf(F.col("symbol")))

    api_df.show()

    assert_all_files_called("../buckets/bronze")