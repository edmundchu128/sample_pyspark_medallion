import logging
import os
import datetime as dt
import time
import json
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Retrieve POLYGON_API_KEY from environmental variables
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    raise EnvironmentError("POLYGON_API_KEY environment variable not set")

POLYGON_BASE_URL = "https://api.polygon.io/v2"

class BronzePipeline:
    """
    BronzePipeline class to fetch data from Polygon API and write it to the bronze bucket. 

    """
    def __init__(self, 
                 POLYGON_API_KEY: str = POLYGON_API_KEY, 
                 POLYGON_BASE_URL: str = POLYGON_BASE_URL):
        self.spark = self._spark_client()
        self.POLYGON_BASE_URL = POLYGON_BASE_URL
        self.POLYGON_API_KEY = POLYGON_API_KEY

    @staticmethod
    def _spark_client() -> SparkSession:
        """
        Initialize and return a SparkSession.
        
        Returns:
            SparkSession: The Spark session object.
        """
        spark = SparkSession.builder \
            .appName("BronzePipeline") \
            .master("local[*]") \
            .getOrCreate()
        return spark

    def get_list_of_stocks(self, path: str) -> list:
        """
        Read the list of stocks from a CSV file and return a list of stock symbols.
        
        Args:
            spark (SparkSession): The Spark session object.
            path (str): The path to the CSV file containing stock symbols.
        
        Returns:
            list: A list of stock symbols.
        
        Raises:
            Exception: If the DataFrame is empty or any other error occurs.
        """
        ### Read the list of stocks from the CSV file and return a list of stock symbols
        try:
            stocks_df = self.spark.read.csv(path, header=True)
            if not stocks_df.head(1):
                raise Exception("Empty dataframe!")
            
            stocks_df = stocks_df.select('symbol').distinct().toPandas()['symbol']
            return list(stocks_df)
        except Exception as e:
            logging.error(str(e))
            raise Exception(str(e))

    ### Retry decorator to handle rate limit with wait time (4s, 8s, 16s, 32s, 60s)
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=4, min=4, max=60), retry=retry_if_exception_type(requests.exceptions.RequestException))
    def get_request(self, url, params):
        """
        Retrieve the specified URL with retries in case of hitting rate limit of 5 api calls per minute.
        
        wait_exponential is used to increase the wait time between retries exponentially and try to retrieve the data at earliest (4s, 8s, 16s, 32s, 60s)

        Args:
            url (str): The URL to make the GET request to.
            params (dict): The parameters to include in the GET request.
        
        Returns:
            requests.Response: The response object.
        
        Raises:
            requests.exceptions.RequestException: If the request fails.
        """
        response = requests.get(url=url, params=params)
        if response.status_code == 200:
            logging.info(f"Data fetched successfully from {url}")
            return response
        elif response.status_code == 429:
            logging.warning("Rate limit hit")
            raise requests.exceptions.RequestException("Rate limit hit")
        else:
            response.raise_for_status()
        return response

    def fetch_ticker_data(self, ticker: str, multiplier: int = 1, timespan: str = "day", start_date: str = "2023-01-01", end_date: str = "2023-12-31", API_KEY: str = POLYGON_API_KEY) -> str:
        """
        Call Polygon ticker API to retrieve data of a given stock.
        
        Args:
            ticker (str): The ticker code as shown in Polygon (i.e. AAPL).
            multiplier (int): The size of the time window, defaults to 1.
            timespan (str): The size of the time window, defaults to "day".
            start_date (str): Fetch dataset starting from this date, defaults to "2023-01-01", inclusive.
            end_date (str): Fetch dataset up to this date, defaults to "2023-12-31", inclusive.
            API_KEY (str): The API key for Polygon.
        
        Returns:
            str: The dataset retrieved from the API.
        """

        ### Build the URL and parameters for the API request
        url = f"{self.POLYGON_BASE_URL}/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
        params = {"apiKey": API_KEY}

        ### Call the get_request method to retrieve the data with retries
        dataset = self.get_request(url, params).json()
    
        ### Make sure the output directory exists
        output_dir = f"../buckets/bronze/{start_date}_{end_date}"
        os.makedirs(os.path.dirname(output_dir + '/'), exist_ok=True)
        output_path = f"{output_dir}/{ticker}_{int(time.time())}.json"
        
        ### Write the dataset to a JSON file
        with open(output_path, "w") as f:
            json.dump(dataset, f)
        
        return dataset

if __name__ == "__main__":
    begin_time = time.time()
    logging.info(f"Ingestion pipeline started at {begin_time}")

    try:

        pipeline = BronzePipeline(POLYGON_API_KEY, POLYGON_BASE_URL)
        logging.info("BronzePipeline initiated.")

        tickers_df = pipeline.get_list_of_stocks( "../seeds/stocks.csv")
        logging.info("Stocks retrieved.")
        
        for ticker in tickers_df:
            logging.info(f"Fetching data for {ticker}")
            pipeline.fetch_ticker_data(ticker, multiplier=1, timespan="day", start_date="2023-01-01", end_date="2024-12-31")
            logging.info(f"Data for {ticker} written")
        
        end_time = time.time()
        logging.info(f"Pipeline finished at {end_time}. Took {end_time - begin_time} seconds")
    except Exception as e:
        logging.error(f"Error in pipeline: {str(e)}")
    finally:
        logging.info("Ingestion pipeline finished")