import logging.config
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

DIM_DATE_PATH = "../seeds/dim_date.csv"

class GoldPipeline:
    def __init__(self, dim_date_path: str = DIM_DATE_PATH):
        """
            Initialize the GoldPipeline class.

            Args:
                dim_date_path (str): The path to the dim_date CSV file.
        """
        self.spark = self.spark_client()
        self.dim_date_path = dim_date_path
        self._valid_dim_date_path = self._check_dim_date_path()

    def _check_dim_date_path(self):
        """
        Check if the dim_date_path file exists. If not, raise a FileNotFoundError.
        """
        # Check if the dim_date_path file exists
        if not os.path.exists(self.dim_date_path):
            logging.error(f"dim_date_path file does not exist: {self.dim_date_path}")
            raise FileNotFoundError(f"dim_date_path file does not exist: {self.dim_date_path}")

    @staticmethod
    def spark_client() -> SparkSession:
        """
        Initialize and return a SparkSession.
        
        Returns:
            SparkSession: The Spark session object.
        """
        spark = SparkSession.builder \
            .appName("GoldPipeline") \
            .master("local[*]") \
            .getOrCreate()
        return spark

    def read_silver(self, path: str) -> DataFrame:
        """
        Read a parquet file from the given path and return a DataFrame.
        
        Args:
            spark (SparkSession): The Spark session object.
            path (str): The path to the parquet file.
        
        Returns:
            DataFrame: The DataFrame containing the data from the parquet file.
        """
        df = self.spark.read.parquet(path, header=True)
        return df

    def get_max_relative_increase_by_year(self, df: DataFrame, date_filter: tuple) -> DataFrame:
        """
        Get the stock with the maximum relative increase in price by year. Use this function with date_filter across years.
        
        Args:
            df (DataFrame): The input DataFrame.
            date_filter (tuple): A tuple containing the start and end dates for filtering.
        
        Returns:
            DataFrame: The DataFrame containing the stock with the maximum relative increase by year.
        """
        ### Read files and filter based on date filter
        df_agg = df.filter(F.col('date').between(date_filter[0], date_filter[1]))
        ticker_window_spec = Window.partitionBy(["ticker", "year"])
        df_agg = df_agg.withColumn("year", F.year("date")) \
                       .withColumn("first_close", F.first("close").over(ticker_window_spec)) \
                       .withColumn("last_close", F.last("close").over(ticker_window_spec))
        
        ### Calculate the net change for each ticker
        df_agg = df_agg.select("ticker"
                               , "year"
                               , "first_close"
                               , "last_close"
                               , ((F.col("last_close") - F.col("first_close")) / F.col("first_close")).alias("net_change")).distinct()
        df_agg = df_agg.orderBy(F.desc("net_change"))
        return df_agg

    def get_max_relative_increase(self, df: DataFrame, date_filter: tuple) -> DataFrame:
        """
        Get the stock with the maximum relative increase in price.
        
        Args:
            df (DataFrame): The input DataFrame.
            date_filter (tuple): A tuple containing the start and end dates for filtering.
        
        Returns:
            DataFrame: The DataFrame containing the stock with the maximum relative increase.
        """
        ### Read files and filter based on date filter
        df_agg = df.filter(F.col('date').between(date_filter[0], date_filter[1]))
        ticker_window_spec = Window.partitionBy(["ticker"])
        df_agg = df_agg.withColumn("first_close", F.first("close").over(ticker_window_spec)) \
                       .withColumn("last_close", F.last("close").over(ticker_window_spec))
        
        ### Calculate the net change for each ticker
        df_agg = df_agg.select("ticker"
                               , "first_close"
                               , "last_close"
                               , ((F.col("last_close") - F.col("first_close")) / F.col("first_close")).alias("net_change")).distinct()
        df_agg = df_agg.orderBy(F.desc("net_change")).limit(1)
        return df_agg

    def get_investment_return(self, df: DataFrame, date_filter: tuple, initial_investment=1000000) -> DataFrame:
        """
        Calculate the investment return if $1 million was invested equally in all stocks.
        
        Args:
            df (DataFrame): The input DataFrame.
            date_filter (tuple): A tuple containing the start and end dates for filtering.
            initial_investment (int, optional): The initial investment amount. Defaults to 1000000.
        
        Returns:
            DataFrame: The DataFrame containing the investment return.
        """
        ### Read files and filter based on date filter
        df_agg = df.filter(F.col('date').between(date_filter[0], date_filter[1]))
        ticker_window_spec = Window.partitionBy(["ticker", "year"])

        ### Calculate the first and last close price for each ticker
        df_agg = df_agg.withColumn("year", F.year("date")) \
                       .withColumn("first_close", F.first("close").over(ticker_window_spec)) \
                       .withColumn("last_close", F.last("close").over(ticker_window_spec))
        
        ### Calculate the net change for each ticker
        df_agg = df_agg.select("ticker", "year", "first_close", "last_close", 
                               ((F.col("last_close") - F.col("first_close")) / F.col("first_close")).alias("net_change")).distinct()
        list_of_tickers = list(df_agg.select(F.col("ticker")).distinct().toPandas()["ticker"])
        investment_per_ticker = initial_investment / len(list_of_tickers)
        
        ### Convert to investment_value and sum of investment_value for each year
        df_agg = df_agg.withColumn("investment_value", (investment_per_ticker / F.col("first_close")) * F.col("last_close"))
        df_agg = df_agg.groupBy("ticker","year").agg(F.sum(F.col("investment_value")).alias("investment_value"))
        
        df_agg = df_agg.groupBy("year").agg(F.sum(F.col("investment_value")).alias("investment_value"))
        return df_agg

    def get_max_CAGR_ticker(self, df: DataFrame, date_filter: tuple, initial_investment=1000000) -> DataFrame:
        """
        Get the stock with the maximum Compound Annual Growth Rate (CAGR) between January and June.
        
        Args:
            df (DataFrame): The input DataFrame.
            date_filter (tuple): A tuple containing the start and end dates for filtering.
            initial_investment (int, optional): The initial investment amount. Defaults to 1000000.
        
        Returns:
            DataFrame: The DataFrame containing the stock with the maximum CAGR.
        """
        df_agg = df.filter(F.col('date').between(date_filter[0], date_filter[1]))
        ticker_window_spec = Window.partitionBy(["ticker", "year"])
        df_agg = df_agg.withColumn("year", F.year("date")) \
                       .withColumn("first_close", F.first("close").over(ticker_window_spec)) \
                       .withColumn("last_close", F.last("close").over(ticker_window_spec))
        
        df_agg = df_agg.select("ticker", "year", "first_close", "last_close", 
                               ((F.col("last_close") - F.col("first_close")) / F.col("first_close")).alias("net_change")).distinct()
        list_of_tickers = list(df_agg.select(F.col("ticker")).distinct().toPandas()["ticker"])
        investment_per_ticker = initial_investment / len(list_of_tickers)
        df_agg = df_agg.withColumn("jan_value", F.lit(investment_per_ticker))
        df_agg = df_agg.withColumn("june_value", (F.col("jan_value") / F.col("first_close")) * F.col("last_close"))
        df_agg = df_agg.withColumn("CAGR", (F.pow(F.col("june_value") / F.col("jan_value"), (1.0 / 0.5)) - 1) * 100)
        df_agg = df_agg.orderBy(F.desc("CAGR")).limit(1)
        return df_agg

    def get_max_decrease_by_week(self, df: DataFrame, date_filter: tuple, initial_investment=1000000) -> DataFrame:
        """
        Get the stock with the maximum decrease in value within a single week.
        
        Args:
            df (DataFrame): The input DataFrame.
            date_filter (tuple): A tuple containing the start and end dates for filtering.
            initial_investment (int, optional): The initial investment amount. Defaults to 1000000.
        
        Returns:
            DataFrame: The DataFrame containing the stock with the maximum decrease in value within a single week.
        """

        ### Read files and filter based on date filter
        dim_date = self.spark.read.csv(self.dim_date_path, header=True, inferSchema=True)
        df_agg = df.filter(F.col('date').between(date_filter[0], date_filter[1]))

        
        dim_date = dim_date.withColumn("date", F.to_date(dim_date["date"], "yyyy-mm-dd")) \
                           .filter(F.col("date").between(date_filter[0], date_filter[1])) \
                           .select("date", "weeknum", "year")
        list_of_tickers = list(df_agg.select(F.col("ticker")).distinct().toPandas()["ticker"])
        investment_per_ticker = initial_investment / len(list_of_tickers)
        
        ### Get the first close price for each ticker, calculate the shares and day value for each ticker
        ticker_window_spec = Window.partitionBy(["ticker"])
        df_agg = df_agg.join(dim_date, on="date", how="left")
        df_agg = df_agg.select(df_agg["ticker"], df_agg["date"], df_agg["close"], dim_date["weeknum"], dim_date["year"], 
                               F.first(df_agg["close"]).over(ticker_window_spec).alias("first_close")) \
                       .withColumn("shares", investment_per_ticker / F.col("first_close")) \
                       .withColumn("day_value", F.col("shares") * F.col("close"))
        
        ### Get the first and last value for each week, year is needed as weeknum could cross years
        ticker_week_asc_window_spec = Window.partitionBy(["ticker", "weeknum", "year"]).orderBy("date")
        ticker_week_desc_window_spec = Window.partitionBy(["ticker", "weeknum", "year"]).orderBy(F.desc("date"))
        df_agg = df_agg.withColumn("week_first_value", F.first("day_value").over(ticker_week_asc_window_spec)) \
                       .withColumn("week_last_value", F.first("day_value").over(ticker_week_desc_window_spec))
        
        ### Calculate the net change for each week
        df_agg = df_agg.select("ticker"
                               , "date"
                               , "weeknum"
                               , "week_last_value"
                               , "week_first_value"
                               , (F.col("week_last_value") - F.col("week_first_value")).alias("net_change")).distinct()
        
        ### Get the maximum decrease/lowest increase in value within a single week
        df_agg = df_agg.orderBy(F.asc("net_change")).limit(10)
        return df_agg

if __name__ == "__main__":

    ## Initialize gold pipeline
    pipeline = GoldPipeline(DIM_DATE_PATH)
    path = "../buckets/silver/stocks.parquet"

    # Read the silver data, used to pass onto the functions for insights
    df = pipeline.read_silver(path)
    
    # a. Which stock has had the greatest relative increase in price in this period?
    pipeline.get_max_relative_increase(df, ("2024-01-01", "2024-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/a.MAX_RELATIVE_INCREASE_2024.csv", header=True)
    pipeline.get_max_relative_increase(df, ("2023-01-01", "2023-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/a.MAX_RELATIVE_INCREASE_2023.csv", header=True)
    
    # b. If you had invested $1 million at the beginning of this period by purchasing $10,000 worth of shares in every company in the list equally, how much would you have today? Technical note, you can assume that it is possible to purchase fractional shares?
    pipeline.get_investment_return(df, ("2024-01-01", "2024-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/b.INVESTMENT_VALUE_2024.csv", header=True)
    pipeline.get_investment_return(df, ("2023-01-01", "2023-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/b.INVESTMENT_VALUE_2023.csv", header=True)
    
    # c. Which stock had the greatest value in monthly CAGR between January and June?
    pipeline.get_max_CAGR_ticker(df, ("2024-01-01", "2024-06-30")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/c.CAGR_TICKER_JAN_JUNE_2024.csv", header=True)
    pipeline.get_max_CAGR_ticker(df, ("2023-01-01", "2023-06-30")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/c.CAGR_TICKER_JAN_JUNE_2023.csv", header=True)
    
    # d. During the year, which stock had the greatest decrease in value within a single week and which week was this?
    pipeline.get_max_decrease_by_week(df, ("2024-01-01", "2024-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/d.MAX_DECREASE_2024.csv", header=True)
    pipeline.get_max_decrease_by_week(df, ("2023-01-01", "2023-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/d.MAX_DECREASE_2023.csv", header=True)