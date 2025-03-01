import os
import sys
import argparse
from pipelines.bronze_polygon import BronzePipeline
from pipelines.silver_polygon import SilverPipeline
from pipelines.gold_polygon import GoldPipeline
import logging
import datetime as dt

# Add the pipelines directory to the system path
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipelines'))

logging.basicConfig(
    filename=f'{dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_pipeline_log.log',
    level=logging.INFO, 
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Initialize constants
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
POLYGON_BASE_URL = "https://api.polygon.io/v2"
DIM_DATE_PATH = "../seeds/dim_date.csv"

def main(run_bronze: bool, run_silver: bool, run_gold: bool):
    bronze_success = True
    silver_success = True

    if run_bronze:
        # Process bronze data
        print("Processing bronze data...")
        try:
            pipeline = BronzePipeline(POLYGON_API_KEY, POLYGON_BASE_URL)
            logging.info("BronzePipeline initiated.")

            tickers_df = pipeline.get_list_of_stocks("../seeds/stocks.csv")
            logging.info("Stocks retrieved.")
            
            for ticker in tickers_df:
                logging.info(f"Fetching data for {ticker}")
                pipeline.fetch_ticker_data(ticker, multiplier=1, timespan="day", start_date="2023-01-01", end_date="2024-12-31")
                logging.info(f"Data for {ticker} written")
            
        except Exception as e:
            logging.error(f"Error in BronzePipeline: {str(e)}")
            bronze_success = False
        finally:
            logging.info("BronzePipeline finished")

    if run_silver and bronze_success:
        # Process silver data
        print("Processing silver data...")
        logging.info("Starting SilverPipeline")
        pipeline = SilverPipeline()

        bronze_bucket = "../buckets/bronze/2023-01-01_2024-12-31"
        silver_bucket = "../buckets/silver/stocks.parquet"

        try:
            pipeline.transform_stocks_to_silver(input_folder_path=bronze_bucket, output_file_path=silver_bucket)
        except Exception as e:
            logging.error("Error in SilverPipeline: %s", e)
            silver_success = False
        finally:
            logging.info("SilverPipeline finished")
    elif run_silver and not bronze_success:
        logging.warning("Skipping SilverPipeline due to BronzePipeline failure")

    if run_gold and silver_success:
        # Process gold data
        print("Processing gold data...")
        pipeline = GoldPipeline(DIM_DATE_PATH)
        path = "../buckets/silver/stocks.parquet"

        try:
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

            # Consolidate insights and output into one table
            pipeline.consolidate_insights(df, ("2023-01-01", "2023-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/2023_consolidated.csv", header=True)
            pipeline.consolidate_insights(df, ("2024-01-01", "2024-12-31")).coalesce(1).write.mode("overwrite").csv("../buckets/gold/2024_consolidated.csv", header=True)
        except Exception as e:
            logging.error("Error in GoldPipeline: %s", e)
        finally:
            logging.info("GoldPipeline finished")
    elif run_gold and not silver_success:
        logging.warning("Skipping GoldPipeline due to SilverPipeline failure")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data pipelines")
    parser.add_argument("--bronze", action="store_true", help="Run the BronzePipeline")
    parser.add_argument("--silver", action="store_true", help="Run the SilverPipeline")
    parser.add_argument("--gold", action="store_true", help="Run the GoldPipeline")
    args = parser.parse_args()

    main(run_bronze=args.bronze, run_silver=args.silver, run_gold=args.gold)