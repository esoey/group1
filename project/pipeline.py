"""Some docstring about document"""
#region Imports
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import sys

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from src.spark_config import initialize_spark
    from pyspark.sql import DataFrame 
    from pyspark.sql.functions import (
        col, lit, isnan, expr, when, count, sum as spark_sum, avg,
        min as spark_min, max as spark_max, round as spark_round,
        desc, asc, to_timestamp, to_date, hour, dayofweek,
        lower, upper, trim, concat, coalesce, explode, split,
        countDistinct, collect_list, first, input_file_name,
        regexp_extract, current_timestamp
    )

except ImportError as e:
    logger.error(f"PySpark import failed: {e}")
    logger.error("Please install PySpark: pip install pyspark")
    sys.exit(1)

try:
    import meteostat as ms
except ImportError as e:
    logger.error(f"Meteostat weather data import failed: {e}")
    logger.error("Please install Meteostat: pip install meteostat")
    sys.exit(1)
try:
    import pandas as pd
except ImportError as e:
    logger.error(f"Pandas import failed: {e}")
    logger.error("Please install Pandas: pip install pandas")
    sys.exit(1)

from pathlib import Path
from datetime import datetime, date, timedelta

from src.helper_functions import get_coordinates, get_paths, get_schema
#endregion

PROJECT_ROOT = Path(__file__).resolve().parent

class Pipeline:
    def __init__(self, data_dir: str = None, shared_dir: str = None):
        """Initialize the pipeline with output directories."""
        if data_dir is None:
            temp_dir = get_paths()
        else:
            temp_dir = get_paths(data_dir)
    
        if shared_dir is None:
            shared_dir = PROJECT_ROOT

        self.competitions_dir = temp_dir["COMPETITIONS"]
        self.matches_dir = temp_dir["MATCHES"]
        self.events_dir = temp_dir["EVENTS"]
        self.bronze_dir = f"{shared_dir}/data/bronze"
        self.silver_dir = f"{shared_dir}/data/silver"
        self.gold_dir = f"{shared_dir}/data/gold"


    def create_spark_session(self):
        """Create and configure Spark session."""
        logger.info("Creating Spark session...")

        self.spark = initialize_spark()
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    #region Bronze Layer
    def ingest_data(self, category: str):
        """a"""
        logger.info(f"Fetching data for {category}.")

        schema = get_schema(category)
        match category:
            case "competitions":
                df = self.spark.read.schema(schema).json(self.competitions_dir, multiLine=True, recursiveFileLookup=True)
            case "matches":
                df = self.spark.read.schema(schema).json(self.matches_dir, multiLine=True, recursiveFileLookup=True)
            case "events":
                df = self.spark.read.schema(schema).json(self.events_dir, multiLine=True, recursiveFileLookup=True)
                df = df.withColumn("match_id", regexp_extract(input_file_name(), r"/(\d+)\.json$", 1).cast("long"))
            case _:
                raise ValueError(f"Unknown category: {category}")
        
        df = df.withColumn("ingest_date", to_date(current_timestamp()))
        df = df.withColumn("ingest_time", current_timestamp())

        logger.info(f"Dataframe successfully created.")
        return df


    def fetch_weather_data(self, match_df: DataFrame) -> DataFrame:
        """
        Fetches weather data from the nearest weather station for the different stadiums.
        
        Args:
            match_df (DataFrame): Statsbomb dataframe containing matches

        Returns:
            (Dataframe): Returns the weather DataFrame.
        """
        logger.info("Fetching weather data from Meteostat API.")
        distinct_df = match_df.select(col("match_id"), col("stadium.name").alias("stadium"), col("stadium.country.name").alias("country"), col("match_date"))
        
        weather = []

        for row in distinct_df.collect():
            try:
                coordinates = get_coordinates(row["stadium"], row["country"])
            except KeyError as e:
                logger.info(f"No stadium found for {row['stadium']} in {row['country']}.")
                continue
            if coordinates is None:
                continue

            lat, long = coordinates

            POINT = ms.Point(lat, long)
            # station = ms.stations.nearby(POINT, limit=1).index[0]
            
            #Forslag: legge til dette og fjerne linja over slik at vi ikke får index error hvis meteostat ikke returner en værstasjon
            stations = ms.stations.nearby(POINT, limit=1)
            
            if stations.empty:
                logger.warning(f"No weather station found for {row['stadium']} in {row['country']}.")
                continue   

            station = stations.index[0] 
            #Forslag slutt
            
            match_date =  datetime.strptime(row["match_date"], "%Y-%m-%d")
            start = match_date + timedelta(hours=12)
            end = match_date + timedelta(hours=24)

            w = ms.hourly(station, start, end).fetch()
            if w is None: # Meaning the weather station has no data for that time-slot
                # TODO: Figure out if we should log it or quarantine it?
                continue

            w["match_id"] = row["match_id"]
            weather.append(w)

        weather = pd.concat(weather).reset_index()
        weather["time"] = pd.to_datetime(weather["time"], utc=True)
        weather_df = self.spark.createDataFrame(weather)
        weather_df = weather_df.withColumn("ingest_date", to_date(current_timestamp()))
        weather_df = weather_df.withColumn("ingest_time", current_timestamp())

        logger.info(f"Finished fetching weather data. \n - Created DataFrame with size {weather_df.count()}.")
        return weather_df


    def save_compressed_data(self, df: DataFrame, category: str):
        """Saves dataframe to parquet with the right path."""
        logger.info(f"Creating Parquet files for {category}.")

        match category:
            case "competitions":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/statsbomb/competitions", partitionBy="ingest_date")
            case "matches":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/statsbomb/matches", partitionBy="ingest_date")
            case "events":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/statsbomb/events", partitionBy="ingest_date")
            case "weather":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/meteostat", partitionBy="ingest_date")
            case "quarantine":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/quarantine", partitionBy="ingest_date")

        logger.info(f"Successfully created Parquet-files for {category}.")
    #endregion

    #region Silver Layer
    
    def load_silver_input(self, category: str):
        logger.info(f"Loading Bronze data for {category}.")

        if category == "weather":
            df = self.spark.read.parquet(f"{self.bronze_dir}/meteostat")
        else:
            df = self.spark.read.parquet(f"{self.bronze_dir}/statsbomb/{category}")

        latest_ingest_date = df.agg(spark_max("ingest_date")).first()[0]

        df = df.filter(col("ingest_date") == latest_ingest_date)

        return df


    def validate_matches(self, laliga_matches: DataFrame):
        logger.info("Validating La Liga matches.")

        # Validation rules
        invalid_match = (
            col("match_id").isNull() |
            col("kick_off").isNull() |
            col("stadium.name").isNull() |
            col("season.season_name").isNull() |
            to_date(col("match_date")).isNull() |
            col("home_score").isNull() |
            col("away_score").isNull() |
            (col("home_score") < 0) |
            (col("away_score") < 0)
        )

        duplicate_match_ids = (laliga_matches.groupBy("match_id").count().filter(col("count") > 1).select("match_id"))

        # Data quality before cleaning
        logger.info(f"Matches before cleaning: {laliga_matches.count()}")
        logger.info(f"Unique matches before cleaning: {laliga_matches.select('match_id').distinct().count()}")
        logger.info(f"Duplicate match ids: {duplicate_match_ids.count()}")
        logger.info(f"Invalid matches: {laliga_matches.filter(invalid_match).count()}")

        # Validation and quarantine
        quarantine_matches = laliga_matches.filter(invalid_match)
        valid_matches = laliga_matches.filter(~invalid_match)

        duplicate_matches = valid_matches.join(
            duplicate_match_ids,
            on="match_id",
            how="inner"
        )

        quarantine_matches = quarantine_matches.unionByName(duplicate_matches)

        valid_matches = valid_matches.join(
            duplicate_match_ids,
            on="match_id",
            how="left_anti"
        )

        # Data quality after validation
        logger.info(f"Invalid matches after cleaning: {valid_matches.filter(invalid_match).count()}")
        logger.info(f"Quarantined matches: {quarantine_matches.count()}")
        logger.info(f"Valid matches: {valid_matches.count()}")

        return valid_matches, quarantine_matches


    def transform_matches(self, valid_matches: DataFrame):
        logger.info("Transforming La Liga matches.")

        silver_matches = valid_matches.select(
            col("match_id"),
            col("kick_off"),
            col("stadium.name").alias("stadium"),
            col("home_team.home_team_name").alias("home_team"),
            col("away_team.away_team_name").alias("away_team"),
            col("home_team.home_team_id").alias("home_team_id"),
            col("away_team.away_team_id").alias("away_team_id"),
            col("home_score"),
            col("away_score"),
            col("season.season_name").alias("season"),
            to_date("match_date").alias("match_date")
        )

        # Data quality after cleaning
        logger.info(f"Matches after cleaning: {silver_matches.count()}")
        logger.info(f"Unique matches after cleaning: {silver_matches.select('match_id').distinct().count()}")
        logger.info(f"Duplicate match ids after cleaning: {silver_matches.groupBy('match_id').count().filter(col('count') > 1).count()}")

        return silver_matches


    def validate_events(self, laliga_events: DataFrame, silver_matches: DataFrame):
        logger.info("Validating La Liga events.")

        # Validation rules
        invalid_event = (
            col("id").isNull() |
            col("match_id").isNull() |
            col("type.name").isNull() |
            (
                (col("type.name") == "Pass") & col("pass").isNull()
            )
        )

        duplicate_event_ids = (laliga_events.groupBy("id").count().filter(col("count") > 1).select("id"))

        # Data quality before cleaning
        logger.info(f"Events before cleaning: {laliga_events.count()}")
        logger.info(f"Unique events before cleaning: {laliga_events.select('id').distinct().count()}")
        logger.info(f"Duplicate event ids: {duplicate_event_ids.count()}")
        logger.info(f"Invalid events: {laliga_events.filter(invalid_event).count()}")

        # Validation and quarantine
        quarantine_events = laliga_events.filter(invalid_event)
        valid_events = laliga_events.filter(~invalid_event)

        duplicate_events = valid_events.join(
            duplicate_event_ids,
            on="id",
            how="inner"
        )

        quarantine_events = quarantine_events.unionByName(duplicate_events)

        valid_events = valid_events.join(
            duplicate_event_ids,
            on="id",
            how="left_anti"
        )

        # Events without a valid match
        orphan_events = valid_events.join(
            silver_matches.select("match_id"),
            on="match_id",
            how="left_anti"
        )

        quarantine_events = quarantine_events.unionByName(orphan_events)

        valid_events = valid_events.join(
            silver_matches.select("match_id"),
            on="match_id",
            how="left_semi"
        )

        # Data quality after validation
        logger.info(f"Invalid events after cleaning: {valid_events.filter(invalid_event).count()}")
        logger.info(f"Quarantined events: {quarantine_events.count()}")
        logger.info(f"Valid events: {valid_events.count()}")

        return valid_events, quarantine_events


    def transform_events(self, valid_events: DataFrame):
        logger.info("Transforming La Liga events.")

        silver_events = valid_events.select(
            col("match_id"),
            col("id").alias("event_id"),
            col("type.name").alias("event_type"),
            col("pass"),
            col("team.name").alias("team"),
            col("player.name").alias("player"),
            col("shot"),
            col("foul_committed")
        )

        # Data quality after cleaning
        logger.info(f"Events after cleaning: {silver_events.count()}")
        logger.info(f"Unique events after cleaning: {silver_events.select('event_id').distinct().count()}")
        logger.info(f"Duplicate event ids after cleaning: {silver_events.groupBy('event_id').count().filter(col('count') > 1).count()}")

        return silver_events


    def validate_weather(self, laliga_weather: DataFrame, silver_matches: DataFrame):
        logger.info("Validating La Liga weather data.")

        weather_columns = ["temp", "rhum", "prcp", "wspd"]

        # Validation rules
        invalid_temp = ~col("temp").between(-50, 60) & ~isnan(col("temp"))
        invalid_rhum = ~col("rhum").between(0, 100) & ~isnan(col("rhum"))
        invalid_prcp = col("prcp") < 0
        invalid_wspd = col("wspd") < 0

        invalid_weather = (
            col("match_id").isNull() |
            col("time").isNull() |
            invalid_temp |
            invalid_rhum |
            invalid_prcp |
            invalid_wspd
        )

        invalid_weather = coalesce(invalid_weather, lit(False))

        # Check for duplicates
        duplicate_weather = (
            laliga_weather.groupBy("match_id", "time")
            .count()
            .filter(col("count") > 1)
        )

        # Data quality before cleaning
        logger.info(f"Weather observations (before cleaning): {laliga_weather.count()}")
        logger.info(f"Matches with weather (before cleaning): {laliga_weather.select('match_id').distinct().count()}")
        logger.info(f"Invalid weather observations (before cleaning): {laliga_weather.filter(invalid_weather).count()}")
        logger.info(f"Duplicate weather keys (before cleaning): {duplicate_weather.count()}")

        for column in weather_columns:
            missing = laliga_weather.filter(col(column).isNull() | isnan(col(column))).count()
            logger.info(f"Missing values in {column}: {missing}")

        # Validation and quarantine
        quarantine_weather = laliga_weather.filter(invalid_weather)
        valid_weather = laliga_weather.filter(~invalid_weather)

        # Quarantine duplicate observations
        duplicate_observations = valid_weather.join(
            duplicate_weather,
            on=["match_id", "time"],
            how="left_semi"
        )

        quarantine_weather = quarantine_weather.unionByName(duplicate_observations)

        valid_weather = valid_weather.join(
            duplicate_weather,
            on=["match_id", "time"],
            how="left_anti"
        )

        # Quarantine observations without a valid match
        orphan_weather = valid_weather.join(
            silver_matches.select("match_id"),
            on="match_id",
            how="left_anti"
        )

        quarantine_weather = quarantine_weather.unionByName(orphan_weather)

        valid_weather = valid_weather.join(
            silver_matches.select("match_id"),
            on="match_id",
            how="left_semi"
        )

        # Data quality after validation
        logger.info(f"Invalid weather observations (after cleaning): {valid_weather.filter(invalid_weather).count()}")
        logger.info(f"Quarantined weather observations: {quarantine_weather.count()}")
        logger.info(f"Valid weather observations: {valid_weather.count()}")

        return valid_weather, quarantine_weather


    def transform_weather(self, valid_weather: DataFrame, silver_matches: DataFrame):
        logger.info("Transforming weather data.")

        weather_columns = ["temp", "rhum", "prcp", "wspd"]

        # Convert NaN to NULL
        clean_weather = valid_weather.replace(
            float("nan"), None, subset=weather_columns
        )

        # Combine match date and kick-off into a timestamp
        match_times = silver_matches.select(
            "match_id",
            to_timestamp(
                concat(col("match_date"), lit(" "), col("kick_off"))
            ).alias("kickoff_time")
        )

        # Join weather observations with match times
        weather_with_kickoff = clean_weather.join(
            match_times,
            on="match_id",
            how="inner"
        )

        # Define time intervals
        during_match = (
            (col("time") >= col("kickoff_time")) &
            (col("time") <= col("kickoff_time") + expr("INTERVAL 2 HOURS"))
        )

        rain_window = (
            (col("time") >= col("kickoff_time") - expr("INTERVAL 2 HOURS")) &
            (col("time") <= col("kickoff_time") + expr("INTERVAL 2 HOURS"))
        )

        # Aggregate weather observations per match
        silver_weather = weather_with_kickoff.filter(rain_window).groupBy("match_id").agg(
            avg(when(during_match, col("temp"))).alias("avg_temp"),
            avg(when(during_match, col("rhum"))).alias("avg_rel_humidity"),
            avg(when(during_match, col("wspd"))).alias("avg_wind_speed"),
            spark_sum("prcp").alias("total_precipitation"),
            count(when(during_match, lit(1))).alias("weather_observations"),
            count("prcp").alias("precipitation_observations")
        )

        # Data quality after transformation
        logger.info(f"Weather records (after cleaning): {silver_weather.count()}")
        logger.info(f"Unique match IDs (after cleaning): {silver_weather.select('match_id').distinct().count()}")
        logger.info(f"Duplicate match IDs (after cleaning): {silver_weather.groupBy('match_id').count().filter(col('count') > 1).count()}")

        for column in ["avg_temp", "avg_rel_humidity", "avg_wind_speed", "total_precipitation"]:
            missing = silver_weather.filter(col(column).isNull()).count()
            logger.info(f"Missing values in {column}: {missing}")

        return silver_weather


    def save_silver_data(self, df: DataFrame, category: str):
        logger.info(f"Writing Silver data to parquet for saving of {category}.")

        df.write.mode("overwrite").parquet(
            f"{self.silver_dir}/{category}"
        )

        logger.info(f"Finished saving Silver data for {category}.")
    #endregion