from pipeline import Pipeline
from pyspark.sql.functions import col
import os

pipeline = Pipeline(data_dir=os.getenv("STATSBOMB_DATA_DIR"), shared_dir=os.getenv("DAT535_OUTPUT_DIR"))

#region Bronze
def bronze(category: str = None):
    print("="*60)
    print("-"*3, " Bronze Layer ", "-"*3)
    print("="*60)
    print("Initializing spark.")
    pipeline.create_spark_session()
    categories = ["competitions", "matches", "events"]

    if category is None:
        for category in categories:
            print(f"Ingesting data for {category}.")
            df = pipeline.ingest_data(category)
            if category != "events":
                df.cache()

            if category == "matches":
                print(f"Fetching weatherdata from Meteostat.")
                weather_df = pipeline.fetch_weather_data(df)
                pipeline.save_compressed_data(weather_df, "weather")
                print(f"Saved data to Parquet for weather with size {weather_df.count()}.")
        
            pipeline.save_compressed_data(df, category)
            print(f"Finished saving data to Parquet for {category} with size {df.count()}.")
            df.unpersist()

    elif category not in categories:
        print("="*60)
        print("Please provide a valid category from these choices [competitions, matches, events].")
        print("="*60)

    else:
        print(f"Ingesting data for {category}.")
        df = pipeline.ingest_data(category)
        if category != "events":
            df.cache()
            
        if category == "matches":
            print(f"Fetching weatherdata from Meteostat.")
            weather_df = pipeline.fetch_weather_data(df)
            pipeline.save_compressed_data(weather_df, "weather")
            print(f"Saved data to Parquet for weather with size {weather_df.count()}.")
    
        pipeline.save_compressed_data(df, category)
        print(f"Finished saving data to Parquet for {category} with size {df.count()}.")
        df.unpersist()
#endregion

#region Silver
def silver():
    print("="*60)
    print("-"*3, " Silver Layer ", "-"*3)
    print("="*60)
    print("Initializing spark.")
    pipeline.create_spark_session()

    # Matches
    print("Loading Bronze data for matches.")
    matches_df = pipeline.load_silver_input("matches")

    print("Filtering La Liga matches.")
    laliga_matches = matches_df.filter(
        col("competition.competition_name") == "La Liga"
    )

    print("Validating matches.")
    valid_matches, quarantine_matches = pipeline.validate_matches(laliga_matches)

    print("Transforming matches.")
    silver_matches = pipeline.transform_matches(valid_matches)

    # Events
    print("Loading Bronze data for events.")
    events_df = pipeline.load_silver_input("events")

    print("Filtering La Liga events.")
    laliga_events = events_df.join(
        laliga_matches.select("match_id"),
        on="match_id",
        how="left_semi"
    )

    print("Validating events.")
    valid_events, quarantine_events = pipeline.validate_events(
        laliga_events, silver_matches
    )

    print("Transforming events.")
    silver_events = pipeline.transform_events(valid_events)

    # Weather
    print("Loading Bronze data for weather.")
    weather_df = pipeline.load_silver_input("weather")

    print("Filtering La Liga weather data.")
    laliga_weather = weather_df.join(
        laliga_matches.select("match_id"),
        on="match_id",
        how="left_semi"
    )

    print("Validating weather data.")
    valid_weather, quarantine_weather = pipeline.validate_weather(
        laliga_weather, silver_matches
    )

    print("Transforming weather data.")
    silver_weather = pipeline.transform_weather(
        valid_weather, silver_matches
    )

    # Join weather with matches
    print("Joining weather data with matches.")
    silver_matches_weather = silver_matches.join(
        silver_weather,
        on="match_id",
        how="left"
    )

    print("Number of matches before join:", silver_matches.count())
    print("Number of matches after join:", silver_matches_weather.count())
    
    # Save Silver data
    print("Saving matches to Parquet.")
    pipeline.save_silver_data(silver_matches_weather, "matches")
    print("Finished saving data to Parquet for matches.")

    print("Saving events to Parquet.")
    pipeline.save_silver_data(silver_events, "events")
    print("Finished saving data to Parquet for events.")

    print("Saving weather to Parquet.")
    pipeline.save_silver_data(silver_weather, "weather")
    print("Finished saving data to Parquet for weather.")

    # Save quarantined records
    print("Saving quarantined matches to Parquet.")
    pipeline.save_silver_data(quarantine_matches, "quarantine/matches")
    print("Finished saving quarantined matches.")

    print("Saving quarantined events to Parquet.")
    pipeline.save_silver_data(quarantine_events, "quarantine/events")
    print("Finished saving quarantined events.")

    print("Saving quarantined weather to Parquet.")
    pipeline.save_silver_data(quarantine_weather, "quarantine/weather")
    print("Finished saving quarantined weather.")

    print("="*60)
    print("Finished processing Silver Layer.")
    print("="*60)
#endregion

bronze()
silver()