import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, TimestampType
from pyspark.sql.window import Window


class Analyzer:
    def __init__(self, _spark_session: SparkSession):
        """
        :param _spark_session: SparkSession instance to use for analyzing the data.
        """
        self.spark_session: SparkSession = _spark_session
        self.data: DataFrame = None
        self.top_songs: DataFrame = None

    def load_data(self, path: str, input_schema: StructType):
        """
        Loads data from a CSV file.

        :param path: Path to the CSV file to load.
        :param input_schema: StructType schema to use for interpreting the data.
        """
        self.data = self.spark_session.read.format("csv") \
            .option("header", "false") \
            .option("delimiter", "\t") \
            .option("inferSchema", "false") \
            .schema(input_schema) \
            .load(path)

    def analyze(self, song_gap_minutes: int, n_top_session: int, n_top_songs: int):
        """
        Analyzes the loaded data and calculates the top songs by count played in the top sessions.

        :param song_gap_minutes: Number of minutes between song start times to consider them as the same session.
        :param n_top_session: Number of top sessions to consider when determining the top songs.
        :param n_top_songs: Number of top songs to include in the output.
        """
        _session_window = Window.partitionBy("user").orderBy("timestamp")
        self.data = self.data.withColumn("prev_timestamp", lag("timestamp", 1).over(_session_window))
        self.data = self.data.withColumn("session_id", sum(when(col("timestamp").cast("long") - col("prev_timestamp").cast("long") > song_gap_minutes * 60, 1).otherwise(0)).over(_session_window))

        _session_count_window = Window.partitionBy("user", "session_id").orderBy("timestamp")
        self.data = self.data.withColumn("song_count", count("*").over(_session_count_window))
        self.data = self.data.filter(col("song_count") > 1)

        _session_window = Window.partitionBy("user", "session_id").orderBy("timestamp")
        self.data = self.data.withColumn("track_count", count("*").over(_session_window))
        _session_window = Window.partitionBy("user").orderBy(desc("track_count"), desc("timestamp"))
        _top_50_sessions = self.data.withColumn("rank", dense_rank().over(_session_window)).filter(col("rank") <= n_top_session)

        self.top_songs = _top_50_sessions.groupBy("artist_name", "track_name").agg(count("*").alias("play_count")).orderBy(desc("play_count"), "artist_name", "track_name").limit(n_top_songs)

    def write_output(self, path: str):
        """
        Writes the top songs data to a TSV file.

        :param path: Path to the output TSV file.
        """
        pandas_df = self.top_songs.toPandas()
        pandas_df.to_csv(path, sep="\t", index=False, mode="w")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger("lastfm-analyzer-app")

    logger.info("Getting path from environment variables...")
    input_path = os.environ.get("INPUT_PATH")
    output_path = os.environ.get("OUTPUT_PATH")
    logger.info(f"Input path: {input_path}")
    logger.info(f"Output path: {output_path}")

    logger.info("Creating Spark session...")
    spark_session = SparkSession.builder \
        .appName("lastfm-analyzer-app") \
        .getOrCreate()

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("musicbrainz_trackid", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("musicbrainz-track-id", StringType(), True),
        StructField("track_name", StringType(), True)
    ])

    logger.info("Starting analysis...")
    analyzer = Analyzer(spark_session)
    logger.info("Loading data...")
    analyzer.load_data(path=input_path, input_schema=schema)
    logger.info("Analyzing data...")
    analyzer.analyze(song_gap_minutes=20, n_top_session=50, n_top_songs=10)
    logger.info("Writing output...")
    analyzer.write_output(path=output_path)
    logger.info(f"Done analyzing data! Top songs:\n{analyzer.top_songs.show()}")
    logger.info("Cleaning up...")
    spark_session.stop()
    logger.info("Done!")
