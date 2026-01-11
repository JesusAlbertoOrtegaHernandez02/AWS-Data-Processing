import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, avg, to_date
from awsglue.dynamicframe import DynamicFrame


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():

    args = getResolvedOptions(sys.argv, ['database', 'table', 'output_path'])
    database = args['database']
    table = args['table']
    output_path = args['output_path']

    logger.info(f"Database: {database}, Table: {table}, Output: {output_path}")

    sc = SparkContext()
    glueContext = GlueContext(sc)

    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )

    df = dynamic_frame.toDF()
    logger.info(f"Registros leídos: {df.count()}")


    df = df.withColumn("day", to_date(col("timestamp")))


    daily_df = df.groupBy("vehicle_id", "day").agg(
        avg("speed").alias("avg_speed"),
        avg("fuel_level").alias("avg_fuel_level")
    ).orderBy("vehicle_id", "day")

    logger.info(f"Registros agregados: {daily_df.count()}")

    output_dynamic_frame = DynamicFrame.fromDF(
        daily_df, glueContext, "output"
    )


    glueContext.write_dynamic_frame.from_options(
        frame=output_dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["day"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )

    logger.info("ETL diario completado correctamente")

if __name__ == "__main__":
    main()
