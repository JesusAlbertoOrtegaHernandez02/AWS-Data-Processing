# consumo_medio_de_vehiculos.py
import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, avg, count, min as spark_min, max as spark_max
from awsglue.dynamicframe import DynamicFrame

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    args = getResolvedOptions(sys.argv, ["database", "table", "output_path"])
    database = args["database"]
    table = args["table"]
    output_path = args["output_path"]

    logger.info(f"Database: {database}, Table: {table}")
    logger.info(f"Output path: {output_path}")

    sc = SparkContext()
    glueContext = GlueContext(sc)

    # Leer desde Glue Catalog
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )
    df = dyf.toDF()

    logger.info("Schema de entrada:")
    df.printSchema()

    # Filtros de seguridad
    df = df.filter(col("vehicle_id").isNotNull())
    df = df.filter(col("speed").isNotNull())
    df = df.filter(col("speed_range").isNotNull())

    # Agregación por vehículo y rango de velocidad
    out_df = (
        df.groupBy("vehicle_id", "speed_range")
          .agg(
              count("*").alias("num_records"),
              avg("speed").alias("avg_speed"),
              spark_min("speed").alias("min_speed"),
              spark_max("speed").alias("max_speed"),
              avg("fuel_level").alias("avg_fuel_level"),
          )
          .orderBy("vehicle_id", "speed_range")
    )

    logger.info(f"Filas agregadas: {out_df.count()}")

    out_dyf = DynamicFrame.fromDF(out_df, glueContext, "out_dyf")

    glueContext.write_dynamic_frame.from_options(
        frame=out_dyf,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["speed_range"]
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )

    logger.info("ETL vehicle-speed-range-aggregation completado OK")

if __name__ == "__main__":
    main()
