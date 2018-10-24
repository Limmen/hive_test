from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
import logging

def setup_logging():
    logger = logging.getLogger("py4j")
    logger.removeHandler(logger.handlers[0])
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)
    return logger


def spark_config():
    conf = SparkConf().setAppName("hive_test").set("hive.metastore.uris", "thrift://10.0.2.15:9083")
    return conf

def main():
    conf = spark_config()
    spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    logger = setup_logging()
    logger.info("SparkSession created and logger initialized")
    logger.info("Tables:")
    tables = spark.sql("show tables")
    logger.info(tables.show())

if __name__ == '__main__':
    main()