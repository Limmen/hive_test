package limmen.github.com.hive_test

import org.apache.log4j.{ Level, LogManager, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.{ SparkConf, SparkContext }
import org.rogach.scallop.ScallopConf

/**
 * Parser of command-line arguments
 */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val hive = opt[String](required = false, descr = "hive config")
  verify()
}

object Main {

  def main(args: Array[String]): Unit = {

    // Setup logging
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info(s"Starting Hive Test")

    //Parse cmd arguments
    val conf = new Conf(args)

    // Setup Spark
    val sparkConf = sparkClusterSetup()

    log.info("Creating Spark Session")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    log.info("Spark Session created")

    val sc = spark.sparkContext

    val clusterStr = sc.getConf.toDebugString
    log.info(s"Cluster settings: \n" + clusterStr)

    import spark.implicits._

    //val dbs = spark.sql("show databases")
    //dbs.show()
    //log.info(dbs.show())
    spark.sql("use test__featurestore")
    val tbls = spark.sql("show tables")
    log.info(tbls.show())
    log.info("Inserting 1 into fg1_1")
    spark.sql("INSERT INTO TABLE fg1__1 VALUES (1.0)")
    log.info("INSERT complete")
    val rows = spark.sql("SELECT * FROM fg1__1")
    log.info(rows.show())
    log.info("saving as table")
    //rows.write.saveAsTable("test_hive")
    rows.write.mode("overwrite").format("orc").saveAsTable("test_hive")
    log.info("table saved")
    val rows2 = spark.sql("SELECT * FROM test_hive")
    log.info()
    log.info(rows2.show())

    //Close
    log.info("Shutting down spark job")
    spark.close
  }

  /**
   * Hard coded settings for cluster spark training
   *
   * @return spark configuration
   */
  def sparkClusterSetup(): SparkConf = {
    new SparkConf()
      .setAppName("hive_test")
      .set("spark.executor.heartbeatInterval", "20s")
      .set("spark.rpc.message.maxSize", "512")
      .set("spark.kryoserializer.buffer.max", "1024")
      .set("hive.metastore.uris", "thrift://10.0.2.15:9083")
    //.set("hive.metastore.warehouse.dir", "hdfs://10.0.2.15:8020/apps/hive/warehouse/test__featurestore.db/")
  }
}
