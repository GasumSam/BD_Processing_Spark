package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

import scala.concurrent.duration.Duration

object StreamingAntennaJob extends StreamingClass {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Streaming Job")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val schema = ScalaReflection.schemaFor[AntennaSource].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json($"bytes".cast(StringType), schema).as("json"))
      .select("json.*")
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as("antenna")
      .join(
        metadataDF.as("metadata"),
        $"antenna.id" === $"metadata.id"
      ).drop($"metadata.id")
  }

//Total de bytes recibidos antenna id app
  override def lecturaMetricasBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withWatermark("timestamp", "5 minutes")
      .select($"id", $"antenna_id", $"bytes", $"app")
      .groupBy($"antenna_id").sum("bytes").as("antenna_total_bytes")
      .groupBy($"id").sum("bytes").as("id_total_bytes")
      .groupBy($"app").sum("bytes").as("app_total_bytes")
      .select("antenna_total_bytes", "id_total_bytes", "app_total_bytes")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }


  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}")
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {

    val metadataDF = readAntennaMetadata("jdbc:postgresql://$IpServer:5432/postgres", "metadata", "postgress", "keepcoding")
    val antennaStreamDF = parserJsonData(readFromKafka("34.67.119.168:9092", "antenna_data"))

    val writeToStorageFut = writeToStorage(antennaStreamDF, "/tmp/proyecto")
    val writeToJdbcFut = writeToJdbc(
      lecturaMetricasBytes(antennaStreamDF, metadataDF),
      "jdbc:postgresql://35.225.198.88:5432/postgres", "antenna_sum", "postgres", "keepcoding"
    )

    val f = Future.sequence(Seq(writeToStorageFut, writeToJdbcFut))
    Await.result(f, Duration.Inf)
  }
}