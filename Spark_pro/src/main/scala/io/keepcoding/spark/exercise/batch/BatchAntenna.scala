package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.streaming.StreamingAntennaJob.{readAntennaMetadata, spark}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, count, lit, max, min, when, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BatchAntenna extends Batch {

  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where(
        $"year" === filterDate.getYear && $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth && $"hour" === filterDate.getHour
      )

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
    antennaDF
      .join(metadataDF,
        antennaDF("id") === metadataDF("id")
      ).drop(metadataDF("id")) // Se quita uno de los ID para que no salgan duplicados en la tabla
  }

  override def lecturaMetricasBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"antenna_id", $"antenna_total_bytes")
      .groupBy($"antenna_total_bytes")

  }

  override def totalBytesEmail(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"id", $"email", $"bytes")
      .groupBy($"email", $"bytes").sum("bytes")
  }

  override def totalBytesApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"app", $"app_total_bytes")
      .groupBy($"app_total_bytes")

      .agg(approx_count_distinct($"id").as("antennas_num"))  // quiero que me cuente los reportes de cada id para agruparlos por error
      .select($"antennas_num", $"w.start".as("date"), $"model", $"version")
  }


  // No comprendo la parte de "Email de usuarios que han sobrepasado la cuota por hora", no especifíca ese máximo, para lo que habría que aplicar un condicional

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
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
  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .format("parquet")
      .save(s"$storageRootPath/historical")
  }

  // def main(args: Array[String]): Unit = {

  def main(args: Array[String]): Unit = {
    val argsTime = "2021-02-21T23:00:00Z"

    val storageDF = readFromStorage("/tmp/proyecto", OffsetDateTime.parse(argsTime))
    val metadataDF = readAntennaMetadata("jdbc:postgresql://$IpServer:5432/postgres", "metadata", "postgress", "keepcoding")
    val enrichDF = enrichAntennaWithMetadata(storageDF, metadataDF)

    writeToJdbc(lecturaMetricasBytes(enrichDF), "jdbc:postgresql://$IpServer:5432/postgres", "antenna_sum", "postgress", "keepcoding")
    writeToJdbc(totalBytesEmail(enrichDF), "jdbc:postgresql://$IpServer:5432/postgres", "antenna_sum", "postgress", "keepcoding")
    writeToJdbc(totalBytesApp(enrichDF), "jdbc:postgresql://$IpServer:5432/postgres", "antenna_sum", "postgress", "keepcoding")

    writeToStorage(storageDF, "/tmp/proyecto/")

  }
}
