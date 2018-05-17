//DAILY  files for VIEWERS AT SHOW LEVEL WITH PIVOT LOGIC
package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.control.NonFatal

object DailyViewersShow {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("CMSDailyViewersShow").enableHiveSupport().getOrCreate()

    try{
    val inputDF1 = spark.sqlContext.sql("SELECT SHOW, SBU, GENRE, LANGUAGE, CLUSTER, MEDIA_ID_COUNT, DATE_STAMP, SUM(NUM_VIEWERS) VIEWERS FROM CMS_VIEWERS_DAILY GROUP BY SHOW, SBU, GENRE, LANGUAGE, CLUSTER, MEDIA_ID_COUNT, DATE_STAMP")

    val pivotDF1 = inputDF1.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER", "MEDIA_ID_COUNT").pivot("DATE_STAMP").sum("VIEWERS").na.fill(0)

    val colsToSort = pivotDF1.columns.lastOption.mkString

    val finalDF = pivotDF1.orderBy(col(colsToSort).desc)

    // save the pivotDF1 output
    finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(args(0))

    val inputDF2 = spark.sqlContext.sql("SELECT '' SHOW, '' SBU, '' GENRE, '' LANGUAGE, '' CLUSTER, '' MEDIA_ID_COUNT, DATE_STAMP, NUM_VIEWERS VIEWERS FROM CMS_VIEWERS_DAILY_UPPERTOTAL")

    // apply logic
    val pivotDF2 = inputDF2.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER", "MEDIA_ID_COUNT").pivot("DATE_STAMP").sum("VIEWERS").na.fill(0)

    // save the pivotDF1 output
    pivotDF2.coalesce(1).write.format("csv").mode("overwrite").option("header", "false").save(args(1))

    spark.stop()
    } catch   {
      case NonFatal(t) =>
        println("************************-----------ERROR---------***************************")
        println("--")
        println("--")
        println(t.toString)

    }
  }
}