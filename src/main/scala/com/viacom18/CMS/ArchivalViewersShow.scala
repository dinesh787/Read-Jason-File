//PIVOT logic applied for Archival files for VIEWERS SHOW LEVEL
package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.control.NonFatal

object ArchivalViewersShow {

  def main(args: Array[String]) {

    try{

    val spark = SparkSession.builder.appName("CMSArchivalViewersShow").enableHiveSupport().getOrCreate()

    val startDate = args(0).      toString()
    val endDate = args(1).toString()

    val inputDF1 = spark.sqlContext.sql(s""" SELECT SHOW, SBU, GENRE, LANGUAGE, CLUSTER, DATE_STAMP, SUM(NUM_VIEWERS) VIEWERS FROM CMS_VIEWERS_ARCHIVAL WHERE DATE_STAMP >= '$startDate' AND DATE_STAMP <= '$endDate' GROUP BY SHOW, SBU, GENRE, LANGUAGE, CLUSTER, DATE_STAMP """)

    // apply logic
    val pivotDF1 = inputDF1.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER").pivot("DATE_STAMP").sum("VIEWERS").na.fill(0)

    val colsToSort = pivotDF1.columns.lastOption.mkString

    val finalDF = pivotDF1.orderBy(col(colsToSort).desc)

    // save the pivotDF1 output
    finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(args(2))

    val inputDF2 = spark.sqlContext.sql(s""" SELECT '' SHOW, '' SBU, '' GENRE, '' LANGUAGE, '' CLUSTER, DATE_STAMP, NUM_VIEWERS VIEWERS FROM CMS_VIEWERS_ARCHIVAL_UPPERTOTAL WHERE DATE_STAMP >= '$startDate' AND DATE_STAMP <= '$endDate' """)

    // apply logic
    val pivotDF2 = inputDF2.groupBy("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER").pivot("DATE_STAMP").sum("VIEWERS").na.fill(0)

    // save the pivotDF1 output
    pivotDF2.coalesce(1).write.format("csv").mode("overwrite").option("header", "false").save(args(3))

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