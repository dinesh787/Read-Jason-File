//PIVOT logic applied for Archival files for VIEWERS MEDIA_ID

package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal

object ArchivalViewersMediaId {

  def main(args: Array[String]) {

    try{

    // create Spark context with Spark configuration
    val spark = SparkSession.builder.appName("CMSArchivalViewersMediaID").enableHiveSupport().getOrCreate()

    val startDate = args(0).toString()
    val endDate = args(1).toString()

    val inputDF1 = spark.sqlContext.sql(s""" SELECT MEDIA_ID, CM.NAME AS EPISODE, REF_SERIES_TITLE AS SHOW, CONTENT_TYPE, CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, CM.TELECAST_DATE, START_DATE, CM.GENRE, REGEXP_REPLACE(CM.CONTENT_SYNOPSIS, '\"', '') CONTENT_SYNOPSIS, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd')) TENURE, CM.LANGUAGE, DATE_STAMP, SUM(NUM_VIEWERS) VIEWERS FROM CMS_ARCHIVAL FA LEFT JOIN CONTENT_MAPPER CM ON FA.MEDIA_ID = CM.ID WHERE FA.DATE_STAMP >= '$startDate' AND FA.DATE_STAMP <= '$endDate' GROUP BY MEDIA_ID, CM.NAME, REF_SERIES_TITLE, CONTENT_TYPE, CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, CM.TELECAST_DATE, START_DATE, CM.GENRE, CM.CONTENT_SYNOPSIS, CM.LANGUAGE, DATE_STAMP """)

    // apply logic of Pivot
    val pivotDF = inputDF1.groupBy("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE").pivot("DATE_STAMP").sum("VIEWERS").na.fill(0)

    val colsToSort = pivotDF.columns.lastOption.mkString

    val finalDF = pivotDF.orderBy(col(colsToSort).desc)

    // save the finalDF output
    finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(args(2))

    val inputDF2 = spark.sqlContext.sql(s""" SELECT '' MEDIA_ID, '' EPISODE, '' SHOW, '' CONTENT_TYPE, '' CONTENT_FILE_NAME, '' SBU, '' CONTENT_DURATION, '' TELECAST_DATE, '' START_DATE, '' GENRE, '' CONTENT_SYNOPSIS, '' LANGUAGE, '' TENURE, DATE_STAMP, NUM_VIEWERS VIEWERS FROM CMS_VIEWERS_ARCHIVAL_UPPERTOTAL WHERE DATE_STAMP >= '$startDate' AND DATE_STAMP <= '$endDate' """)

    // apply logic
    val pivotDF2 = inputDF2.groupBy("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE").pivot("DATE_STAMP").sum("VIEWERS").na.fill(0)

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
