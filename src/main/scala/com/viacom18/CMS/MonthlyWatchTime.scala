//DAILY  files for WATCHTIME WITH PIVOT LOGIC
package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal

object MonthlyWatchTime {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("CMSMonthlyWatchTime").enableHiveSupport().getOrCreate()

    import spark.sqlContext.implicits._

    try{

    //val inputDF = spark.sqlContext.sql("SELECT MEDIA_ID,CM.NAME AS EPISODE, REF_SERIES_TITLE AS SHOW,CONTENT_TYPE,CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, TELECAST_DATE, START_DATE,GENRE, REGEXP_REPLACE(CM.CONTENT_SYNOPSIS, '\"', '') CONTENT_SYNOPSIS, CM.LANGUAGE, DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd')) TENURE, SUM(FA.CONTENT_DURATION_SEC) WATCHTIME FROM CMS_MONTHLY FA LEFT JOIN CONTENT_MAPPER CM ON FA.MEDIA_ID = CM.ID GROUP BY MEDIA_ID,CM.NAME, REF_SERIES_TITLE, CONTENT_TYPE,CONTENT_FILE_NAME, MONTH_NAME,SBU,CONTENT_DURATION,TELECAST_DATE,START_DATE,GENRE, CM.CONTENT_SYNOPSIS,CM.LANGUAGE,DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd'))")
    val inputDF = spark.sqlContext.sql("SELECT MO.MEDIA_ID, CM.NAME AS EPISODE, CM.REF_SERIES_TITLE AS SHOW, CM.CONTENT_TYPE, CM.CONTENT_FILE_NAME, CM.SBU, CM.CONTENT_DURATION, CM.TELECAST_DATE, CM.START_DATE, CM.GENRE, REGEXP_REPLACE(CM.CONTENT_SYNOPSIS, '\"', '') CONTENT_SYNOPSIS, CM.LANGUAGE, DATE_FORMAT(CONCAT(MO.MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CM.START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd')) TENURE, SUM(MO.CONTENT_DURATION_SEC) WATCHTIME FROM CMS_MONTHLY MO LEFT JOIN CONTENT_MAPPER CM ON MO.MEDIA_ID = CM.ID GROUP BY MO.MEDIA_ID, CM.NAME, CM.REF_SERIES_TITLE, CM.CONTENT_TYPE, CM.CONTENT_FILE_NAME, MO.MONTH_NAME, CM.SBU, CM.CONTENT_DURATION, CM.TELECAST_DATE, CM.START_DATE, CM.GENRE, CM.CONTENT_SYNOPSIS, CM.LANGUAGE, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CM.START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd'))")

    // apply pivot logic
    val pivotDF = inputDF.groupBy("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE").pivot("MONTH_NAME").sum("WATCHTIME").na.fill(0)

    val colsToSum1 = pivotDF.columns.slice(13, pivotDF.columns.length)

    val colsToSort = pivotDF.columns.lastOption.mkString

    val sumRowDF = pivotDF.withColumn("TOTAL",colsToSum1.map(col).reduce((c1, c2) => c1 + c2)).orderBy(col(colsToSort).desc)

    val rightColsList = colsToSum1.toList

    val leftColsList = List("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE", "TOTAL")

    val allColsList = leftColsList.union(rightColsList)

    val finalDF = sumRowDF.select(allColsList.map(col): _*)

    // save the finalDF output
    finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(args(0))

    val colsToSum2 = finalDF.columns.slice(13, finalDF.columns.length)

    val sumColDF = finalDF.groupBy().sum(colsToSum2: _*)

    val emptyDF = Seq.empty[(String, String, String, String, String, String, String, String, String, String, String, String, String)].toDF("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE")

    val emptyDF1 = emptyDF.withColumn("id", lit(1))

    val sumColDF1 = sumColDF.withColumn("id", lit(1))

    val joinedDF = emptyDF1.join(sumColDF1, Seq("id"), "outer").drop("id")

    // save the joinedDF output
    joinedDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "false").save(args(1))

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
