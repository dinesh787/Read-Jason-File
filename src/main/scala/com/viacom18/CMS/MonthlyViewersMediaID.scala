package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal

object MonthlyViewersMediaID {

  def main(args: Array[String]): Unit = {

    try{
    val file1 ="/cms/mviewers_mid/1"
    val file2 = "/cms/mviewers_mid/2"
    val spark = SparkSession.builder.appName("CMSMonthlyViews").enableHiveSupport().getOrCreate()
    val inputDF1 = spark.sqlContext.sql("SELECT MO.MEDIA_ID, CM.NAME AS EPISODE, upper(SHOW) as SHOW, CM.CONTENT_TYPE, CM.CONTENT_FILE_NAME, S.SBU, CM.CONTENT_DURATION, CM.TELECAST_DATE, CM.START_DATE, CM.GENRE, REGEXP_REPLACE(CM.CONTENT_SYNOPSIS, '\"', '') CONTENT_SYNOPSIS, CM.LANGUAGE, DATE_FORMAT(CONCAT(MO.MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CM.START_DATE, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) TENURE, SUM(MO.NUM_VIEWERS) VIEWERS FROM CMS_MONTHLY MO LEFT JOIN CONTENT_MAPPER CM ON MO.MEDIA_ID = CM.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU= CM.SBU  GROUP BY MO.MEDIA_ID, CM.NAME, upper(SHOW), CM.CONTENT_TYPE, CM.CONTENT_FILE_NAME, MO.MONTH_NAME, S.SBU, CM.CONTENT_DURATION, CM.TELECAST_DATE, CM.START_DATE, CM.GENRE, CM.CONTENT_SYNOPSIS, CM.LANGUAGE, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(CM.START_DATE, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd'))")
    inputDF1.createOrReplaceTempView("inputDF1")
    val inputDF2 = spark.sql("SELECT MEDIA_ID, EPISODE,upper(SHOW) SHOW, CONTENT_TYPE, CONTENT_FILE_NAME, SBU, CONTENT_DURATION, TELECAST_DATE, START_DATE, GENRE, CONTENT_SYNOPSIS, TENURE, LANGUAGE, SUM(NUM_VIEWERS) TOTAL FROM CMS_VIEWERS_MONTHLY_SIDETOTAL_MID2 GROUP BY MEDIA_ID, EPISODE, upper(SHOW), CONTENT_TYPE, CONTENT_FILE_NAME, SBU, CONTENT_DURATION, TELECAST_DATE, START_DATE, GENRE, CONTENT_SYNOPSIS, TENURE, LANGUAGE")
    inputDF2.createOrReplaceTempView("inputDF2")

    val joinedDF = spark.sqlContext.sql("""
select
a.MEDIA_ID, upper(a.EPISODE) as EPISODE, upper(a.SHOW) SHOW,
a.CONTENT_TYPE, a.CONTENT_FILE_NAME,
a.SBU, a.CONTENT_DURATION, a.TELECAST_DATE,
a.START_DATE, a.GENRE, a.CONTENT_SYNOPSIS,
 a.TENURE, a.LANGUAGE,
 Sum(b.TOTAL) as TOTAL, a.MONTH_NAME,
  a.VIEWERS
  from inputDF1 a left join inputDF2 b on a.media_id = b.media_id
  group by a.MONTH_NAME , a.media_id ,upper( a.EPISODE),upper(a.SHOW),
a.CONTENT_TYPE, a.CONTENT_FILE_NAME,
a.SBU, a.CONTENT_DURATION, a.TELECAST_DATE,
a.START_DATE, a.GENRE, a.CONTENT_SYNOPSIS,
 a.TENURE, a.LANGUAGE, a.VIEWERS """)

    val pivotDF1 = joinedDF.groupBy("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE", "TOTAL").pivot("MONTH_NAME").sum("VIEWERS").na.fill(0)


    val colsToSort = pivotDF1.columns.lastOption.mkString

    val finalDF = pivotDF1.orderBy(col(colsToSort).desc)

    // save the finalDF output
    finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(file1)

    val inputDF3 = spark.sql("SELECT '' MEDIA_ID, '' EPISODE, '' SHOW, '' CONTENT_TYPE, '' CONTENT_FILE_NAME, '' SBU, '' CONTENT_DURATION, '' TELECAST_DATE, '' START_DATE, '' GENRE, '' CONTENT_SYNOPSIS, '' LANGUAGE, '' TENURE, '' TOTAL, DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') MONTH_NAME, NUM_VIEWERS VIEWERS FROM CMS_VIEWERS_MONTHLY_UPPERTOTAL1 where DATE_FORMAT(CONCAT(MONTH_NAME,'-','01'), 'yyyy-MM') not in('2017-04') ")

    val pivotDF2 = inputDF3.groupBy("MEDIA_ID", "EPISODE", "SHOW", "CONTENT_TYPE", "CONTENT_FILE_NAME", "SBU", "CONTENT_DURATION", "TELECAST_DATE", "START_DATE", "GENRE", "CONTENT_SYNOPSIS", "LANGUAGE", "TENURE", "TOTAL").pivot("MONTH_NAME").sum("VIEWERS").na.fill(0)

    // save the pivotDF2 output
    pivotDF2.coalesce(1).write.format("csv").mode("overwrite").option("header", "false").save(file2)

} catch   {
  case NonFatal(t) =>
    println("************************-----------ERROR---------***************************")
    println("--")
    println("--")
    println(t.toString)

}


  }
  }