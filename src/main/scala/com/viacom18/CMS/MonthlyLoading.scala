package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import java.util.concurrent.Executors

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, _}
import scala.util.control.NonFatal

object MonthlyLoading {

  def main(args: Array[String]) {
    try{
    val spark = SparkSession.builder.appName("CMSMonthlyLoading").enableHiveSupport().getOrCreate()

    /*..
/* ...
    val cmonth = args(0)
    val cyear = args(1)
    val pmonth = args(2)
    val pyear = args(3)
    val startDate = args(4).toString()
    val endDate = args(5).toString() */

    val cmonth = "05"
    val cyear = "2018"
    val pmonth = "04"
    val pyear = "2017"
    val startDate = "2018-05-01"
    val endDate ="2018-05-13"

    // Table loading for monthly views, watchtime and viewers media_id
    //..spark.sql(s""" ALTER TABLE CMS_MONTHLY SET TBLPROPERTIES('EXTERNAL'='FALSE') """)

    spark.sql(s""" ALTER TABLE CMS_MONTHLY DROP IF EXISTS PARTITION (year = '$cyear', month = '$cmonth') """)

    spark.sql(s""" ALTER TABLE CMS_MONTHLY DROP IF EXISTS PARTITION (year = '$pyear', month = '$pmonth') """)

    //..spark.sql(s""" ALTER TABLE CMS_MONTHLY SET TBLPROPERTIES('EXTERNAL'='TRUE') """)

    val viewAppDF = spark.sqlContext.sql(s""" SELECT 'APP' PROJECT, SUBSTRING(CAST(DATE_STAMP AS STRING),1,7) MONTH_NAME, V.MEDIA_ID, UPPER(M.REF_SERIES_TITLE) SHOW, COUNT(DISTINCT(CASE WHEN V.EVENT = ('mediaReady') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS, SUM(CASE WHEN V.EVENT = 'mediaReady' THEN 1 ELSE 0 END) AS NUM_VIEWS, SUM(CASE WHEN V.EVENT = 'Video Watched' AND (CAST (V.DURATION_SECONDS AS BIGINT) BETWEEN 0 AND 36000) THEN ABS(CAST(V.DURATION_SECONDS AS BIGINT)) ELSE 0 END)/60 CONTENT_DURATION_SEC FROM VOOT_APP_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID WHERE V.EVENT IN ('mediaReady', 'Video Watched') AND V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' GROUP BY SUBSTRING(CAST(V.DATE_STAMP AS STRING),1,7), V.MEDIA_ID, UPPER(M.REF_SERIES_TITLE) """)

    val viewWebDF = spark.sqlContext.sql(s""" SELECT 'WEB' PROJECT, SUBSTRING(CAST(DATE_STAMP AS STRING),1,7) MONTH_NAME, V.MEDIA_ID, UPPER(M.REF_SERIES_TITLE) SHOW, COUNT(DISTINCT(CASE WHEN V.EVENT = ('First Play') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS, SUM(CASE WHEN V.EVENT = 'First Play' THEN 1 ELSE 0 END) AS NUM_VIEWS, 0 AS CONTENT_DURATION_SEC FROM VOOT_WEB_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID WHERE V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' AND V.EVENT = ('First Play') GROUP BY SUBSTRING(CAST(V.DATE_STAMP AS STRING),1,7), V.MEDIA_ID, UPPER(M.REF_SERIES_TITLE) """)

    val viewFinalDF = viewAppDF.union(viewWebDF)

    viewFinalDF.createOrReplaceTempView("CMS_MONTHLY_TEMP")

    spark.sql(s""" INSERT INTO TABLE CMS_MONTHLY PARTITION(year = '$cyear', month = '$cmonth') SELECT * FROM CMS_MONTHLY_TEMP """)


    println("Load CMS_MONTHLY over ")

    //viewFinalDF.write.mode("overwrite").saveAsTable("CMS_MONTHLY")

    // Table loading for monthly viewers show
     //.. spark.sql(s""" ALTER TABLE CMS_VIEWERS_MONTHLY SET TBLPROPERTIES('EXTERNAL'='FALSE') """)

    spark.sql(s""" ALTER TABLE CMS_VIEWERS_MONTHLY DROP IF EXISTS PARTITION (year = '$cyear', month = '$cmonth') """)

    spark.sql(s""" ALTER TABLE CMS_VIEWERS_MONTHLY DROP IF EXISTS PARTITION (year = '$pyear', month = '$pmonth') """)

   //.. spark.sql(s""" ALTER TABLE CMS_VIEWERS_MONTHLY SET TBLPROPERTIES('EXTERNAL'='TRUE') """)

    val mainAppDF = spark.sqlContext.sql(s""" SELECT 'APP' PROJECT, SUBSTRING(CAST(DATE_STAMP AS STRING),1,7) MONTH_NAME, UPPER(M.REF_SERIES_TITLE) SHOW, M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW CLUSTER, COUNT(DISTINCT(CASE WHEN V.EVENT = ('mediaReady') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS FROM VOOT_APP_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU= M.SBU WHERE V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' AND EVENT = ('mediaReady') GROUP BY SUBSTRING(CAST(DATE_STAMP AS STRING),1,7), UPPER(M.REF_SERIES_TITLE), M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW """)

    val mainWebDF = spark.sqlContext.sql(s""" SELECT 'WEB' PROJECT, SUBSTRING(CAST(DATE_STAMP AS STRING),1,7) MONTH_NAME, UPPER(M.REF_SERIES_TITLE) SHOW, M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW CLUSTER, COUNT(DISTINCT(CASE WHEN V.EVENT = ('First Play') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS FROM VOOT_WEB_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU= M.SBU WHERE V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' AND EVENT = ('First Play') GROUP BY SUBSTRING(CAST(DATE_STAMP AS STRING),1,7), UPPER(M.REF_SERIES_TITLE), M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW """)

    val mainFinalDF = mainAppDF.union(mainWebDF)

    val mediaDF = spark.sqlContext.sql("SELECT UPPER(M.REF_SERIES_TITLE) SHOW, M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW CLUSTER, COUNT(DISTINCT M.ID) MEDIA_ID_COUNT FROM CONTENT_MAPPER M LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU = M.SBU GROUP BY UPPER(M.REF_SERIES_TITLE), M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW")

    val joinedDF = mainFinalDF.join(mediaDF, Seq("SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER"), "left").select("PROJECT", "MONTH_NAME", "SHOW", "SBU", "GENRE", "LANGUAGE", "CLUSTER", "NUM_VIEWERS", "MEDIA_ID_COUNT")

    joinedDF.createOrReplaceTempView("CMS_VIEWERS_MONTHLY_TEMP")

    spark.sql(s""" INSERT INTO TABLE CMS_VIEWERS_MONTHLY PARTITION(year = '$cyear', month = '$cmonth') SELECT * FROM CMS_VIEWERS_MONTHLY_TEMP """)

    println("Load CMS_VIEWERS_MONTHLY over ")


    //mainFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_MONTHLY")

    // Table loading for uppertotal monthly viewers show and media_id
    val upperAppDF = spark.sqlContext.sql("SELECT SUBSTRING(CAST(DATE_STAMP AS STRING),1,7) MONTH_NAME, COUNT(DISTINCT(CASE WHEN V.EVENT = ('mediaReady') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS FROM VOOT_APP_BASE V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU = M.SBU WHERE V.DATE_STAMP >= ADD_MONTHS(TRUNC(CURRENT_DATE,'MM'),-13) AND EVENT = ('mediaReady') GROUP BY SUBSTRING(CAST(DATE_STAMP AS STRING),1,7)")

    val upperWebDF = spark.sqlContext.sql("SELECT SUBSTRING(CAST(DATE_STAMP AS STRING),1,7) MONTH_NAME, COUNT(DISTINCT(CASE WHEN V.EVENT = ('First Play') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS FROM VOOT_WEB_BASE V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU = M.SBU WHERE V.DATE_STAMP >= ADD_MONTHS(TRUNC(CURRENT_DATE,'MM'),-13) AND EVENT = ('First Play') GROUP BY SUBSTRING(CAST(DATE_STAMP AS STRING),1,7)")

    val upperFinalDF = upperAppDF.union(upperWebDF)

    upperFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_MONTHLY_UPPERTOTAL")


    println("Load CMS_VIEWERS_MONTHLY_UPPERTOTAL over ")

    // Table loading for sidetotal monthly viewers show
    val sideAppDF = spark.sqlContext.sql("SELECT UPPER(M.REF_SERIES_TITLE) SHOW, M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW CLUSTER, COUNT(DISTINCT(CASE WHEN V.EVENT = ('mediaReady') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS FROM VOOT_APP_BASE V LEFT JOIN ADSALES_CONTENT_MAPPER M ON V.MEDIA_ID = M.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU= M.SBU WHERE V.DATE_STAMP >= ADD_MONTHS(TRUNC(CURRENT_DATE,'MM'),-13) AND EVENT = ('mediaReady') GROUP BY UPPER(M.REF_SERIES_TITLE), M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW")

    val sideWebDF = spark.sqlContext.sql("SELECT UPPER(M.REF_SERIES_TITLE) SHOW, M.SBU, M.GENRE, M.LANGUAGE,S.SBU_NEW CLUSTER, COUNT(DISTINCT(CASE WHEN V.EVENT = ('First Play') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS FROM VOOT_WEB_BASE V LEFT JOIN ADSALES_CONTENT_MAPPER M ON V.MEDIA_ID = M.ID LEFT JOIN ADSALES_SBU_MAPPER S ON S.SBU= M.SBU WHERE V.DATE_STAMP >= ADD_MONTHS(TRUNC(CURRENT_DATE,'MM'),-13) AND EVENT = ('First Play') GROUP BY UPPER(M.REF_SERIES_TITLE), M.SBU, M.GENRE, M.LANGUAGE, S.SBU_NEW")

    val sideFinalDF = sideAppDF.union(sideWebDF)

    sideFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_MONTHLY_SIDETOTAL")

    println("Load CMS_VIEWERS_MONTHLY_SIDETOTAL over ")


    */


    // Table loading for sidetotal monthly viewers media_id

    val sideAppDF1 = spark.sqlContext.sql("SELECT MEDIA_ID, CM.NAME AS EPISODE, UPPER(REF_SERIES_TITLE) AS SHOW, cm.CONTENT_TYPE, CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, CM.TELECAST_DATE, START_DATE, CM.GENRE, REGEXP_REPLACE(CM.CONTENT_SYNOPSIS, '\"', '') CONTENT_SYNOPSIS, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd')) TENURE, CM.LANGUAGE, count(distinct(distinct_id)) NUM_VIEWERS FROM voot_app_base FA LEFT JOIN CONTENT_MAPPER CM ON FA.MEDIA_ID = CM.ID where event = ('mediaReady') AND fa.DATE_STAMP >= ADD_MONTHS(TRUNC(CURRENT_DATE,'MM'),-13) GROUP BY MEDIA_ID, CM.NAME, UPPER(REF_SERIES_TITLE), cm.CONTENT_TYPE, CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, CM.TELECAST_DATE, START_DATE, CM.GENRE, CM.CONTENT_SYNOPSIS, CM.LANGUAGE")

    val sideWebDF1 = spark.sqlContext.sql("SELECT MEDIA_ID, CM.NAME AS EPISODE, UPPER(REF_SERIES_TITLE) AS SHOW, cm.CONTENT_TYPE, CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, CM.TELECAST_DATE, START_DATE, CM.GENRE, REGEXP_REPLACE(CM.CONTENT_SYNOPSIS, '\"', '') CONTENT_SYNOPSIS, DATEDIFF(CURRENT_DATE, DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(START_DATE, 'yyyy-MMM-dd HH:mm:ss')) ,'yyyy-MM-dd')) TENURE, CM.LANGUAGE, count(distinct(distinct_id)) NUM_VIEWERS  FROM voot_web_base FA LEFT JOIN CONTENT_MAPPER CM ON FA.MEDIA_ID = CM.ID where event = ('First Play') AND fa.DATE_STAMP >= ADD_MONTHS(TRUNC(CURRENT_DATE,'MM'),-13) GROUP BY MEDIA_ID, CM.NAME, UPPER(REF_SERIES_TITLE), cm.CONTENT_TYPE, CONTENT_FILE_NAME, CM.SBU, CONTENT_DURATION, CM.TELECAST_DATE, START_DATE, CM.GENRE, CM.CONTENT_SYNOPSIS, CM.LANGUAGE")

    val sideFinalDF1 = sideAppDF1.union(sideWebDF1)

    sideFinalDF1.write.mode("overwrite").saveAsTable("CMS_VIEWERS_MONTHLY_SIDETOTAL_MID")


    println("Load CMS_VIEWERS_MONTHLY_SIDETOTAL_MID over ")

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
