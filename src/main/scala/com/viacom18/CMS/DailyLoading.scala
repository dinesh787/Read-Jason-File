//DATA LOADING FILES FOR VIEWS AND WATCHTIME AT MEDIA_ID LEVEL
package com.viacom18.CMS

import org.apache.spark.sql.SparkSession
import scala.util.control.NonFatal

object DailyLoading {

  def main(args: Array[String]) {

    try{

    val spark = SparkSession.builder.appName("CMSDailyLoading").enableHiveSupport().getOrCreate()

    // Table loading for daily views, watchtime and viewers media id
    val viewAppDF = spark.sqlContext.sql("SELECT 'APP' PROJECT, DATE_FORMAT(V.DATE_STAMP,'yyyy-MM-dd') DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE SHOW, COUNT(DISTINCT(CASE WHEN V.EVENT = ('mediaReady') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS, SUM(CASE WHEN V.EVENT = 'mediaReady' THEN 1 ELSE 0 END) AS NUM_VIEWS, SUM(CASE WHEN  V.EVENT = 'Video Watched' AND (CAST (DURATION_SECONDS  AS BIGINT) BETWEEN 0 AND 36000) THEN ABS(CAST(V.DURATION_SECONDS AS BIGINT)) ELSE 0 END)/60 CONTENT_DURATION_SEC FROM VOOT_APP_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID WHERE V.DATE_STAMP >= DATE_SUB(CURRENT_DATE,60) AND V.EVENT IN ('mediaReady', 'Video Watched') GROUP BY V.DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE")

    val viewWebDF = spark.sqlContext.sql("SELECT 'WEB' PROJECT, DATE_FORMAT(V.DATE_STAMP,'yyyy-MM-dd') DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE SHOW, COUNT(DISTINCT(CASE WHEN V.EVENT = ('First Play') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS, SUM(CASE WHEN V.EVENT = 'First Play' THEN 1 ELSE 0 END) AS NUM_VIEWS, 0 AS CONTENT_DURATION_SEC FROM VOOT_WEB_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID WHERE V.DATE_STAMP >= DATE_SUB(CURRENT_DATE,60) AND V.EVENT = ('First Play') GROUP BY V.DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE")

    val viewFinalDF = viewAppDF.union(viewWebDF)

    viewFinalDF.write.mode("overwrite").saveAsTable("CMS_DAILY")

    // Table loading for daily views, watchtime and viewers media id
    val mainAppDF = spark.sqlContext.sql("Select 'APP' Project, DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp, m.ref_series_title Show, m.SBU, m.GENRE, m.language, s.sbu_new cluster, count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)) NUM_VIEWERS, COUNT(DISTINCT M.ID) MEDIA_ID_COUNT FROM voot_app_base_event v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU = m.SBU where event = ('mediaReady') and v.date_stamp >= DATE_SUB(CURRENT_DATE,60) group by date_stamp, m.ref_series_title,m.SBU, m.GENRE, m.language,s.sbu_new")

    val mainWebDF = spark.sqlContext.sql("Select 'WEB' Project, DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp , m.ref_series_title Show, m.SBU, m.GENRE, m.language, s.sbu_new cluster, count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)) NUM_VIEWERS, COUNT(DISTINCT M.ID) MEDIA_ID_COUNT FROM voot_web_base_event v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU = m.SBU where event = ('First Play') and v.date_stamp >= DATE_SUB(CURRENT_DATE,60) group by date_stamp,m.ref_series_title,m.SBU, m.GENRE, m.language,s.sbu_new")

    val mainFinalDF = mainAppDF.union(mainWebDF)

    //spark.sqlContext.sql("CREATE TABLE IF NOT EXISTS CMS_VIEWERS_DAILY(PROJECT STRING, DATE_STAMP TIMESTAMP, SHOW STRING, SBU STRING, GENRE STRING, LANGUAGE STRING, CLUSTER STRING, NUM_VIEWERS DOUBLE)")

    mainFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_DAILY")

    val upperAppDF = spark.sqlContext.sql("Select DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp, count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)) NUM_VIEWERS FROM voot_app_base v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU= m.SBU where  event = ('mediaReady') and v.date_stamp >= DATE_SUB(CURRENT_DATE,60) group by date_stamp")

    val upperWebDF = spark.sqlContext.sql("Select DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp, count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)) NUM_VIEWERS FROM voot_web_base v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU= m.SBU where  event = ('First Play') and v.date_stamp >= DATE_SUB(CURRENT_DATE,60) group by date_stamp")

    val upperFinalDF = upperAppDF.union(upperWebDF)

    //spark.sqlContext.sql("CREATE TABLE IF NOT EXISTS CMS_VIEWERS_DAILY_UPPERTOTAL(DATE_STAMP TIMESTAMP, NUM_VIEWERS DOUBLE)")

    upperFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_DAILY_UPPERTOTAL")

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
