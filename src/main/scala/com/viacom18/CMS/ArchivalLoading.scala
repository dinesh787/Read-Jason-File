//Archival file loading for month - 2 days data

package com.viacom18.CMS
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession

object ArchivalLoading {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("CMSArchivalLoading").enableHiveSupport().getOrCreate()


    try{
    val startDate = args(0).toString()
    val endDate = args(1).toString()

	
	//Fetching show level Archival data of (month - 2) of APP and WEB
	
    //val startDate = spark.sqlContext.sql(s""" SELECT CONCAT(SUBSTRING(ADD_MONTHS(CURRENT_DATE,-1),1,7),'-01') """).take(1)(0)(0).toString()
    //val endDate = spark.sqlContext.sql(s""" SELECT DATE_ADD(CURRENT_DATE, -CAST(DAY(CURRENT_DATE) AS INT)) """).take(1)(0)(0).toString()

    val viewAppDF = spark.sqlContext.sql(s""" SELECT 'APP' PROJECT, DATE_FORMAT(V.DATE_STAMP,'yyyy-MM-dd') DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE SHOW, COUNT(DISTINCT(CASE WHEN V.EVENT = ('mediaReady') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS, SUM(CASE WHEN V.EVENT = 'mediaReady' THEN 1 ELSE 0 END) AS NUM_VIEWS, SUM(CASE WHEN  V.EVENT = 'Video Watched' AND (CAST (DURATION_SECONDS  AS BIGINT) BETWEEN 0 AND 36000) THEN ABS(CAST(V.DURATION_SECONDS AS BIGINT)) ELSE 0 END)/60 CONTENT_DURATION_SEC FROM VOOT_APP_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID WHERE V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' AND V.EVENT IN ('mediaReady', 'Video Watched') GROUP BY V.DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE """)

    val viewWebDF = spark.sqlContext.sql(s""" SELECT 'WEB' PROJECT, DATE_FORMAT(V.DATE_STAMP,'yyyy-MM-dd') DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE SHOW, COUNT(DISTINCT(CASE WHEN V.EVENT = ('First Play') THEN V.DISTINCT_ID ELSE '' END)) NUM_VIEWERS, SUM(CASE WHEN V.EVENT = 'First Play' THEN 1 ELSE 0 END) AS NUM_VIEWS, 0 AS CONTENT_DURATION_SEC FROM VOOT_WEB_BASE_EVENT V LEFT JOIN CONTENT_MAPPER M ON V.MEDIA_ID = M.ID WHERE V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' AND V.EVENT = ('First Play') GROUP BY V.DATE_STAMP, V.MEDIA_ID, M.REF_SERIES_TITLE """)

    val viewFinalDF = viewAppDF.union(viewWebDF)

    viewFinalDF.write.mode("overwrite").saveAsTable("CMS_ARCHIVAL")

	
	//Fetching Media Id level Archival data of (month - 2) of APP and WEB
	
    val mainAppDF = spark.sqlContext.sql(s""" Select 'APP' Project, DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp, m.ref_series_title Show, m.SBU, m.GENRE, m.language, s.sbu_new cluster, count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)) NUM_VIEWERS, COUNT(DISTINCT M.ID) MEDIA_ID_COUNT FROM voot_app_base_event v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU = m.SBU where event = ('mediaReady') and V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' group by date_stamp, m.ref_series_title,m.SBU, m.GENRE, m.language,s.sbu_new """)

    val mainWebDF = spark.sqlContext.sql(s""" Select 'WEB' Project, DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp , m.ref_series_title Show, m.SBU, m.GENRE, m.language, s.sbu_new cluster, count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)) NUM_VIEWERS, COUNT(DISTINCT M.ID) MEDIA_ID_COUNT FROM voot_web_base_event v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU = m.SBU where event = ('First Play') and V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' group by date_stamp,m.ref_series_title,m.SBU, m.GENRE, m.language,s.sbu_new """)

    val mainFinalDF = mainAppDF.union(mainWebDF)

    mainFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_ARCHIVAL")

	
	//Fetching upper total of APP and WEB as per the date
	
    val upperAppDF = spark.sqlContext.sql(s""" Select DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp, count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)) NUM_VIEWERS FROM voot_app_base v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU= m.SBU where  event = ('mediaReady') and V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' group by date_stamp """)

    val upperWebDF = spark.sqlContext.sql(s""" Select DATE_FORMAT(DATE_STAMP,'yyyy-MM-dd') date_stamp, count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)) NUM_VIEWERS FROM voot_web_base v left join CONTENT_MAPPER m on v.media_id = m.id left join Adsales_SBU_MAPPER s on s.SBU= m.SBU where  event = ('First Play') and V.DATE_STAMP >= '$startDate' AND V.DATE_STAMP <= '$endDate' group by date_stamp """)

    val upperFinalDF = upperAppDF.union(upperWebDF)

    upperFinalDF.write.mode("overwrite").saveAsTable("CMS_VIEWERS_ARCHIVAL_UPPERTOTAL")

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
