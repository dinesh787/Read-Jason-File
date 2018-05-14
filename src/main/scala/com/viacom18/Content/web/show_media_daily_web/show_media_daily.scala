package com.viacom18.Content.web.show_media_daily_web

import org.apache.spark.sql.{SQLContext, SparkSession}

object show_media_daily {

  val spark = SparkSession.builder()
    .appName("showmediadaily")
    .enableHiveSupport()
//    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
//    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
//    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("funnel start date ", date_zero)
    show_media_daily(date_zero,Load_type)

  }

  def show_media_daily(date_zero: String , Load_type : String) = {
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    val VOOTWEBBASE =sqlcontext.sql(s"""select * from  voot_web_base_ext where mediaid is not null and date_stamp = '$date_zero'""")
    VOOTWEBBASE.createOrReplaceTempView("VOOTWEBBASE")

    val CONTENT_MAPPER =sqlcontext.sql("select * from CONTENT_MAPPER")
    CONTENT_MAPPER.createOrReplaceTempView("CONTENT_MAPPER")

    val SBU_CHANNEL_MAPPER =sqlcontext.sql("select * from SBU_CHANNEL_MAPPER")
    SBU_CHANNEL_MAPPER.createOrReplaceTempView("SBU_CHANNEL_MAPPER")

    val cmSBU = sqlcontext.sql("SELECT c.id,c.type,c.name,c.description,c.ref_series_title,c.content_synopsis,c.episode_main_title,c.content_type,c.content_subject,c.content_file_name,c.is_downable,c.ref_series_season,c.episode_no,c.content_duration,c.telecast_date,c.release_year,c.genre,c.media_external_id,c.language,c.start_date,c.content_duration_sec,c.keywords,c.characterlist,c.contributorlist,c.movie_director,c.movie_producer,c.load_date,v.channel_name_vendor_name,v.load_date,v.sbu,v.own_bought,v.kids,v.sbu_cluster FROM CONTENT_MAPPER C LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU")
    cmSBU.createOrReplaceTempView("cmSBU")

    val FINAL = sqlcontext.sql(s"""select 'Web' as PROJECT,Cast(date_stamp as Date) AS DATE,C.SBU_CLUSTER as SBU_CLUSTER,C.sbu as SBU,C.channel_name_vendor_name as Channel_Vendor_Name,C.Own_Bought as Own_Bought_Flag,C.Kids as Kids_Flag,case when upper(C.content_Type) = 'FULL EPISODE' then 'FULL EPISODE' else 'CAC' end as CAC_FE_Flag,C.Genre,C.Language as Content_Language,C.Ref_Series_Title as Show_Name,M.mediaid,C.Name,C.Telecast_Date,C.Episode_No,C.Start_Date,C.Release_Year,0 as Content_Duration_Sec,COUNT(DISTINCT Case when upper(M.event) = upper('First Play') then distinct_id else NULL end) as Num_Viewers,SUM(Case when upper(M.event) = upper('First Play') then count_plain else 0 end) as Num_Views,0 as duration_Watched_secs,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=7 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_7D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=14 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_14D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=30 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_30D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=90 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_90D FROM VOOTWEBBASE M  LEFT JOIN cmSBU C on C.id = M.mediaid where date_stamp = '$date_zero' Group By date_stamp,C.SBU_CLUSTER,C.sbu,C.channel_name_vendor_name,C.Own_Bought,C.Kids,case when upper(C.content_Type) = 'FULL EPISODE' then 'FULL EPISODE' else 'CAC' end,C.Genre,C.Language,C.Ref_Series_Title,M.mediaid,C.Name,C.Telecast_Date,C.Episode_No,C.Start_Date,C.Release_Year,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=7 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=14 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=30 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=90 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END""")
    FINAL.createOrReplaceTempView("FINAL")

    val mediate = sqlcontext.sql("select F.PROJECT ,F.DATE,F.SBU_CLUSTER,F.SBU,F.Channel_Vendor_Name,F.Own_Bought_Flag,F.Kids_Flag,F.CAC_FE_Flag,F.Genre,F.Content_Language,F.Show_Name,F.mediaid,F.Name,F.Telecast_Date,F.Episode_No,F.Start_Date,F.Release_Year,cm.Content_Duration_Sec,F.Num_Viewers,F.Num_Views,F.duration_Watched_secs,F.RECENT_7D,F.RECENT_14D,F.RECENT_30D,F.RECENT_90D from FINAL F left join cmSBU cm on F.mediaid = cm.id")
    mediate.createOrReplaceTempView("mediate")

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")


    sqlcontext.sql(s"""insert into f_agg_media_dly partition(date_part_col , project_part_col) select PROJECT,cast(`DATE` as timestamp) as date,SBU_CLUSTER,SBU,Channel_Vendor_Name,Own_Bought_Flag,Kids_Flag,CAC_FE_Flag,Genre,Content_Language,Show_Name,mediaid,Name,Telecast_Date,Episode_No,Start_Date,Release_Year,Content_Duration_Sec,Num_Viewers,Num_Views,duration_Watched_secs,RECENT_7D,RECENT_14D,RECENT_30D,RECENT_90D ,cast(`date` as date) as date_part_col ,'WEB' as project_part_col from mediate where `date` ='$date_zero'""")


    val count = sqlcontext.sql(s""" select count(*) from  f_agg_media_dly where date_part_col ='$date_zero' and project_part_col ='WEB'""").take(1)(0).get(0).toString
    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory select CURRENT_DATE ,'$date_zero' ,'f_agg_media_dly' ,'$Load_type','WEB', '$count','$st_time','$end_time','$min'  from  DataTableloadHistory  limit 1""")




  }
}