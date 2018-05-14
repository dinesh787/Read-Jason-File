package com.viacom18.Content.app.agg_media_hrly_app
import org.apache.spark.sql.{SQLContext, SparkSession}

object agg_media_hrly {
  val spark = SparkSession.builder().appName("aggmediahrlyapp").enableHiveSupport()
    //.config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb").config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("agg_media_hrly_app start date", date_zero)
    agg_media_hrly(date_zero,Load_type)

  }

  def agg_media_hrly(date_zero: String , Load_type : String) = {

    println("agg_media_hrly_app JOB started : ",Load_type )
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString


    val  VOOTAPPBASE =sqlcontext.sql(s""" select * from voot_app_base_ext where date_stamp = '$date_zero'""")
    VOOTAPPBASE.createOrReplaceTempView("VOOTAPPBASE")

    val CONTENT_MAPPER =sqlcontext.sql("select * from CONTENT_MAPPER")
    CONTENT_MAPPER.createOrReplaceTempView("CONTENT_MAPPER")

    val SBU_CHANNEL_MAPPER =sqlcontext.sql("select * from SBU_CHANNEL_MAPPER")
    SBU_CHANNEL_MAPPER.createOrReplaceTempView("SBU_CHANNEL_MAPPER")

    val cmSBU = sqlcontext.sql("SELECT V.SBU_CLUSTER ,CAST(ID AS BIGINT),REF_SERIES_TITLE,C.SBU,CONTENT_DURATION,GENRE,LANGUAGE,CONTENT_DURATION_SEC,channel_name_vendor_name,own_bought ,content_type,kids,genre,ref_series_title FROM CONTENT_MAPPER C LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU")
    cmSBU.createOrReplaceTempView("cmSBU")


    val mediate = sqlcontext.sql("Select distinct 'App' as Program ,Cast(date_stamp as date) as Date, hour(cast(from_unixtime(time) as string)) as ts ,V.SBU_CLUSTER as SBU_CLUSTER,C.SBU as SBU,channel_name_vendor_name AS CHANNEL_VENDOR_NAME,own_bought AS OWN_BOUGHT_FLAG,V.Kids as Kids_Flag,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) AS CAC_FE_Flag,C.Genre,C.Language as Content_Language,C.Ref_Series_Title as Show_Name,M.mediaid,C.Name,C.Telecast_Date,C.Episode_No,C.Start_Date,C.Release_Year,COUNT( DISTINCT Case when upper(M.event) = 'MEDIAREADY' then distinct_id else NULL end) as Num_Viewers,SUM(Case when upper(M.event) = 'MEDIAREADY' then cast(count_plain as bigint) else 0 end) as Num_Views,SUM(case WHEN (m.app_version IN ('47','1.2.16','1.2.21') and (cast(duration as bigint) between 0 and 36000)  and upper(m.event) = 'VIDEO WATCHED') THEN cast(duration as bigint) WHEN (m.app_version NOT IN ('47','1.2.16','1.2.21') and (cast(duration_seconds as bigint) between 0 and 36000) and upper(m.event) = 'VIDEO WATCHED') THEN cast(duration_seconds as bigint) else 0 END) as duration_Watched_secs,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=7 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_7D ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=14 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_14D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=30 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_30D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=90 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_90D From VOOTAPPBASE M left join Content_Mapper C on C.id = M.mediaid left join SBU_Channel_Mapper V on V.SBU= C.SBU  Group By Cast(date_stamp as date), hour(cast(from_unixtime(time) as string))  ,V.SBU_CLUSTER,C.SBU,channel_name_vendor_name,Own_Bought,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END),V.Kids,C.Genre,C.Language,C.Ref_Series_Title,M.mediaid,C.Name,C.Telecast_Date,C.Episode_No,C.Start_Date,C.Release_Year,M.date_stamp,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=7 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END  ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=14 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=30 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=90 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END ")
    mediate.createOrReplaceTempView("mediate")
    //val mediate = sqlcontext.sql("Select 'App' as Program ,Cast(date_stamp as date) as Date, hour(cast(from_unixtime(time) as string)) as ts ,V.SBU_CLUSTER as SBU_CLUSTER,C.SBU as SBU,channel_name_vendor_name AS CHANNEL_VENDOR_NAME,own_bought AS OWN_BOUGHT_FLAG,V.Kids as Kids_Flag,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) AS CAC_FE_Flag,C.Genre,C.Language as Content_Language,C.Ref_Series_Title as Show_Name,M.mediaid,C.Name,C.Telecast_Date,C.Episode_No,C.Start_Date,C.Release_Year,COUNT( DISTINCT Case when upper(M.event) = 'MEDIAREADY' then distinct_id else NULL end) as Num_Viewers,SUM(Case when upper(M.event) = 'MEDIAREADY' then count_plain else 0 end) as Num_Views,SUM(case WHEN (m.app_version IN ('47','1.2.16','1.2.21') and (duration between 0 and 36000)  and upper(m.event) = 'VIDEO WATCHED')THEN duration WHEN (m.app_version NOT IN ('47','1.2.16','1.2.21') and (duration_seconds between 0 and 36000) and upper(m.event) = 'VIDEO WATCHED') THEN duration_seconds else 0 END)  as duration_Watched_secs,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=7 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_7D ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=14 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_14D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=30 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_30D,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=90 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END AS RECENT_90D From VOOTAPPBASE M left join Content_Mapper C on C.id = M.mediaid left join SBU_Channel_Mapper V on V.SBU= C.SBU  Group By Cast(date_stamp as date), hour(cast(from_unixtime(time) as string))  ,V.SBU_CLUSTER,C.SBU,channel_name_vendor_name,Own_Bought,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END),V.Kids,C.Genre,C.Language,C.Ref_Series_Title,M.mediaid,C.Name,C.Telecast_Date,C.Episode_No,C.Start_Date,C.Release_Year,M.date_stamp,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=7 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END  ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=14 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=30 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END ,CASE WHEN DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) <=90 AND DATEDIFF(CAST(DATE_STAMP AS DATE),date_format(from_unixtime(unix_timestamp(start_date, 'yyyy-MMM-dd HH:mm:ss')),'yyyy-MM-dd')) >=0 THEN  'Y' ELSE 'N' END ")


    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")

    sqlcontext.sql(s"""insert into F_Agg_Media_Hrly partition(date_part_col,project_part_col) select distinct Program,Date,ts,SBU_CLUSTER,SBU,CHANNEL_VENDOR_NAME,OWN_BOUGHT_FLAG,Kids_Flag,CAC_FE_Flag,Genre,Content_Language,Show_Name,mediaid,Name,Telecast_Date,Episode_No,Start_Date,Release_Year,Num_Viewers,Num_Views,duration_Watched_secs,RECENT_7D,RECENT_14D,RECENT_30D,RECENT_90D,cast(`date` as date) as date_part_col ,'APP' as project_part_col from mediate where date='$date_zero'""")


    val count = sqlcontext.sql(s""" select count(*) from  F_Agg_Media_Hrly where date_part_col ='$date_zero' and project_part_col ='APP'""").take(1)(0).get(0).toString
    println("agg_media_hrly_app data inserted : ",count )

    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory select CURRENT_DATE ,'$date_zero' ,'F_Agg_Media_Hrly' ,'$Load_type','APP', '$count','$st_time','$end_time','$min'  from  DataTableloadHistory  limit 1""")







  }
}

