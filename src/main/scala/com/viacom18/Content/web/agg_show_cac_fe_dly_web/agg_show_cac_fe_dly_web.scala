package com.viacom18.Content.web.agg_show_cac_fe_dly_web

import org.apache.spark.sql.{SQLContext, SparkSession}

object agg_show_cac_fe_dly_web {

  val spark = SparkSession.builder()
    .appName("aggshowcacfedlyweb")
    .enableHiveSupport()
    //    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    //    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    //    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("agg_show_cac_fe_dly_web start date ", date_zero)
    agg_show_cac_fe_dly(date_zero,Load_type)

  }

  def agg_show_cac_fe_dly(date_zero: String , Load_type : String) = {
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    val VOOTWEBBASE =sqlcontext.sql(s"""select * from voot_web_base_ext where date_stamp = '$date_zero'""")
    VOOTWEBBASE.createOrReplaceTempView("VOOTWEBBASE")

    val CONTENT_MAPPER =sqlcontext.sql("select * from CONTENT_MAPPER")
    CONTENT_MAPPER.createOrReplaceTempView("CONTENT_MAPPER")

    val SBU_CHANNEL_MAPPER =sqlcontext.sql("select * from SBU_CHANNEL_MAPPER")
    SBU_CHANNEL_MAPPER.createOrReplaceTempView("SBU_CHANNEL_MAPPER")

    val cmSBU = sqlcontext.sql("SELECT V.SBU_CLUSTER ,CAST(ID AS BIGINT),REF_SERIES_TITLE,C.SBU,CONTENT_DURATION,GENRE,LANGUAGE,CONTENT_DURATION_SEC,channel_name_vendor_name,own_bought,content_type,kids,genre,ref_series_title FROM CONTENT_MAPPER C LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU")
    cmSBU.createOrReplaceTempView("cmSBU")

    val mediate = sqlcontext.sql("Select 'Web' as Program,CAST(DATE_STAMP AS DATE) AS DATE , V.SBU_CLUSTER as SBU_CLUSTER,C.SBU as SBU,channel_name_vendor_name AS CHANNEL_VENDOR_NAME,own_bought AS OWN_BOUGHT_FLAG,V.Kids as Kids_Flag,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) AS content_type ,C.Genre,C.Ref_Series_Title  as Show_Name, 'All' as Content_Language, COUNT( DISTINCT Case when upper(M.event) = 'FIRST PLAY' then distinct_id else NULL end) as Num_Viewers, SUM(Case when upper(M.event) = 'FIRST PLAY' then count_plain else 0 end) as Num_Views, 0 as duration_Watched_secs From VOOTWEBBASE M left join Content_Mapper C on C.id = M.mediaid left join SBU_Channel_Mapper V on V.SBU= C.SBU Group By date_stamp ,V.SBU_CLUSTER ,C.SBU,channel_name_vendor_name,V.Own_Bought,CASE WHEN upper(content_type)='FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END ,V.Kids,C.Genre,C.Ref_Series_Title")
    mediate.createOrReplaceTempView("mediate")

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")

    sqlcontext.sql(s"""insert into f_agg_show_cac_fe_dly partition(date_part_col,project_part_col) select PROGRAM,cast(`DATE` as timestamp) as date,SBU_CLUSTER, SBU, CHANNEL_VENDOR_NAME, OWN_BOUGHT_FLAG, KIDS_FLAG, content_type, GENRE, SHOW_NAME, CONTENT_LANGUAGE, NUM_VIEWERS, NUM_VIEWS, DURATION_WATCHED_SECS,cast(`date` as date) as date_part_col,'WEB' as project_part_col from mediate where `date` ='$date_zero'""")

    val mediate1 = sqlcontext.sql("Select 'Web' as Program,CAST(DATE_STAMP AS DATE) AS DATE , V.SBU_CLUSTER as SBU_CLUSTER,C.SBU as SBU,channel_name_vendor_name AS CHANNEL_VENDOR_NAME,own_bought AS OWN_BOUGHT_FLAG,V.Kids as Kids_Flag,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) AS content_type ,C.Genre,C.Ref_Series_Title  as Show_Name,C.LANGUAGE AS CONTENT_LANGUAGE, COUNT( DISTINCT Case when upper(M.event) = 'FIRST PLAY' then distinct_id else NULL end) as Num_Viewers, SUM(Case when upper(M.event) = 'FIRST PLAY' then count_plain else 0 end) as Num_Views, 0 as duration_Watched_secs From VOOTWEBBASE M left join Content_Mapper C on C.id = M.mediaid left join SBU_Channel_Mapper V on V.SBU= C.SBU  Group By date_stamp ,V.SBU_CLUSTER ,C.SBU,channel_name_vendor_name,V.Own_Bought,CASE WHEN upper(content_type)='FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END ,V.Kids,C.Genre,C.Ref_Series_Title,C.LANGUAGE")
    mediate1.createOrReplaceTempView("mediate1")

    sqlcontext.sql(s"""insert into f_agg_show_cac_fe_dly partition(date_part_col,project_part_col) select PROGRAM,cast(`DATE` as timestamp) as date,SBU_CLUSTER, SBU, CHANNEL_VENDOR_NAME, OWN_BOUGHT_FLAG, KIDS_FLAG, content_type, GENRE, SHOW_NAME, CONTENT_LANGUAGE, NUM_VIEWERS, NUM_VIEWS, DURATION_WATCHED_SECS,cast(`date` as date) as date_part_col,'WEB' as project_part_col from mediate1 where `date` ='$date_zero'""")


    val count = sqlcontext.sql(s""" select count(*) from  f_agg_show_cac_fe_dly where date_part_col ='$date_zero' and project_part_col ='WEB'""").take(1)(0).get(0).toString
    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory partition(date_stamp) select CURRENT_DATE ,'$date_zero' ,'f_agg_show_cac_fe_dly' ,'$Load_type','WEB', '$count','$st_time','$end_time','$min','yes','content','$date_zero' as date_stamp from  DataTableloadHistory  limit 1""")



  }
}