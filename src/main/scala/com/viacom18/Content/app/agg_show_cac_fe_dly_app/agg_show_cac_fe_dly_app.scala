package com.viacom18.Content.app.agg_show_cac_fe_dly_app

import org.apache.spark.sql.{SQLContext, SparkSession}
object agg_show_cac_fe_dly_app {

  val spark = SparkSession
    .builder()
    .appName("aggshowcacfedlyapp")
    .enableHiveSupport()
    //    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    //    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    //    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("agg_show_cac_fe_dly_app start date", date_zero)
    agg_show_cac_fe_dly(date_zero,Load_type)
  }
  def agg_show_cac_fe_dly(date_zero: String , Load_type : String) = {
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    val  VOOTAPPBASE =sqlcontext.sql(s"""select * from voot_app_base_ext  where date_stamp = '$date_zero'""")
    VOOTAPPBASE.createOrReplaceTempView("VOOTAPPBASE")

    val CONTENT_MAPPER =sqlcontext.sql("select * from CONTENT_MAPPER")
    CONTENT_MAPPER.createOrReplaceTempView("CONTENT_MAPPER")

    val SBU_CHANNEL_MAPPER =sqlcontext.sql("select * from SBU_CHANNEL_MAPPER")
    SBU_CHANNEL_MAPPER.createOrReplaceTempView("SBU_CHANNEL_MAPPER")

    val cmSBU = sqlcontext.sql("SELECT V.SBU_CLUSTER ,CAST(ID AS BIGINT),REF_SERIES_TITLE,C.SBU,CONTENT_DURATION,GENRE,LANGUAGE,CONTENT_DURATION_SEC,channel_name_vendor_name,V.own_bought ,content_type,kids,genre,ref_series_title FROM CONTENT_MAPPER C LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU  ")
    cmSBU.createOrReplaceTempView("cmSBU")

    val mediate = sqlcontext.sql(s"""SELECT 'App' AS PROGRAM ,CAST(DATE_STAMP AS DATE) AS DATE,C.SBU_CLUSTER AS SBU_CLUSTER,C.SBU AS SBU,channel_name_vendor_name AS CHANNEL_VENDOR_NAME,own_bought AS OWN_BOUGHT_FLAG,C.KIDS AS KIDS_FLAG,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) AS content_type ,C.GENRE,C.REF_SERIES_TITLE AS SHOW_NAME,'ALL' AS CONTENT_LANGUAGE,COUNT(DISTINCT CASE WHEN upper(M.EVENT)='MEDIAREADY' THEN DISTINCT_ID ELSE NULL END) AS NUM_VIEWERS,SUM(CASE WHEN upper(M.EVENT)='MEDIAREADY' THEN cast(COUNT_PLAIN as bigint) ELSE 0 END) AS NUM_VIEWS,SUM(CASE WHEN (M.APP_VERSION IN ('47','1.2.16','1.2.21') AND (cast(DURATION as BIGINT)BETWEEN 0 AND 36000)  AND upper(M.EVENT) = 'VIDEO WATCHED')THEN cast(DURATION as bigint) WHEN (M.APP_VERSION NOT IN ('47','1.2.16','1.2.21') AND (cast(DURATION_SECONDS as BIGINT) BETWEEN 0 AND 36000) AND upper(M.EVENT) = 'VIDEO WATCHED')THEN cast(DURATION_SECONDS as BIGINT)ELSE 0 END) AS DURATION_WATCHED_SECS FROM  VOOTAPPBASE M LEFT JOIN cmSBU C on C.id = M.mediaid  GROUP BY DATE_STAMP ,C.SBU_CLUSTER,C.SBU,channel_name_vendor_name,own_bought,(CASE WHEN upper(content_type)='FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) ,KIDS,GENRE,REF_SERIES_TITLE""")
    mediate.createOrReplaceTempView("mediate")

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")

    sqlcontext.sql(s"insert into f_agg_show_cac_fe_dly partition(date_part_col,project_part_col) select PROGRAM,cast(DATE as timestamp) as date,SBU_CLUSTER, SBU, CHANNEL_VENDOR_NAME, OWN_BOUGHT_FLAG, KIDS_FLAG, content_type, GENRE, SHOW_NAME, CONTENT_LANGUAGE, NUM_VIEWERS, NUM_VIEWS, DURATION_WATCHED_SECS,cast(`date` as date) as date_part_col ,'APP' as project_part_col from mediate where date ='$date_zero'")

    val mediate1 = sqlcontext.sql("SELECT 'App' AS PROGRAM ,CAST(DATE_STAMP AS DATE) AS DATE,C.SBU_CLUSTER AS SBU_CLUSTER,C.SBU AS SBU,channel_name_vendor_name AS CHANNEL_VENDOR_NAME,own_bought AS OWN_BOUGHT_FLAG,C.KIDS AS KIDS_FLAG,(CASE WHEN upper(content_type) = 'FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) AS content_type ,C.GENRE,C.REF_SERIES_TITLE AS SHOW_NAME,C.LANGUAGE AS CONTENT_LANGUAGE,COUNT(DISTINCT CASE WHEN upper(M.EVENT)='MEDIAREADY' THEN DISTINCT_ID ELSE NULL END) AS NUM_VIEWERS,SUM(CASE WHEN upper(M.EVENT)='MEDIAREADY' THEN COUNT_PLAIN ELSE 0 END) AS NUM_VIEWS,SUM(CASE WHEN (M.APP_VERSION IN ('47','1.2.16','1.2.21') AND (cast(DURATION as BIGINT)BETWEEN 0 AND 36000)  AND upper(M.EVENT) = 'VIDEO WATCHED')THEN DURATION WHEN (M.APP_VERSION NOT IN ('47','1.2.16','1.2.21') AND (cast(DURATION_SECONDS as BIGINT) BETWEEN 0 AND 36000) AND upper(M.EVENT) = 'VIDEO WATCHED')THEN cast(DURATION_SECONDS as BIGINT)ELSE 0 END) AS DURATION_WATCHED_SECS FROM VOOTAPPBASE M LEFT JOIN  cmSBU C on C.id = M.mediaid  GROUP BY DATE_STAMP ,C.SBU_CLUSTER,C.SBU,channel_name_vendor_name,own_bought,(CASE WHEN upper(content_type)='FULL EPISODE' THEN 'FULL EPISODE' ELSE 'CAC' END) ,KIDS,GENRE,REF_SERIES_TITLE,C.LANGUAGE")
    mediate1.createOrReplaceTempView("mediate1")

    sqlcontext.sql(s"""insert into f_agg_show_cac_fe_dly partition(date_part_col,project_part_col) select PROGRAM,cast(`DATE` as timestamp) as date,SBU_CLUSTER, SBU, CHANNEL_VENDOR_NAME, OWN_BOUGHT_FLAG, KIDS_FLAG, content_type, GENRE, SHOW_NAME, CONTENT_LANGUAGE, NUM_VIEWERS, NUM_VIEWS, DURATION_WATCHED_SECS,cast(`date` as date) as date_part_col, 'APP' as project_part_col from mediate1 where `date` ='$date_zero'""")

    val count = sqlcontext.sql(s""" select count(*) from  f_agg_show_cac_fe_dly where date_part_col ='$date_zero' and project_part_col ='APP'""").take(1)(0).get(0).toString
    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory partition(date_stamp) select CURRENT_DATE ,'$date_zero' ,'f_agg_show_cac_fe_dly' ,'$Load_type','APP', '$count','$st_time','$end_time','$min','yes','content','$date_zero' as date_stamp  from  DataTableloadHistory  limit 1""")

  }
}