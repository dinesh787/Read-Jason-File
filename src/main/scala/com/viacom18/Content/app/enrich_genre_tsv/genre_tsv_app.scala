package com.viacom18.Content.app.enrich_genre_tsv
import org.apache.spark.sql.{SQLContext, SparkSession}

object genre_tsv_app {

  val spark = SparkSession.builder().appName("genretsv").enableHiveSupport()
    //.config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb").config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("genre_tsv_app start date ", date_zero)
    genre_tsv_app(date_zero,Load_type)

  }

  def genre_tsv_app(date_zero: String , Load_type : String) = {


    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString


    val VOOTAPPBASE =sqlcontext.sql(s"""select * from  voot_app_base_ext where mediaid is not null and date_stamp = '$date_zero'""")
    VOOTAPPBASE.createOrReplaceTempView("VOOTAPPBASE")

    val CONTENT_MAPPER =sqlcontext.sql("select * from CONTENT_MAPPER")
    CONTENT_MAPPER.createOrReplaceTempView("CONTENT_MAPPER")

    val SBU_CHANNEL_MAPPER =sqlcontext.sql("select * from SBU_CHANNEL_MAPPER")
    SBU_CHANNEL_MAPPER.createOrReplaceTempView("SBU_CHANNEL_MAPPER")

    val cmSBU = sqlcontext.sql("SELECT V.SBU_CLUSTER ,CAST(ID AS BIGINT),REF_SERIES_TITLE,C.SBU,CONTENT_DURATION,GENRE,LANGUAGE,CONTENT_DURATION_SEC FROM CONTENT_MAPPER C LEFT JOIN SBU_CHANNEL_MAPPER V ON V.SBU= C.SBU")
    cmSBU.createOrReplaceTempView("cmSBU")

    val tsv_genre = sqlcontext.sql(s"""select 'APP' as project,DATE_STAMP,C.SBU_CLUSTER,C.GENRE,'ALL' AS LANGUAGE,SUM(CASE WHEN UPPER(M.EVENT) = 'MEDIAREADY' THEN COUNT_PLAIN ELSE 0 END) AS NUM_VIEWS,COUNT(DISTINCT CASE WHEN UPPER(M.EVENT) = 'MEDIAREADY' THEN  DISTINCT_ID ELSE NULL END) AS  NUM_VIEWERS,SUM(CASE WHEN (M.APP_VERSION IN ('47','1.2.16','1.2.21') AND (CAST(DURATION AS BIGINT) BETWEEN 0 AND 36000)  AND upper(M.EVENT) = 'VIDEO WATCHED')THEN CAST(DURATION AS BIGINT) WHEN (M.APP_VERSION NOT IN ('47','1.2.16','1.2.21') AND (CAST(DURATION_SECONDS AS BIGINT) BETWEEN 0 AND 36000) AND UPPER(M.EVENT) = 'VIDEO WATCHED') THEN CAST(DURATION_SECONDS AS BIGINT) ELSE 0 END)  AS DURATION_WATCHED_SECS,(CASE WHEN (COUNT(DISTINCT CASE WHEN UPPER(M.EVENT) = 'MEDIAREADY' THEN  DISTINCT_ID ELSE NULL END)) = 0 THEN 0 END) AS GENRE_TSV FROM cmSBU C LEFT JOIN VOOTAPPBASE M on C.id = M.mediaid where DATE_STAMP = '$date_zero' GROUP BY DATE_STAMP,C.SBU_CLUSTER,C.GENRE""")
    tsv_genre.createOrReplaceTempView("tsv_genre")

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")

    sqlcontext.sql(s"""insert into F_AGG_GENRE_TSV partition(date_part_col,project_part_col) SELECT project,cast(DATE_STAMP as timestamp),SBU_CLUSTER,GENRE,LANGUAGE,NUM_VIEWERS,NUM_VIEWS,DURATION_WATCHED_SECS,CASE WHEN GENRE_TSV IS NULL THEN  DURATION_WATCHED_SECS/NUM_VIEWERS ELSE 0 END AS GENRE_TSV,cast(DATE_STAMP as date) as date_part_col ,'APP' as project_part_col  FROM tsv_genre where DATE_STAMP = '$date_zero'""")

    val tsv_genre1 = sqlcontext.sql(s"""select 'App' as project,DATE_STAMP,C.SBU_CLUSTER,C.GENRE,C.LANGUAGE AS LANGUAGE,SUM(CASE WHEN UPPER(M.EVENT) = 'MEDIAREADY' THEN COUNT_PLAIN ELSE 0 END) AS NUM_VIEWS,COUNT(DISTINCT CASE WHEN UPPER(M.EVENT) = 'MEDIAREADY' THEN  DISTINCT_ID ELSE NULL END) AS  NUM_VIEWERS,SUM(CASE WHEN (M.APP_VERSION IN ('47','1.2.16','1.2.21') AND (CAST(DURATION AS BIGINT) BETWEEN 0 AND 36000)  AND upper(M.EVENT) = 'VIDEO WATCHED')THEN CAST(DURATION AS BIGINT) WHEN (M.APP_VERSION NOT IN ('47','1.2.16','1.2.21') AND (CAST(DURATION_SECONDS AS BIGINT) BETWEEN 0 AND 36000) AND UPPER(M.EVENT) = 'VIDEO WATCHED') THEN CAST(DURATION_SECONDS AS BIGINT) ELSE 0 END)  AS DURATION_WATCHED_SECS,(CASE WHEN (COUNT(DISTINCT CASE WHEN UPPER(M.EVENT) = 'MEDIAREADY' THEN  DISTINCT_ID ELSE NULL END)) = 0 THEN 0 END) AS GENRE_TSV FROM cmSBU C LEFT JOIN VOOTAPPBASE M on C.id = M.mediaid where date_stamp = '$date_zero' GROUP BY DATE_STAMP,C.SBU_CLUSTER,C.GENRE,C.LANGUAGE""")
    tsv_genre1.createOrReplaceTempView("tsv_genre1")

    sqlcontext.sql(s"""INSERT INTO F_AGG_GENRE_TSV partition(date_part_col,project_part_col) SELECT project,cast(DATE_STAMP as timestamp),SBU_CLUSTER,GENRE,LANGUAGE,NUM_VIEWERS,NUM_VIEWS,DURATION_WATCHED_SECS,CASE WHEN GENRE_TSV IS NULL THEN  DURATION_WATCHED_SECS/NUM_VIEWERS ELSE 0 END AS GENRE_TSV,cast(DATE_STAMP as date) as date_part_col ,'APP' as project_part_col FROM tsv_genre1 where DATE_STAMP = '$date_zero'""")

    val count = sqlcontext.sql(s""" select count(*) from  f_agg_show_cac_fe_dly where date_part_col ='$date_zero' and project_part_col ='APP'""").take(1)(0).get(0).toString
    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory select CURRENT_DATE ,'$date_zero' ,'F_AGG_GENRE_TSV' ,'$Load_type','APP', '$count','$st_time','$end_time','$min'  from  DataTableloadHistory  limit 1""")


    println("Job finished sucessfully ", date_zero)
  }
}
