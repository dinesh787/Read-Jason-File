package com.viacom18.Executive.web

import org.apache.spark.sql.{SQLContext, SparkSession}

object grain_web {

  val spark = SparkSession
    .builder()
    .appName("grainweb")
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
    grain_web(date_zero,Load_type)

  }

  def grain_web(date_zero: String,Load_type:String) = {
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")

    val vootwebbase =sqlcontext.sql(s"select * from  voot_web_base_ext where date_stamp = '$date_zero'")
    vootwebbase.createOrReplaceTempView("vootwebbase")
    val z =sqlcontext.sql("SELECT date_stamp,distinct_id FROM vootwebbase GROUP BY date_stamp,distinct_id")
    z.createOrReplaceTempView("z")
    val b = sqlcontext.sql("SELECT date_stamp,distinct_id ,COUNT(*) AS WEB_VISITORS FROM vootwebbase WHERE upper(event) IN (upper('Page Viewed')) GROUP BY date_stamp,distinct_id")
    b.createOrReplaceTempView("b")
    val c = sqlcontext.sql("SELECT date_stamp,distinct_id ,COUNT(*) AS WEB_VIEWERS,SUM(cast(COUNT_PLAIN as bigint)) AS COUNT_PLAIN_VIEWS FROM vootwebbase WHERE upper(event) IN (upper('First Play')) GROUP BY date_stamp,distinct_id")
    c.createOrReplaceTempView("c")
    val d = sqlcontext.sql("SELECT date_stamp,distinct_id,COUNT(*) AS VIDEO_WATCHED,SUM(Cast(effective_time as decimal(38,2))) AS duration_watched_ms FROM vootwebbase WHERE upper(event) = upper('youbora-play') GROUP BY date_stamp,distinct_id")
    d.createOrReplaceTempView("d")

    val web = sqlcontext.sql("SELECT Web.distinct_id,Web.date_stamp,upper(Web.platform) AS PLATFORM FROM vootwebbase Web WHERE upper(Web.platform) = upper('Web') GROUP BY Web.distinct_id,Web.date_stamp,upper(Web.platform)")
    web.createOrReplaceTempView("web")
    val web_4 = sqlcontext.sql("SELECT Web.distinct_id,Web.date_stamp,Web.Utm_Source FROM vootwebbase Web WHERE Web.Utm_Source IS NOT NULL GROUP BY Web.distinct_id,Web.date_stamp,Web.utm_source")
    web_4.createOrReplaceTempView("web_4")
    val web_1 = sqlcontext.sql("SELECT Web.distinct_id,Web.date_stamp,upper(Web.first_time) as first_time FROM vootwebbase Web WHERE upper(Web.first_time) = 'TRUE' GROUP BY Web.distinct_id,Web.date_stamp,upper(Web.first_time)")
    web_1.createOrReplaceTempView("web_1")
    val web_2 = sqlcontext.sql("SELECT Web.distinct_id,Web.date_stamp,1 AS IsRegistered FROM vootwebbase Web WHERE (Web.user_type is NOT NULL AND upper(Web.user_type) <> upper('Guest')) GROUP BY Web.distinct_id,Web.date_stamp")
    web_2.createOrReplaceTempView("web_2")


    val finalweb = sqlcontext.sql("select z.distinct_id,z.date_stamp,CASE WHEN upper(w.PLATFORM) = 'WEB' THEN 'Desktop' ELSE 'Mobile Web' END AS PLATFORM,CASE WHEN w1.first_time = 'TRUE' THEN 'NEW' ELSE 'REPEAT' END AS NEW_REPEAT_FLAG,CASE WHEN w4.utm_source is not null THEN 'INORGANIC' ELSE 'ORGANIC' END AS ORGANIC_INORGANIC_INSTALL_FLAG,CASE WHEN w2.IsRegistered = 1 THEN 'Y' ELSE 'N' END AS REG_USER_FLAG,b.WEB_VISITORS,c.WEB_VIEWERS,c.COUNT_PLAIN_VIEWS,d.VIDEO_WATCHED,CASE WHEN cast(d.duration_watched_ms as bigint) < 0 OR cast(d.duration_watched_ms as bigint) > 36000000 THEN 0 ELSE cast(d.duration_watched_ms as bigint) END AS DURATION_WATCHED_MS,0 AS duration_bucket,0 AS frequency_bucket,'N' AS MONTHLY_POWER_USER,CASE WHEN (cast(d.duration_watched_ms as bigint)/60000) >= 30 THEN 'Power User' ELSE 'Non Power User' END AS DAILY_POWER_USER,'Y' AS DAILY_POWER_USER_FLAG from z z left join b b on z.date_stamp = b.date_stamp and z.distinct_id = b.distinct_id left join c c on z.date_stamp = c.date_stamp and z.distinct_id = c.distinct_id left join d d on z.date_stamp = d.date_stamp and z.distinct_id = d.distinct_id left join web w on z.date_stamp = w.date_stamp and z.distinct_id = w.distinct_id left join web_4 w4 on z.date_stamp = w4.date_stamp and z.distinct_id = w4.distinct_id left join web_1 w1 on z.date_stamp = w1.date_stamp and z.distinct_id = w1.distinct_id left join web_2 w2 on z.date_stamp = w2.date_stamp and z.distinct_id = w2.distinct_id")
    finalweb.createOrReplaceTempView("finalweb")

    val GRAINWEB =sqlcontext.sql("select * from  finalweb")
    GRAINWEB.createOrReplaceTempView("GRAINWEB")

    sqlcontext.sql(s"insert into F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH partition(date_part_col,project_part_col) select 'Web' AS PROJECT,date_stamp AS DATE,PLATFORM AS PLATFORM,'NA' AS MANUFACTURER,'NA' AS PRICE_BAND,'NA' AS APP_VER_PK_NPK_FLAG,ORGANIC_INORGANIC_INSTALL_FLAG AS ORGANIC_INORGANIC_INSTALL_FLAG,NEW_REPEAT_FLAG AS NEW_REPEAT_FLAG,REG_USER_FLAG AS REG_USER_FLAG,CASE WHEN DURATION_BUCKET IS NULL THEN 'OTHERS' ELSE DURATION_BUCKET END AS DURATION_BUCKET,FREQUENCY_BUCKET,MONTHLY_POWER_USER,DAILY_POWER_USER,DAILY_POWER_USER_FLAG,COUNT(WEB_VISITORS) AS NUM_VISITORS,COUNT(WEB_VIEWERS) AS NUM_VIEWERS,0 AS NUM_INSTALLS,0 AS NUM_VISITORS_FUNNEL,SUM( CASE WHEN (CAST(WEB_VISITORS AS BIGINT) >=1 AND CAST(WEB_VIEWERS AS BIGINT) >= 1) THEN 1 ELSE 0 END) AS NUM_VIEWERS_FUNNEL,COUNT(CASE WHEN (CAST(WEB_VISITORS AS BIGINT) >=1 AND CAST(WEB_VIEWERS AS BIGINT) >= 1 AND CAST(VIDEO_WATCHED AS BIGINT) >=1 AND (CAST(duration_watched_ms AS BIGINT)/60000) > 30 ) THEN distinct_id END) AS NUM_POWER_VIEWERS_FUNNEL,SUM(CAST(COUNT_PLAIN_VIEWS AS BIGINT)) AS NUM_VIEWS,SUM(CASE WHEN (CAST(WEB_VISITORS AS BIGINT) >=1 AND CAST(WEB_VIEWERS AS BIGINT) >= 1) THEN CAST(COUNT_PLAIN_VIEWS AS BIGINT) ELSE 0 END) AS NUM_VIEWS_FUNNEL,SUM(CAST(DURATION_WATCHED_MS AS BIGINT))/1000 AS DURATION_WATCHED_SECS,SUM(CASE WHEN (CAST(WEB_VISITORS AS BIGINT) >=1 AND CAST(WEB_VIEWERS AS BIGINT) >= 1 AND CAST(VIDEO_WATCHED AS BIGINT) >=1 ) THEN CAST(DURATION_WATCHED_MS AS BIGINT)/1000 ELSE 0 END) AS DURATION_WATCHED_SECS_FUNNEL,COUNT(DISTINCT DISTINCT_ID) AS POWER_USERS, DATE_STAMP as date_part_col,'WEB' as project_part_col from GRAINWEB where DATE_STAMP = '$date_zero' GROUP BY date_stamp,PLATFORM,ORGANIC_INORGANIC_INSTALL_FLAG,NEW_REPEAT_FLAG,REG_USER_FLAG,DURATION_BUCKET,FREQUENCY_BUCKET,MONTHLY_POWER_USER,DAILY_POWER_USER,DAILY_POWER_USER_FLAG")


    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val count = sqlcontext.sql(s""" select count(*) from  F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH where date_part_col ='$date_zero' and project_part_col ='WEB'""").take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString

    sqlcontext.sql(s"""insert into DataTableloadHistory select CURRENT_DATE ,'$date_zero' ,'F_Agg_Show_Dly' ,'$Load_type','WEB', '$count','$st_time','$end_time','$min'  from  DataTableloadHistory  limit 1""")




  }
}