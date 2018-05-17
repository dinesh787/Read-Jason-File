package com.viacom18.Executive.app


import org.apache.spark.sql.{SQLContext, SparkSession}

object grain_app {

  val spark = SparkSession.builder().appName("grainapp").enableHiveSupport()
    //    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    //    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    //    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val Load_type = args(1)
    println("Grain App start date ", date_zero)
    grain_app(date_zero,Load_type)

  }

  def grain_app(date_zero: String,Load_type: String) = {


    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")
    //fetchig data from hive external table based on date_Stamp and storing in temp table
    val VOOTAPPBASE = sqlcontext.sql(s"select * from  voot_app_base_ext where date_stamp = '$date_zero'")
    VOOTAPPBASE.createOrReplaceTempView("VOOTAPPBASE")

    //selecting date_Stamp and distinct id columns and grouping those two columns from above temp tab
    val z = sqlcontext.sql("SELECT distinct date_stamp,distinct_id FROM VOOTAPPBASE GROUP BY date_stamp,distinct_id")
    z.createOrReplaceTempView("z")
    //counting number of records for app install and grouping distinct date_stamp,distinct_id columns
    val a = sqlcontext.sql("SELECT distinct date_stamp,distinct_id ,COUNT(*) AS APP_INSTALLS FROM VOOTAPPBASE WHERE upper(event) = 'APP INSTALL' GROUP BY date_stamp,distinct_id")
    a.createOrReplaceTempView("a")
    //counting number of records for app_visitors in specific events like ('APP LAUNCHED', 'APP OPENED', 'APP ACCESS')  and grouping distinct date_stamp,distinct_id columns
    val b = sqlcontext.sql("SELECT distinct date_stamp,distinct_id ,COUNT(*) AS APP_VISITORS FROM VOOTAPPBASE WHERE upper(event) IN ('APP LAUNCHED', 'APP OPENED', 'APP ACCESS') GROUP BY date_stamp,distinct_id")
    b.createOrReplaceTempView("b")
    //counting number of records for APP_VIEWERS and sum of count_plain from vootappbase in a mediaready event
    val c = sqlcontext.sql("SELECT distinct date_stamp,distinct_id ,COUNT(*) AS APP_VIEWERS,SUM(cast(COUNT_PLAIN as bigint)) AS COUNT_PLAIN_VIEWS FROM VOOTAPPBASE WHERE upper(event) IN ('MEDIAREADY') GROUP BY date_stamp,distinct_id")
    c.createOrReplaceTempView("c")
    //counting number of records for VIDEO_WATCHED and sum duration watched seconds from specif app versions in vootappbase VIDEO WATCHED event
    val d = sqlcontext.sql("SELECT distinct date_stamp,distinct_id,COUNT(*) AS VIDEO_WATCHED,SUM(CASE WHEN (app_version IN ('47','1.2.16','1.2.21') and (cast(duration as bigint) between 0 and 36000)) THEN cast(duration as bigint) WHEN (app_version NOT IN ('47','1.2.16','1.2.21') and (cast(duration_seconds as bigint) between 0 and 36000)) THEN cast(duration_seconds as bigint) else 0 END) AS duration_watched_secs FROM VOOTAPPBASE WHERE upper(event) = 'VIDEO WATCHED' GROUP BY date_stamp,distinct_id")
    d.createOrReplaceTempView("d")

    //select specific  columns (distinct_id and  date_stamp) where platform is not null
    val app = sqlcontext.sql("SELECT distinct app.distinct_id,app.date_stamp,upper(app.platform) AS PLATFORM FROM VOOTAPPBASE app WHERE platform IS NOT NULL GROUP BY app.distinct_id,app.date_stamp,upper(app.platform)")
    app.createOrReplaceTempView("app")

    //select specific  columns (distinct_id ,date_stamp,MANUFACTURER)
    val app_1 = sqlcontext.sql("SELECT distinct App.distinct_id,App.date_stamp,upper(App.manufacturer) AS MANUFACTURER FROM VOOTAPPBASE App GROUP BY App.distinct_id,App.date_stamp,upper(App.manufacturer)")
    app_1.createOrReplaceTempView("app_1")
    //join the two tables VOOTAPPBASE and Model_PriceBand_Mapper on model and Mobile_Model_Name
    val app_2 = sqlcontext.sql("SELECT distinct App.distinct_id,App.date_stamp,UPPER(PB.Price_Band) AS PRICE_BAND FROM VOOTAPPBASE App INNER JOIN Model_PriceBand_Mapper PB ON App.model = PB.Mobile_Model_Name GROUP BY App.distinct_id,App.date_stamp,UPPER(PB.Price_Band)")
    app_2.createOrReplaceTempView("app_2")
    //
    val app_3 = sqlcontext.sql("SELECT distinct App.distinct_id,App.date_stamp,MAX(app_version) AS app_version,upper(AV.PK_NonPK) AS APP_VER_PK_NPK_FLAG FROM VOOTAPPBASE App INNER JOIN AppVer_Mapper AV ON App.app_version = AV.AppVersion WHERE App.app_version IS NOT NULL GROUP BY App.distinct_id,App.date_stamp,upper(PK_NonPK)")
    app_3.createOrReplaceTempView("app_3")
    val app_4 = sqlcontext.sql("SELECT distinct App.distinct_id,App.date_stamp,upper(App.media_source) as media_source FROM VOOTAPPBASE App WHERE UPPER(APP.MEDIA_SOURCE) = 'ORGANIC' GROUP BY App.distinct_id,App.date_stamp,upper(App.media_source)")
    app_4.createOrReplaceTempView("app_4")
    val app_5 = sqlcontext.sql("SELECT distinct App.distinct_id,App.date_stamp,'TRUE' AS  first_app_launch FROM VOOTAPPBASE App WHERE App.date_stamp = SUBSTRING(App.first_app_launch_Date,1,10) GROUP BY App.distinct_id,App.date_stamp")
    app_5.createOrReplaceTempView("app_5")
    val app_6 = sqlcontext.sql("SELECT distinct App.distinct_id,App.date_stamp,1 AS IsRegistered FROM VOOTAPPBASE App WHERE (App.user_type is NOT NULL AND upper(App.user_type) <> 'GUEST' ) GROUP BY App.distinct_id,App.date_stamp")
    app_6.createOrReplaceTempView("app_6")

    val finalapp = sqlcontext.sql("select DISTINCT z.distinct_id,z.date_stamp,a.APP_INSTALLS,b.APP_VISITORS,c.APP_VIEWERS,c.COUNT_PLAIN_VIEWS,d.VIDEO_WATCHED,d.duration_watched_secs,app.PLATFORM as PLATFORM,app1.MANUFACTURER,case when app2.PRICE_BAND is null then 'UNKNOWN' else app2.PRICE_BAND end AS PRICE_BAND,CASE WHEN app3.APP_VER_PK_NPK_FLAG = NULL THEN 'UNKNOWN' ELSE app3.APP_VER_PK_NPK_FLAG END AS APP_VER_PK_NPK_FLAG,CASE WHEN app4.media_source = 'ORGANIC' THEN 'ORGANIC' ELSE 'INORGANIC' END AS ORGANIC_INORGANIC_INSTALL_FLAG,CASE WHEN app5.first_app_launch = 'TRUE' THEN 'NEW' ELSE 'REPEAT' END AS NEW_REPEAT_FLAG,CASE WHEN cast(app6.IsRegistered as bigint) = 1 THEN 'Y' ELSE 'N' END AS REG_USER_FLAG,0 AS duration_bucket,0 AS frequency_bucket,'N' AS MONTHLY_POWER_USER,CASE WHEN (cast(d.duration_watched_secs as bigint)/60) >= 30 THEN 'Power User' ELSE 'Non Power User' END AS DAILY_POWER_USER,CASE WHEN (cast(d.duration_watched_secs as bigint)/60) >= 30 AND  (cast(d.duration_watched_secs as bigint)/60) < 40 THEN '>=30Mins' WHEN (cast(d.duration_watched_secs as bigint)/60) >= 40 AND  (cast(d.duration_watched_secs as bigint)/60) < 50 THEN '>=40Mins' WHEN (cast(d.duration_watched_secs as bigint)/60) >= 50 AND  (cast(d.duration_watched_secs as bigint)/60) < 80 THEN '>=50Mins' WHEN (cast(d.duration_watched_secs as bigint)/60) >= 80  THEN '>=80Mins' ELSE 'N' END AS DAILY_POWER_USER_FLAG from z z left join a a on z.date_stamp = a.date_stamp and z.distinct_id = a.distinct_id left join b b on z.date_stamp = b.date_stamp and z.distinct_id = b.distinct_id left join c c on z.date_stamp = c.date_stamp and z.distinct_id = c.distinct_id left join d d on z.date_stamp = d.date_stamp and z.distinct_id = d.distinct_id left join app app on z.date_stamp = app.date_stamp and z.distinct_id = app.distinct_id left join app_1 app1 on z.date_stamp = app1.date_stamp and z.distinct_id = app1.distinct_id left join app_2 app2 on z.date_stamp = app2.date_stamp and z.distinct_id = app2.distinct_id left join app_3 app3 on z.date_stamp = app3.date_stamp and z.distinct_id = app3.distinct_id left join app_4 app4 on z.date_stamp = app4.date_stamp and z.distinct_id = app4.distinct_id left join app_5 app5 on z.date_stamp = app5.date_stamp and z.distinct_id = app5.distinct_id left join app_6 app6 on z.date_stamp = app6.date_stamp and z.distinct_id = app6.distinct_id")
    finalapp.createOrReplaceTempView("finalapp")


    val GRAINAPP = sqlcontext.sql("select * from  finalapp")
    GRAINAPP.createOrReplaceTempView("GRAINAPP")

    sqlcontext.sql(s"insert into F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH partition(date_part_col,project_part_col) SELECT 'App' AS PROJECT,DATE_STAMP AS DATE,PLATFORM AS PLATFORM,MANUFACTURER AS MANUFACTURER,PRICE_BAND AS PRICE_BAND,APP_VER_PK_NPK_FLAG AS APP_VER_PK_NPK_FLAG,ORGANIC_INORGANIC_INSTALL_FLAG AS ORGANIC_INORGANIC_INSTALL_FLAG,NEW_REPEAT_FLAG AS NEW_REPEAT_FLAG,REG_USER_FLAG AS REG_USER_FLAG,CASE WHEN DURATION_BUCKET IS NULL THEN 'OTHERS' ELSE DURATION_BUCKET END AS DURATION_BUCKET,FREQUENCY_BUCKET,MONTHLY_POWER_USER,DAILY_POWER_USER,DAILY_POWER_USER_FLAG,COUNT(APP_VISITORS) AS NUM_VISITORS,COUNT(APP_VIEWERS) AS NUM_VIEWERS,COUNT(APP_INSTALLS) AS NUM_INSTALLS,SUM(CASE WHEN (CAST(APP_INSTALLS AS BIGINT) >=1 AND CAST(APP_VISITORS AS BIGINT) >=1) THEN 1 ELSE 0 END) AS NUM_VISITORS_FUNNEL,SUM(CASE WHEN (CAST(APP_INSTALLS AS BIGINT) >=1 AND CAST(APP_VISITORS AS BIGINT) >=1 AND CAST(APP_VIEWERS AS BIGINT) >= 1) THEN 1 ELSE 0 END) AS NUM_VIEWERS_FUNNEL,COUNT(CASE WHEN (CAST(APP_INSTALLS AS BIGINT) >=1 AND CAST(APP_VISITORS AS BIGINT) >=1 AND CAST(APP_VIEWERS AS BIGINT) >= 1 AND CAST(VIDEO_WATCHED AS BIGINT) >=1 AND (CAST(duration_watched_secs AS BIGINT)/60) > 30 ) THEN distinct_id END) AS NUM_POWER_VIEWERS_FUNNEL,SUM(CAST(COUNT_PLAIN_VIEWS AS BIGINT)) AS NUM_VIEWS,SUM(CASE WHEN CAST(APP_INSTALLS AS BIGINT) >=1 AND CAST(APP_VISITORS AS BIGINT) >=1 AND CAST(APP_VIEWERS AS BIGINT) >= 1 THEN COUNT_PLAIN_VIEWS ELSE 0 END) AS NUM_VIEWS_FUNNEL,SUM(CAST(DURATION_WATCHED_SECS AS BIGINT)) AS DURATION_WATCHED_SECS,SUM(CASE WHEN (CAST(APP_INSTALLS AS BIGINT) >=1 AND CAST(APP_VISITORS AS BIGINT) >=1 AND CAST(APP_VIEWERS AS BIGINT) >= 1 AND CAST(VIDEO_WATCHED AS BIGINT) >=1) THEN CAST(DURATION_WATCHED_SECS AS BIGINT) ELSE 0 END) AS DURATION_WATCHED_SECS_FUNNEL,COUNT(DISTINCT DISTINCT_ID) AS POWER_USERS,DATE_STAMP as date_part_col,'APP' as project_part_col FROM GRAINAPP where DATE_STAMP = '$date_zero' GROUP BY DATE_STAMP,PLATFORM,MANUFACTURER,PRICE_BAND,APP_VER_PK_NPK_FLAG,ORGANIC_INORGANIC_INSTALL_FLAG,NEW_REPEAT_FLAG,REG_USER_FLAG,DURATION_BUCKET,FREQUENCY_BUCKET,MONTHLY_POWER_USER,DAILY_POWER_USER_FLAG,DAILY_POWER_USER")
    println("inserted into table F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH")


    val end_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
    val count = sqlcontext.sql(s""" select count(*) from  F_AGG_PRODUCT_DLY_DAY_GRAIN_BOTH where date_part_col ='$date_zero' and project_part_col ='APP'""").take(1)(0).get(0).toString
    val min = sqlcontext.sql(s"""SELECT (unix_timestamp('$end_time') - unix_timestamp('$st_time'))/3600""").take(1)(0).get(0).toString.toDouble.toInt.toString


    sqlcontext.sql(s"""insert into default.DataTableloadHistory partition(date_stamp) select CURRENT_DATE ,'$date_zero' ,'Executive_APP' ,'$Load_type','APP', '$count','$st_time','$end_time','$min','yes','grain_app','$date_zero' as date_stamp from  DataTableloadHistory  limit 1""")
    println("inserted into table default.DataTableloadHistory")




  }
}