package com.viacom.vr.funnelShows

import org.apache.spark.sql.SparkSession

object ViewerRetentioncac {

  val spark = SparkSession
    .builder()
    .appName("ViewerRetention")
    .enableHiveSupport()
    .config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    .config("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb")
    .config("fs.azure.account.key.v18biblobstorprod.blob.core.windows.net", "K1ZcztvuFkq6Hy4P8Uf13kP3yXgXxQJBs/bKZ6Y4cfgzD/9nmjHg9uMwIbAe3ZC7vmCS59Mk/3iloAVOs3zdYQ==")
    .getOrCreate()

  val sqlcontext = spark.sqlContext

  def main(args: Array[String]) {

    val date_zero = args(0)
    val outputfilepath = args(1) //output directory path
    val showone = args(2)
    val showtwo = args(3)
    println("funnel start date ", date_zero)
    println("parquet file location to save ", outputfilepath)
    println("Show one  ",showone)
    println("Show Two ",showtwo)
    cac(date_zero, outputfilepath,showone,showtwo)

  }

  def cac(date_zero: String, outputfilepath: String, show_one: String, show_two: String ) = {

    // to read content mapper table
    println("Started to content Mapper to read table")
    val cmdf = sqlcontext.sql("select * from content_mapper")
    cmdf.createOrReplaceTempView("cmdf")
    println("FINISHED to content Mapper to read table")

    // 7 days dates generating from start date i.e date zero as argument passing
    val day1 = sqlcontext.sql(s""" select date_add('$date_zero',1)""").take(1)(0)
    val day11 = day1.get(0).toString
    val day2 = sqlcontext.sql(s""" select date_add('$date_zero',2)""").take(1)(0)
    val day12 = day2.get(0).toString
    val day3 = sqlcontext.sql(s""" select date_add('$date_zero',3)""").take(1)(0)
    val day13 = day3.get(0).toString
    val day4 = sqlcontext.sql(s""" select date_add('$date_zero',4)""").take(1)(0)
    val day14 = day4.get(0).toString
    val day5 = sqlcontext.sql(s""" select date_add('$date_zero',5)""").take(1)(0)
    val day15 = day5.get(0).toString
    val day6 = sqlcontext.sql(s""" select date_add('$date_zero',6)""").take(1)(0)
    val day16 = day6.get(0).toString

    // CAC level count
    // Seven days data with disitinct and by show name
    println("started to CAC count")
    val cacfunneldf = sqlcontext.sql(s" select distinct_id, media_id, event,date_stamp,cm.ref_series_title as show_name from voot_app_base tempbase INNER JOIN cmdf cm ON tempbase.media_id = cm.id where date_stamp >= '$date_zero' and date_stamp <= date_add('$date_zero',6) and event = 'mediaReady' and cm.ref_series_title IN ('$show_one','$show_two') ")
    cacfunneldf.createOrReplaceTempView("cacfunneldf")
    //for one day on distinct and event = mediaReady creating a base table
    val basecacdf = sqlcontext.sql(s" select distinct distinct_id,cm.ref_series_title as show_name from cacfunneldf afsdf INNER JOIN cmdf cm ON afsdf.media_id = cm.id where date_stamp = '$date_zero' and event='mediaReady' and cm.content_type <>  'Full Episode' and cm.ref_series_title IN ('$show_one','$show_two')")
    basecacdf.createOrReplaceTempView("basecacdf")
    //for seven day on distinct and event = mediaReady creating a base table
    val sevendaycacdf = sqlcontext.sql(s"select distinct afsdf.distinct_id, afsdf.date_stamp,afsdf.show_name from cacfunneldf afsdf INNER JOIN basecacdf bsdf ON afsdf.distinct_id = bsdf.distinct_id and afsdf.show_name = bsdf.show_name INNER JOIN cmdf cm ON afsdf.media_id = cm.id where afsdf.event='mediaReady' and cm.content_type <> 'Full Episode' and cm.ref_series_title IN ('$show_one','$show_two') group by  afsdf.distinct_id, afsdf.date_stamp,afsdf.show_name ")
    sevendaycacdf.createOrReplaceTempView("sevendaycacdf")
    // no of viewers for seven days
    val sevendf = sqlcontext.sql("select count(distinct_id) as viewers, show_name,date_stamp from sevendaycacdf group by show_name,date_stamp")
    sevendf.createOrReplaceTempView("sevendf")
    println("COMPLETED to CAC count")


    //Full Episode level percentage
    println("STARTED to CAC per")

    val cacfunnelperdf = sqlcontext.sql(s"select distinct_id,event,media_id, cm.ref_series_title as show_name ,cm.content_type as content_type, date_stamp, event,app_version,duration,duration_seconds from voot_app_base vab INNER JOIN cmdf cm ON vab.media_id = cm.id where date_stamp >= '$date_zero' and date_stamp <= date_add('$date_zero',6) and cm.ref_series_title IN ('$show_one','$show_two') group by distinct_id,media_id, cm.ref_series_title,date_stamp,cm.content_type, event,app_version,duration,duration_seconds")
    cacfunnelperdf.createOrReplaceTempView("cacfunnelperdf")

    val basecacperdf = sqlcontext.sql(s"select distinct_id,cm.ref_series_title as show_name from cacfunnelperdf afsdf INNER JOIN cmdf cm ON afsdf.media_id = cm.id where date_stamp = '$date_zero' and cm.content_type <>  'Full Episode' and cm.ref_series_title IN ('$show_one','$show_two') group by distinct_id,cm.ref_series_title")
    basecacperdf.createOrReplaceTempView("basecacperdf")

    val cacmediaperdf = sqlcontext.sql("select asdf.media_id,bspdf.show_name, bspdf.distinct_id, asdf.date_stamp,event,app_version,duration,duration_seconds from  basecacperdf bspdf INNER JOIN cacfunnelperdf asdf ON asdf.distinct_id = bspdf.distinct_id and asdf.show_name = bspdf.show_name where asdf.content_type <> 'Full Episode' group by asdf.media_id,bspdf.distinct_id,bspdf.show_name, asdf.date_stamp, event, app_version, duration, duration_seconds")
    cacmediaperdf.createOrReplaceTempView("cacmediaperdf")

    val cacperdf = sqlcontext.sql(s"select dzmdf.media_id, show_name,dzmdf.date_stamp,count(distinct dzmdf.distinct_id) as distinctid,cm.Content_Duration_sec,sum(case when app_version in ('47', '1.2.16','1.2.21') and event = 'Video Watched' and (cast(duration as bigint)between 0 and 36000)  then cast(duration as bigint)when app_version NOT in ('47', '1.2.16','1.2.21') and event = 'Video Watched' and (cast (duration_seconds  as bigint) between 0 and 36000) then cast(duration_seconds as bigint) end )as total_watched, (Content_Duration_sec/60) * count(distinct case when event='mediaReady' then dzmdf.distinct_id else NULL END)as show_comp from cacmediaperdf dzmdf INNER JOIN cmdf cm ON cm.id = dzmdf.media_id and cm.ref_series_title IN ('$show_one','$show_two') group by dzmdf.media_id,show_name, dzmdf.date_stamp,cm.Content_Duration_sec")
    cacperdf.createOrReplaceTempView("cacperdf")

    val caccompdf = sqlcontext.sql("select date_stamp, show_name,CASE WHEN SUM(CAST(show_comp AS DECIMAL(15,2))) = '0' THEN '0' ELSE (CAST((SUM(CAST(total_watched AS DECIMAL(15,2)))/60) AS DECIMAL(15,2)) / CAST(SUM(CAST(show_comp AS DECIMAL(15,2))) AS DECIMAL(15,2))) END  AS SHOW_COMP_RATE FROM cacperdf group by date_stamp, show_name")
    caccompdf.createOrReplaceTempView("caccompdf")

    val cacdf = sqlcontext.sql("select 'APP' as PROJECT, scdf.date_stamp, scdf.show_name,SHOW_COMP_RATE, viewers, 'CAC' as level_flag from caccompdf scdf INNER JOIN sevendf sdf ON scdf.date_stamp = sdf.date_stamp and scdf.show_name = sdf.show_name group by scdf.date_stamp, scdf.show_name,SHOW_COMP_RATE, viewers ")
    cacdf.createOrReplaceTempView("cacdf")

    // cacdf.printSchema()
    println("COMPLETED to CAC per")

    // pivot main
    println("STARTED to PIVOT cacviewrs")

    val cacviewrs = sqlcontext.sql(s"select PROJECT,'$date_zero' as DATE_VR,'' as WEEK_NUM, date_stamp,show_name,cast(viewers as int) as viewers,SHOW_COMP_RATE,level_flag from cacdf").groupBy("show_name", "level_flag", "PROJECT", "WEEK_NUM", "DATE_VR").pivot("date_stamp").sum("viewers")
    cacviewrs.createOrReplaceTempView("cacviewrs")
    // cacviewrs.printSchema()

    val cacviewrscol = sqlcontext.sql(s"""select PROJECT,level_flag,DATE_VR,WEEK_NUM,show_name,cast(`$date_zero` as string) as NUM_VIEWERS_DAY0,cast(`$day11` as string)as NUM_VIEWERS_DAY1,cast(`$day12` as string) as NUM_VIEWERS_DAY2,cast(`$day13` as string) as NUM_VIEWERS_DAY3,cast(`$day14` as string) as NUM_VIEWERS_DAY4,cast(`$day15` as string) as NUM_VIEWERS_DAY5,cast(`$day16` as string)as NUM_VIEWERS_DAY6 from cacviewrs """)
    cacviewrscol.createOrReplaceTempView("cacviewrscol")

    println("COMPLETED to PIVOT cac viewrs")

    // cacviewrscol.printSchema()
    println("STARTED to PIVOT cac per")

    val cacper = sqlcontext.sql("""select date_stamp,show_name,viewers,cast(SHOW_COMP_RATE as double),level_flag from cacdf""").groupBy("show_name", "level_flag").pivot("date_stamp").sum("SHOW_COMP_RATE")
    cacper.createOrReplaceTempView("cacper")
    //cacper.printSchema()

    val cacpercol = sqlcontext.sql(s"""select show_name,level_flag,cast(`$date_zero` as string) as COMP_RATE_DAY0,cast(`$day11` as string) as COMP_RATE_DAY1,cast(`$day12` as string) as COMP_RATE_DAY2,cast(`$day13` as string)as COMP_RATE_DAY3, cast(`$day14` as string) as COMP_RATE_DAY4, cast(`$day15` as string) as COMP_RATE_DAY5,cast(`$day16` as string)as COMP_RATE_DAY6 from cacper """)
    cacpercol.createOrReplaceTempView("cacpercol")
    //cacpercol.printSchema()

    val cacvw = sqlcontext.sql(s"select PROJECT,WEEK_NUM, vc.show_name as SHOW_NAME,DATE_VR, NUM_VIEWERS_DAY0 ,COMP_RATE_DAY0, NUM_VIEWERS_DAY1 ,COMP_RATE_DAY1,NUM_VIEWERS_DAY2,COMP_RATE_DAY2,NUM_VIEWERS_DAY3, COMP_RATE_DAY3, NUM_VIEWERS_DAY4,COMP_RATE_DAY4,NUM_VIEWERS_DAY5,COMP_RATE_DAY5,NUM_VIEWERS_DAY6,COMP_RATE_DAY6,pc.level_flag as level_flag from cacviewrscol vc JOIN cacpercol pc ON vc.show_name = pc.show_name and vc.level_flag = pc.level_flag")
    cacvw.createOrReplaceTempView("cacvw")
    println("COMPLETED to PIVOT cac per")

    println("Started to write parquet for cac vw")

    //Load Layer
    cacvw.write.format("parquet").mode("append").option("spark.sql.parquet.compression.codec", "snappy").save(outputfilepath)

    println("Completed to write parquet for cac vw")

    //Write to Parquet Files.
  }
}