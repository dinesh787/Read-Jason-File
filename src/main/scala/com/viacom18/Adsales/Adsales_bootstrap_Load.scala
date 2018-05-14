
package com.viacom18.Adsales
import java.util.concurrent.Executors

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, _}
import scala.util.control.NonFatal

object Adsales_bootstrap_Load {

  val spark = SparkSession.builder().appName("Ads_bootstrap").enableHiveSupport().getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {
    val LoadType = args(0)
    val Load_Data_date = args(1)
    try{

   val startdate = sqlcontext.sql(s""" select date_sub('$Load_Data_date',90)""").take(1)(0)(0).toString()
   val  enddate = sqlcontext.sql(s""" select '$Load_Data_date' """).take(1)(0)(0).toString()

    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")
     // Load 90 days base data

   println(" verify voot_app_base_event :")
   sqlcontext.sql(" select count(*),date_Stamp from  voot_app_base_event group by date_Stamp order by date_Stamp ").take(1000).foreach(println)
   println("verify voot_web_base_event :")
   sqlcontext.sql(" select count(*),date_Stamp from  voot_web_base_event group by date_Stamp order by date_Stamp ").take(1000).foreach(println)



   println("verify mapper tables : ")

   sqlcontext.sql("select count(*) from Adsales_Mapper   ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from CONTENT_MAPPER     ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from ADSALES_CONTENT_MAPPER ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from Adsales_sbu_mapper ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from city_mapper        ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from Network_Mapper     ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from ModelMapper        ").take(1000).foreach(println)
   sqlcontext.sql("select count(*) from adsales_state        ").take(1000).foreach(println)



   println("start Drop table ")

    sqlcontext.sql(s""" drop table IF EXISTS `voot_app_Adsales_base` """).take(1000).foreach(println)
    println("app adsales base droped")

      sqlcontext.sql(s""" drop table IF EXISTS `voot_web_Adsales_base` """).take(1000).foreach(println)
    println("web adsales base droped")


    println( "create base table ")
    sqlcontext.sql(s""" CREATE external TABLE `voot_app_Adsales_base`(
  `distinct_id` string,
  `date_stamp` string,
  `event` string,
  `sbu_cluster` string,
  `language` string,
  `state` string,
  `area` string,
  `city` string,
  `age` string,
  `gender` string,
  `show` string,
  `carrier` string,
  `wifi` string,
  `model` string)
PARTITIONED BY (
  `date_part_col` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Run$Load_Data_date/voot_app_Adsales_base' """).take(1000).foreach(println)

    println("app adsales base created")
    sqlcontext.sql(s""" CREATE external TABLE `voot_web_Adsales_base`(
  `distinct_id` string,
  `date_stamp` string,
  `event` string,
  `sbu_cluster` string,
  `language` string,
  `state` string,
  `area` string,
  `city` string,
  `age` string,
  `show` string)
PARTITIONED BY (
  `date_part_col` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Run$Load_Data_date/voot_web_Adsales_base' """).take(1000).foreach(println)
    println("web adsales base created")



    println( "App Base data Load started")
    sqlcontext.sql(s"""  insert  Overwrite  table voot_app_Adsales_base  partition(date_part_col)
select
distinct_id,
date_stamp,
event,
case when s.sbu_new is null then 'NOT TAGGED' else s.sbu_new end  as SBU_CLUSTER,
case when m.language is null then 'NOT TAGGED' else m.language end as  language,
case when UPPER(LTRIM(RTRIM(d.state))) is null then 'NOT TAGGED' else UPPER(LTRIM(RTRIM(d.state))) end  as state,
case when UPPER(LTRIM(RTRIM(v.city))) is null then 'NOT TAGGED' else UPPER(LTRIM(RTRIM(v.city))) end  as area,
case when UPPER(LTRIM(RTRIM(c.city))) is null then 'NOT TAGGED' else UPPER(LTRIM(RTRIM(c.city))) end  as city,
(case
when v.lr_age between '15' and '17' then '15-17'
when v.lr_age between '18' and '21' then '18-21'
when v.lr_age between '22' and '25' then '22-25'
when v.lr_age between '26' and '30' then '26-30'
when v.lr_age between '31' and '35' then '31-35'
when v.lr_age between '36' and '40' then '36-40'
when v.lr_age between '41' and '45' then '41-45'
when v.lr_age between '46' and '60' then '46-60'
else 'NOT TAGGED'
end) as age ,
(
case when UPPER(v.lr_gender) = 'MALE' then 'M'
when UPPER(v.lr_gender) = 'M' then 'M'
when UPPER(v.lr_gender) = 'FEMALE' then 'F'
when UPPER(v.lr_gender) = 'F' then 'F' ELSE 'NOT TAGGED' end) as gender,
case when UPPER(LTRIM(RTRIM(m.ref_series_title))) is null then 'Not Tagged' else UPPER(LTRIM(RTRIM(m.ref_series_title))) end show,
`carrier`,
`wifi`,
model,
cast(cast(`date_stamp` as date) as String) as date_part_col
From voot_app_base_event v  left join CONTENT_MAPPER m
on v.media_id = m.id
left join Adsales_sbu_mapper s
on s.SBU= m.SBU
left join city_mapper c on c.area = v.city
left join adsales_state d on d.state = v.region
Where  v.event IN ('App Launched','App Access','mediaReady')
and date_stamp >= '$startdate' and date_stamp <= '$enddate'
 """).take(1000).foreach(println)
    sqlcontext.sql(s""" select distinct date_part_col from voot_app_Adsales_base order by date_part_col  """).take(100).foreach(println)

    println( "Web Base data Load started")
    sqlcontext.sql(s"""  insert Overwrite  table  voot_web_Adsales_base  partition(date_part_col)
select
distinct_id,
date_stamp,
`event` ,
case when s.sbu_new is null then 'NOT TAGGED' else s.sbu_new end  as SBU_CLUSTER,
case when m.language is null then 'NOT TAGGED' else m.language end as  language,
case when UPPER(LTRIM(RTRIM(d.state))) is null then 'NOT TAGGED' else UPPER(LTRIM(RTRIM(d.state))) end  as state,
case when UPPER(LTRIM(RTRIM(v.city))) is null then 'NOT TAGGED' else UPPER(LTRIM(RTRIM(v.city))) end  as area,
case when UPPER(LTRIM(RTRIM(c.city))) is null then 'NOT TAGGED' else UPPER(LTRIM(RTRIM(c.city))) end  as city,
(case
when v.lr_age between '15' and '17' then '15-17'
when v.lr_age between '18' and '21' then '18-21'
when v.lr_age between '22' and '25' then '22-25'
when v.lr_age between '26' and '30' then '26-30'
when v.lr_age between '31' and '35' then '31-35'
when v.lr_age between '36' and '40' then '36-40'
when v.lr_age between '41' and '45' then '41-45'
when v.lr_age between '46' and '60' then '46-60'
else 'NOT TAGGED'
end) as age ,
case when UPPER(LTRIM(RTRIM(m.ref_series_title))) is null then 'Not Tagged' else UPPER(LTRIM(RTRIM(m.ref_series_title))) end ref_series_title,
cast(cast(`date_stamp` as date) as String) as date_part_col
From voot_web_base_event v  left join CONTENT_MAPPER m
on v.media_id = m.id
left join Adsales_sbu_mapper s
on s.SBU= m.SBU
left join city_mapper c on c.area = v.city
left join adsales_state d on d.state = v.region
Where  v.event IN ('Page Viewed','First Play')
and date_stamp >= '$startdate' and date_stamp <= '$enddate'
 """).take(1000).foreach(println)
    sqlcontext.sql(s""" select distinct date_part_col from voot_web_Adsales_base order by date_part_col""").take(100).foreach(println)


    //Drop  Agggregated table

    println("start Drop table ")

    sqlcontext.sql(s""" drop table IF EXISTS  voot_adsales_views_tab1        """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS  voot_adsales_views_tab2          """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS  voot_adsales_viewer_age         """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS voot_adsales_viewer_city         """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS  voot_adsales_viewer_region       """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS  voot_adsales_viewer_all        """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS voot_adsales_content_viewer_all  """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS voot_adsales_content_viewer_state""").take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS  voot_adsales_content_viewer_show """).take(1000).foreach(println)

    sqlcontext.sql(s""" drop table IF EXISTS voot_adsales_content_viewer_city """).take(1000).foreach(println)


    // Create

    println("start Create  table ")


    sqlcontext.sql(s""" CREATE EXTERNAL TABLE `voot_adsales_views_tab1`(
      `recency_date` string,
      `project` string,
      `ref_series_title` string,
      `sbu_cluster` string,
      `language` string,
      `region` string,
      `city` string,
      `lr_age` string,
      `lr_gender` string,
      `lr_country` string,
      `views` double)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_views_tab1'""").take(1000).foreach(println)

    sqlcontext.sql(s"""  CREATE EXTERNAL TABLE `voot_adsales_views_tab2`(
      `recency_date` string,
      `project` string,
      `telco` string,
      `devices` string,
      `wifi_network` string,
      `views` double)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_views_tab2'""").take(1000).foreach(println)


    sqlcontext.sql(s""" CREATE EXTERNAL TABLE `voot_adsales_viewer_age`(
      `recency_date` string,
      `project` string,
      `ref_series_title` string,
      `sbu` string,
      `language` string,
      `age` string,
      `gender` string,
      `viewers` string)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_viewer_age'""").take(1000).foreach(println)

    sqlcontext.sql(s""" CREATE EXTERNAL TABLE `voot_adsales_viewer_city`(
      `recency_date` string,
      `project` string,
      `show` string,
      `sbu` string,
      `language` string,
      `area` string,
      `viewers` bigint,
      `city` string)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_viewer_city'""").take(1000).foreach(println)

    sqlcontext.sql(s""" CREATE EXTERNAL TABLE `voot_adsales_viewer_region`(
      `recency_date` string,
      `project` string,
      `show` string,
      `sbu` string,
      `language` string,
      `region` string,
      `viewers` bigint)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_viewer_region'""").take(1000).foreach(println)

    sqlcontext.sql(s"""  CREATE EXTERNAL TABLE `voot_adsales_viewer_all`(
      `recency_date` string,
      `project` string,
      `ref_series_title` string,
      `sbu` string,
      `language` string,
      `viewers` string)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_viewer_all'""").take(1000).foreach(println)


    sqlcontext.sql(s"""   CREATE EXTERNAL TABLE `voot_adsales_content_viewer_all`(
      `recency_date` string,
      `project` string,
      `region` string,
      `area` string,
      `show` string,
      `age` string,
      `gender` string,
      `viewers` double,
      `views` double,
      `visitors` double,
      `city` string)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_content_viewer_all'""").take(1000).foreach(println)

    sqlcontext.sql(s"""  CREATE EXTERNAL TABLE `voot_adsales_content_viewer_state`(
      `recency_date` string,
      `project` string,
      `region` string,
      `city` string,
      `show` string,
      `age` string,
      `gender` string,
      `viewers` double,
      `views` double,
      `visitors` double)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_content_viewer_state'""").take(1000).foreach(println)

    sqlcontext.sql(s"""  CREATE EXTERNAL TABLE `voot_adsales_content_viewer_city`(
      `recency_date` string,
      `project` string,
      `region` string,
      `area` string,
      `show` string,
      `age` string,
      `gender` string,
      `viewers` double,
      `views` double,
      `visitors` double,
      `city` string)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_content_viewer_city'""").take(1000).foreach(println)

    sqlcontext.sql(s"""CREATE EXTERNAL TABLE `voot_adsales_content_viewer_show`(
      `recency_date` string,
      `project` string,
      `region` string,
      `area` string,
      `show` string,
      `age` string,
      `gender` string,
      `viewers` double,
      `views` double,
      `visitors` double,
      `city` string)
    PARTITIONED BY (`days_part_col` string,`Qry_part_col` string)
    ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
    'wasb://viacom18@v18biblobstorprod.blob.core.windows.net/voot/Adsales/Run$Load_Data_date/voot_adsales_content_viewer_show'""").take(1000).foreach(println)




    println("verify agg tables")

    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_views_tab1 group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_views_tab2 group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_viewer_age group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_viewer_city group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_viewer_region group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_viewer_all group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_content_viewer_all group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_content_viewer_state group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_content_viewer_city group by recency_date order by recency_date ").take(1000).foreach(println)
    sqlcontext.sql(" select count(*),recency_date from  voot_adsales_content_viewer_show group by recency_date order by recency_date ").take(1000).foreach(println)


    sqlcontext.sql(" show create table  voot_adsales_views_tab1   ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_views_tab2  ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_viewer_age ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_viewer_city  ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_viewer_region ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_viewer_all ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_content_viewer_all  ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_content_viewer_state ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_content_viewer_city ").take(1000).foreach(println)
    sqlcontext.sql(" show create table  voot_adsales_content_viewer_show  ").take(1000).foreach(println)

    println("*******----------verify agg tables OVER------------*********** ")

    Thread.sleep(300000)

    } catch   {
      case NonFatal(t) =>
        println("************************-----------ERROR---------***************************")
        println("--")
        println("--")
        println(t.toString)

    }

  }

}
