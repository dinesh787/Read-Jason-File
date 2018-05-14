package com.viacom18.Adsales
import java.util.concurrent.Executors

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent._
import scala.util.control.NonFatal

object Ads_Viewer_city
{
  val spark = SparkSession.builder().appName("Ads_Viewer_city").enableHiveSupport().getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {
    val LoadType = args(0)
    val Load_Data_date = args(1)
    val adsalesmapper = sqlcontext.sql("select * from Adsales_Mapper where table = 'voot_adsales_viewer_city' and flag=1")
    var rowcount = sqlcontext.sql("select count(*) from Adsales_Mapper").take(1)(0)(0).toString.toInt
    adsalesmapper.select("Days", "recency_date").take(10).foreach(x => loadData(LoadType,Load_Data_date, x(0).toString, x(1).toString))
  }

  def loadData(loadType: String, Load_Data_date :String, recencyDate: String, display_value: String) = {

    println("Start-", display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    var startdate = ""
    var enddate = ""
    println(display_value)
    if (recencyDate.toInt == 101) {
      println(recencyDate)
      startdate = sqlcontext.sql(s""" select date_add('$Load_Data_date',1 - day('$Load_Data_date')) """).take(1)(0)(0).toString()
      enddate = sqlcontext.sql(s""" select '$Load_Data_date' """).take(1)(0)(0).toString()
    }
    else if (recencyDate.toInt == 102) {
      println(recencyDate)
      startdate = sqlcontext.sql(s"""  select concat(substring(ADD_MONTHS('$Load_Data_date',-1),1,7),'-01')  """).take(1)(0)(0).toString()
      enddate = sqlcontext.sql(s""" select   date_add('$Load_Data_date', -cast(day('$Load_Data_date') as int)) """).take(1)(0)(0).toString()
    }
    else {
      println(recencyDate)
      startdate = sqlcontext.sql(s""" select date_sub('$Load_Data_date','$recencyDate')""").take(1)(0)(0).toString()
      enddate = sqlcontext.sql(s""" select '$Load_Data_date' """).take(1)(0)(0).toString()
    }


    val pool = Executors.newFixedThreadPool(4)
    implicit val xc = ExecutionContext.fromExecutorService(pool)


    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")


  Qry1(startdate,enddate,display_value)
    Qry2(startdate,enddate,display_value)
   Qry3(startdate,enddate,display_value)
    Qry4(startdate,enddate,display_value)



    println("END-",display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  }


  def Qry1(startdate: String, enddate: String, display_value: String)= {
    println("Qry1-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
select '$display_value','APP' Project,'All' Show, 'All' SBU,'All' language,  area,count(distinct(distinct_id)),city,
'$display_value' as days_part_col  , '01' as Qry_part_col
From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col between '$startdate' and '$enddate'
group by area,city""")
      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
select '$display_value','WEB' Project,'All' Show, 'All' SBU,'All' language,  area,count(distinct(distinct_id)),city,
'$display_value' as days_part_col  , '01' as Qry_part_col
From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col between '$startdate' and '$enddate'
group by city,area """ )

      println("Qry1-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry1 failed")
    }
  }

  def Qry2(startdate: String, enddate: String, display_value: String) =  {
    println("Qry2-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
  SELECT
  '$display_value',
  'APP' Project,
  show,
  'All' sbu,
  'All' language,
  area,
  COUNT(DISTINCT (v.distinct_id )),
  city,'$display_value' as days_part_col  , '02' as Qry_part_col
  From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col between '$startdate' and '$enddate'
group by show ,area,city """)
sqlcontext.sql( s""" INSERT INTO TABLE voot_adsales_viewer_city partition (days_part_col, Qry_part_col)
 SELECT
  '$display_value',
  'WEB' Project,
  show,
  'All' SBU_CLUSTER,
'All' language,
  area,
  COUNT(DISTINCT (v.distinct_id )),city,'$display_value' as days_part_col  , '02' as Qry_part_col
  From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col between '$startdate' and '$enddate'
group by show ,area,city """ )

      println("Qry2-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry2 failed")
    }
  }

  def Qry3(startdate: String, enddate: String, display_value: String)=  {
    println("Qry3-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
  SELECT
  '$display_value',
  'APP' Project,
  'All' Show,
  SBU_CLUSTER,
  'All' language,
   area,
  COUNT(DISTINCT (v.distinct_id )),city,'$display_value' as days_part_col  , '03' as Qry_part_col
  From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col between '$startdate' and '$enddate'
group by
  SBU_CLUSTER,area,city """)
      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
  SELECT
  '$display_value',
  'WEB' Project,
 'All',
   SBU_CLUSTER,
'All' language,
  area,
  COUNT(DISTINCT (v.distinct_id )),city,'$display_value' as days_part_col  , '03' as Qry_part_col
  From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col between '$startdate' and '$enddate'
group by
  SBU_CLUSTER,area,city """ )

      println("Qry3-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry3 failed")
    }
  }

  def Qry4(startdate: String, enddate: String, display_value: String) =  {
    println("Qry4-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'APP' Project,
'All',
'All' SBU_CLUSTER,
 language,
area,
COUNT(DISTINCT (v.distinct_id )),city,'$display_value' as days_part_col  , '04' as Qry_part_col
From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col between '$startdate' and '$enddate'
group by
language,area,city
""")
      sqlcontext.sql( s""" INSERT  INTO TABLE voot_adsales_viewer_city  partition(days_part_col,Qry_part_col)
  SELECT
  '$display_value',
  'WEB' Project,
  'All',
  'All' SBU_CLUSTER,
language,area,
  COUNT(DISTINCT (v.distinct_id )),City,'$display_value' as days_part_col  , '04' as Qry_part_col
  From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col between '$startdate' and '$enddate'
group by
language,area,city """ )

      println("Qry4-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry4 failed")
    }
  }

}