package com.viacom18.Adsales
import java.util.concurrent.Executors

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, _}
import scala.util.control.NonFatal

object Ads_Viewer_age {
  val spark = SparkSession.builder().appName("Ads_Viewer_age").enableHiveSupport().getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {
    val LoadType = args(0)
    val Load_Data_date = args(1)
    val adsalesmapper = sqlcontext.sql("select * from Adsales_Mapper where table = 'voot_adsales_viewer_age' and flag=1")
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


    val pool = Executors.newFixedThreadPool(8)
    implicit val xc = ExecutionContext.fromExecutorService(pool)


    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")


    val TQry1 = Qry1(startdate, enddate, display_value)
    val TQry2 = Qry2(startdate, enddate, display_value)
    val TQry3 = Qry3(startdate, enddate, display_value)
    val TQry4 = Qry4(startdate, enddate, display_value)
    val TQry5 = Qry5(startdate, enddate, display_value)
    val TQry6 = Qry6(startdate, enddate, display_value)
    val TQry7 = Qry7(startdate, enddate, display_value)
    val TQry8 = Qry8(startdate, enddate, display_value)




    // Now wait for the tasks to finish before exiting the app
    Await.result(Future.sequence(Seq(TQry1, TQry2, TQry3, TQry4, TQry5,
      TQry6, TQry7, TQry8
    )), 20.hours)

    println("END-", display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  }


  def Qry1(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry1-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {
println("Qry1")
sqlcontext.sql(
s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
select  '$display_value','APP' Project,'All' Show, 'All' SBU,'All' language,   Age,
Gender,count(distinct(distinct_id)),'$display_value' as days_part_col  , '01' as Qry_part_col
From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by age,gender
""")


      println("Qry1-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry1 failed")
    }
  }

  def Qry2(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry2-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql(
        s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
select  '$display_value','WEB' Project,'All' Show, 'All' SBU,'All' language,  Age, 'Not Tagged'  Gender,count(distinct(distinct_id)),
'$display_value' as days_part_col  , '02' as Qry_part_col
From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by age
""")

      println("Qry2-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry2 failed")
    }
  }

  def Qry3(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry3-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {
      sqlcontext.sql(
        s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'APP' Project,
show,
'All' SBU_CLUSTER,
'All' language,Age,
Gender,
count(distinct(distinct_id)),'$display_value' as days_part_col  , '03' as Qry_part_col
From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by show ,age,gender
""")

      println("Qry3-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry3 failed")
    }
  }

  def Qry4(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry4-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {
      sqlcontext.sql(
        s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'WEB' Project,
show,
'All' SBU_CLUSTER,
'All' language,
Age, 'Not Tagged' Gender,
COUNT(DISTINCT (v.distinct_id )),'$display_value' as days_part_col  , '04' as Qry_part_col
From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by show,Age
""")

      println("Qry4-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry4 failed")
    }
  }

  def Qry5(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry5-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {
sqlcontext.sql(
 s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'APP' Project,
'All' Show,
 SBU_CLUSTER,
'All' language,
 Age,
Gender,
count(distinct(distinct_id)),'$display_value' as days_part_col  , '05' as Qry_part_col
From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by age,gender,SBU_CLUSTER
""")

      println("Qry5-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry5 failed")
    }
  }

  def Qry6(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry6-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql(
        s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'WEB' Project,
'All' Show,
SBU_CLUSTER,
'All' language,
 Age, 'Not Tagged' Gender,
COUNT(DISTINCT (v.distinct_id )),'$display_value' as days_part_col  , '06' as Qry_part_col
From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
 SBU_CLUSTER,age
""")
      println("Qry6-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry6 failed")
    }
  }

  def Qry7(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry7-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql(
        s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'APP' Project,
'All' Show,
'All' SBU_CLUSTER,
language,
 Age,
 Gender,
count(distinct(distinct_id)),'$display_value' as days_part_col  , '07' as Qry_part_col
From voot_app_Adsales_base v
Where v.event = ('mediaReady') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
language,age,gender
""")
      println("Qry7-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry7 failed")
    }
  }

  def Qry8(startdate: String, enddate: String, display_value: String)(implicit xc: ExecutionContext) = Future {
    println("Qry8-", startdate, enddate, display_value, java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {
      sqlcontext.sql(
 s""" INSERT into TABLE voot_adsales_viewer_age partition(days_part_col,Qry_part_col)
SELECT
'$display_value',
'WEB' Project,
'All',
'All' SBU_CLUSTER,
language,Age, 'Not Tagged' Gender,
COUNT(DISTINCT (v.distinct_id )),'$display_value' as days_part_col  , '08' as Qry_part_col
From voot_web_Adsales_base v
Where v.event = ('First Play') and
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
language,age
""")

      println("Qry8-finished", java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry8 failed")
    }
  }

}

