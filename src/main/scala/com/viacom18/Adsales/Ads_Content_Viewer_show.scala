package com.viacom18.Adsales
import java.io.IOException
import java.util.concurrent.Executors
import scala.util.control.NonFatal

import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, _}

object Ads_Content_Viewer_show
{
  val spark = SparkSession.builder().appName("Ads_Content_Viewer_show").enableHiveSupport().getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String]) {
    val LoadType = args(0)
    val Load_Data_date = args(1)
    val adsalesmapper = sqlcontext.sql("select * from Adsales_Mapper where table = 'voot_adsales_content_viewer_show' and flag=1")
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


    val pool = Executors.newFixedThreadPool(12)
    implicit val xc = ExecutionContext.fromExecutorService(pool)


    sqlcontext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlcontext.sql("set hive.exec.dynamic.partition=true")


    val TQry1 = Qry1(startdate,enddate,display_value)
    val TQry2 = Qry2(startdate,enddate,display_value)
    val TQry3 = Qry3(startdate,enddate,display_value)
    val TQry4 = Qry4(startdate,enddate,display_value)
    val TQry5 = Qry5(startdate,enddate,display_value)
    val TQry6 = Qry6(startdate,enddate,display_value)
    val TQry7 = Qry7(startdate,enddate,display_value)
    val TQry8 = Qry8(startdate,enddate,display_value)
    val TQry9 = Qry9(startdate,enddate,display_value)
    val TQry10 = Qry10(startdate,enddate,display_value)
    val TQry11 = Qry11(startdate,enddate,display_value)
    val TQry12 = Qry12(startdate,enddate,display_value)
    val TQry13 = Qry13(startdate,enddate,display_value)
    val TQry14 = Qry14(startdate,enddate,display_value)
    val TQry15 = Qry15(startdate,enddate,display_value)
    val TQry16= Qry16(startdate,enddate,display_value)
    val TQry17 = Qry17(startdate,enddate,display_value)
    val TQry18 = Qry18(startdate,enddate,display_value)
    val TQry19 = Qry19(startdate,enddate,display_value)
    val TQry20 = Qry20(startdate,enddate,display_value)
    val TQry21 = Qry21(startdate,enddate,display_value)
    val TQry22 = Qry22(startdate,enddate,display_value)
    val TQry23 = Qry23(startdate,enddate,display_value)
    val TQry24 = Qry24(startdate,enddate,display_value)



    // Now wait for the tasks to finish before exiting the app
    Await.result(Future.sequence(Seq(TQry1,TQry2,TQry3,TQry4,TQry5,
      TQry6,TQry7,TQry8,TQry9,TQry10,TQry11,TQry12,TQry13,TQry14,
      TQry15,TQry16,TQry17,TQry18,TQry19,TQry20,TQry21,TQry22,
      TQry23,TQry24
    )),20.hours)

    println("END-",display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  }



  def Qry1(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future {
    println("Qry1-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    try {

      sqlcontext.sql(s""" insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
'All' State,
'All' City,
Show,
'All' Age,
'All' Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col , '01' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show """)

      println("Qry1-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    } catch   {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry1 failed")
    }
  }
  def Qry2(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {
    try {
      println("Qry2-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
      sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
'All' State,
'All' City,
 Show,
'All' Age,
'All' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City

, '$display_value' as days_part_col  , '02' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show """ )
      println("Qry2-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


    } catch   {
      case NonFatal(t) =>
        println(t.toString)
        println("Qry2 failed")    }
  }
  def Qry3(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry3-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
'All' State,
'All' City,
 Show,
 Age,
'All' Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '03' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,Age
""" )
    println("Qry3-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {
    case NonFatal(t) =>
      println("Qry3 failed")    }
  }
  def Qry4(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry4-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
'All' State,
'All' City,
Show,
 Age,
'All' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '04' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show ,Age """ )

    println("Qry4-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {
    case NonFatal(t) =>
      println("Qry4 failed")    }
  }
  def Qry5(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry5-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
'All' State,
'All' City,
 Show,
'All' Age,
Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '05' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,Gender  """ )
    println("Qry5-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {
    case NonFatal(t) =>
      println("Qry5 failed")    }
  }
  def Qry6(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry6-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)

select
'$display_value',
'WEB' Project,
'All' State,
'All' City,
 Show,
'All' Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '06' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
Show """ )
    println("Qry6-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


  } catch   {
    case NonFatal(t) =>
      println("Qry6 failed")    }
  }
  def Qry7(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry7-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)

select
'$display_value',
'APP' Project,
'All' State,
'All' City,
Show,
Age,
Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '07' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,age,gender  """ )

    println("Qry7-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {
    case e: IOException =>{ e.toString }
      println("Qry7 failed")
  }
  }
  def Qry8(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry8-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
'All' State,
'All' City,
 Show,
 Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '08' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,
age  """ )
    println("Qry8-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {     case e: IOException =>{ e.toString }
    println("Qry8 failed")    }
  }
  def Qry9(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry9-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
 State,
'All' City,
 Show,
'All' Age,
'All' Gender  ,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '09' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show ,state """ )


    println("Qry9-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


  } catch   {     case e: IOException =>{ e.toString }
    println("Qry9 failed")


  }
  }
  def Qry10(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry10-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
 State,
'All' City,
 Show ,
'All' Age,
'All' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '10' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,state """ )

    println("Qry10-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {     case e: IOException =>{ e.toString }
    println("Qry10 failed")    }
  }
  def Qry11(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {


    println("Qry11-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
State,
'All' City,
Show,
Age,
'All'  Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '11' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,state,age  """ )

    println("Qry11-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {     case e: IOException =>{ e.toString }
    println("Qry11 failed")    }
  }
  def Qry12(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry12-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
State,
'All' City,
Show ,
 Age,
'All' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '12' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,state,age """ )
    println("Qry12-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


  } catch   {     case e: IOException =>{ e.toString }
    println("Qry12 failed")    }
  }
  def Qry13(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry13-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
 State,
'All'  City,
 Show,
 Age,
Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '13' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,state,age,gender
""" )

    println("Qry13-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {     case e: IOException =>{ e.toString }
    println("Qry13 failed")    }
  }
  def Qry14(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {
    try {

      println("Qry14-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


      sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
 State,
'All' City,
Show ,
Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '14' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,state,age """ )

      println("Qry14-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    } catch   {     case e: IOException =>{ e.toString }
      println("Qry14 failed")    }
  }
  def Qry15(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry15-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))



    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
State,
'All' City,
 Show,
'All' Age,
 Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '15' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,state,gender """ )

    println("Qry15-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch
    {     case e: IOException =>{ e.toString }
      println("Qry15 failed")
    }
  }
  def Qry16(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry16-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
State,
'All' City,
 Show ,
'All'  Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
'All' City
, '$display_value' as days_part_col  , '16' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show ,state """ )

    println("Qry16-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch
    { case e: IOException =>{ e.toString }
      println("Qry16 failed")
    }
  }
  def Qry17(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry17-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))


    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
'All' State,
area,
Show,
'All'  Age,
'All'  Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '17' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
area,
city,
show """)
    println("Qry17-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {
    case e: IOException =>{ e.toString }
      println("Qry17 failed")
  }
  }
  def Qry18(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry18-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)

select
'$display_value',
'WEB' Project,
'All' State,
area,
 Show ,
'All' Age,
'All' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '18' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,area,city """)

    println("Qry18-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {     case e: IOException =>{ e.toString }
    println("Qry18 failed")    }
  }
  def Qry19(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry19-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'APP' Project,
'All' State,
area,
Show,
Age,
Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '19' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,area,
age,
gender, city """ )

    println("Qry19-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {     case e: IOException =>{ e.toString }
    println("Qry19 failed")    }
  }
  def Qry20(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {

    println("Qry20-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
'All' State,
area,
show ,
 Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '20' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,city,area,age
""" )
    println("Qry20-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {    case e: IOException =>{ e.toString }

    println("Qry20 failed")
  }
  }
  def Qry21(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry21-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
 select
'$display_value',
'APP' Project,
'All' State,
 area,
 Show,
Age,
'All'  Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end))
,city
, '$display_value' as days_part_col  , '21' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,
area,
age,city """ )

    println("Qry21-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {     case e: IOException =>{ e.toString }

    println("Qry21 failed")    }
  }
  def Qry22(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry22-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
'All' State,
 area,
 Show ,
Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '22' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,city,
age,area """ )

    println("Qry22-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {     case e: IOException =>{ e.toString }
    println("Qry22 failed")    }
  }
  def Qry23(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry23-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)

select
'$display_value',
'APP' Project,
'All' State,
area,
Show,
'All' Age,
Gender,
count(distinct(case when v.event = ('mediaReady') then v.distinct_id else '' end)),
sum((case when v.event = ('mediaReady') then 1 else 0 end)),
count(distinct(case when v.event IN('App Launched','App Access') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '23' as Qry_part_col from   voot_app_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show,
area,
gender,
city     """ )

    println("Qry23-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

  } catch   {
    case e: IOException =>{ e.toString }
      println("Qry23 failed")    }
  }
  def Qry24(startdate:String,enddate:String,display_value:String)(implicit xc: ExecutionContext) = Future
  {  try {
    println("Qry24-",startdate,enddate,display_value,java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))

    sqlcontext.sql( s"""  insert into voot_adsales_content_viewer_show  partition(days_part_col,Qry_part_col)
select
'$display_value',
'WEB' Project,
'All' State,
area,
Show ,
'All' Age,
'Not Tagged' Gender,
count(distinct(case when v.event = ('First Play') then v.distinct_id else '' end)),
sum((case when v.event = ('First Play') then 1 else 0 end)),
count(distinct(case when v.event IN ('Page Viewed') then v.distinct_id else '' end)),
city
, '$display_value' as days_part_col  , '24' as Qry_part_col from   voot_web_Adsales_base v
Where
v.date_part_col >= '$startdate' and v.date_part_col <= '$enddate'
group by
show, city, area """ )

    println("Qry24-finished",java.time.LocalDateTime.now.plusHours(5).plusMinutes(30))
  } catch   {
    case e: IOException =>{ e.toString }
      println("Qry24 failed")
  }
  }
}
