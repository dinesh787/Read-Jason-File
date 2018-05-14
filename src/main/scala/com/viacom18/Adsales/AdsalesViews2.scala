package com.viacom18.Adsales

import org.apache.spark.sql.{SQLContext, SparkSession}

object AdsalesViews2 {

  val spark = SparkSession.builder().appName("AdsalesViews2").enableHiveSupport().getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext

  def main(args: Array[String])
  {
    val LoadType = args(0)
    val Load_Data_date = args(1)
    val adsalesmapper = sqlcontext.sql("select * from Adsales_Mapper where table = 'voot_adsales_views_tab2' and flag=1")
    var rowcount =  sqlcontext.sql("select count(*) from Adsales_Mapper").take(1)(0)(0).toString.toInt
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
    println(startdate,enddate)



    sqlcontext.sql( s""" INSERT   INTO TABLE voot_adsales_views_tab2
SELECT '$display_value','App' Project,case when nm.new_network is null then 'Not Tagged' else nm.new_network end Network,
case when mm.new_model is null then 'Not Tagged' else mm.new_model end Model,
(case when wifi like '%,%' then
(case when upper(substr(wifi,1,instr(wifi,',')-1))= 'TRUE' then 'Wifi' else 'Network' end) else
(case when upper(ltrim(rtrim(wifi))) = 'TRUE' then 'Wifi' else 'Network' end)
end),
count(*)
From
voot_app_base_event v  left join Network_Mapper NM on nm.network = v.carrier
left join ModelMapper mm on mm.model = v.model
Where v.event IN ('mediaReady') and
v.date_stamp between '$startdate' and '$enddate'
group by case when nm.new_network is null then 'Not Tagged' else nm.new_network end ,
case when mm.new_model is null then 'Not Tagged' else mm.new_model end ,
(case when wifi like '%,%' then
(case when upper(substr(wifi,1,instr(wifi,',')-1))= 'TRUE' then 'Wifi' else 'Network' end) else
(case when upper(ltrim(rtrim(wifi))) = 'TRUE' then 'Wifi' else 'Network' end)
end)
""")
    sqlcontext.sql( s""" INSERT   INTO TABLE voot_adsales_views_tab2
SELECT '$display_value','Web' Project,'Not Tagged' Network,
'Not Tagged' Model,
'Not Tagged' Wifi_Network,
count(*)
From
voot_web_base_event v
Where v.event IN ('First Play') and
v.date_stamp between '$startdate' and '$enddate' """)

  }
}
