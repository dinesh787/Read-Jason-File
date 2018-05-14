package com.viacom18.Adsales

import org.apache.spark.sql.{SQLContext, SparkSession}

object AdsalesViews1 {

  val spark = SparkSession.builder().appName("AdsalesViews1").enableHiveSupport().getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext
  def main(args: Array[String])
  {
    val LoadType = args(0)
    val Load_Data_date = args(1)
    val adsalesmapper = sqlcontext.sql("select * from Adsales_Mapper where table = 'voot_adsales_views_tab1' and flag=1")
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
    println("qry1")
    sqlcontext.sql( s""" INSERT into TABLE voot_adsales_views_tab1
                       SELECT
                       '$display_value' Recency_Date,
                       'App' Project,
                       case when  upper(ltrim(rtrim(ref_series_title))) is null then 'NOT TAGGED' else  upper(ltrim(rtrim(ref_series_title))) end Show,
                       case when s.sbu_new is null then 'NOT TAGGED' else s.sbu_new end SBU_CLUSTER,
                       case when m.language is null then 'NOT TAGGED' else m.language end language,
                       case when v.region is null then 'NOT TAGGED' else v.region end,
                       case when ltrim(rtrim(c.city)) is null then 'NOT TAGGED' else ltrim(rtrim(c.city)) end,
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
                       end),
                       (case when v.lr_gender = 'Male' then 'M'
                       when v.lr_gender = 'M' then 'M'
                       when v.lr_gender = 'Female' then 'F'
                       when v.lr_gender = 'F' then 'F' ELSE 'NOT TAGGED' end),
                       'NOT TAGGED',
                       count(*)
                       From
                       voot_app_base_event v  left join CONTENT_MAPPER m
                       on v.media_id = m.id
                       left join Adsales_sbu_mapper s
                       on s.SBU= m.SBU
                       left join city_mapper c on c.area = v.city
                       Where v.event IN ('mediaReady') and
                       v.date_stamp between '$startdate' and '$enddate'
                       group by  upper(ltrim(rtrim(ref_series_title))),
                       case when s.sbu_new is null then 'NOT TAGGED' else s.sbu_new end ,
                       case when m.language is null then 'NOT TAGGED' else m.language end ,
                       case when v.region is null then 'NOT TAGGED' else v.region end,
                       case when ltrim(rtrim(c.city)) is null then 'NOT TAGGED' else ltrim(rtrim(c.city)) end,
                       (case when v.lr_gender = 'Male' then 'M'
                       when v.lr_gender = 'M' then 'M'
                       when v.lr_gender = 'Female' then 'F'
                       when v.lr_gender = 'F' then 'F' ELSE 'NOT TAGGED' end),
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
                       end)

""")
    println("qry2")
    sqlcontext.sql( s""" INSERT   INTO TABLE voot_adsales_views_tab1
                       SELECT
                       '$display_value' Recency_Date,
                       'Web' Project,
                       case when upper(ltrim(rtrim(ref_series_title))) is null then 'NOT TAGGED' else  upper(ltrim(rtrim(ref_series_title))) end Show,
                       case when s.sbu_new is null then 'NOT TAGGED' else s.sbu_new end SBU_CLUSTER,
                       case when m.language is null then 'NOT TAGGED' else m.language end language,
                       case when v.region is null then 'NOT TAGGED' else v.region end,
                       case when ltrim(rtrim(c.city)) is null then 'NOT TAGGED' else ltrim(rtrim(c.city)) end,
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
                       end),
                        'NOT TAGGED' ,
                       'NOT TAGGED',
                        count(*)
                       From
                       voot_web_base_event v
                       left join content_mapper m
                       on v.media_id = m.id
                       left join Adsales_sbu_mapper s
                       on s.SBU= m.SBU left join city_mapper c on c.area = v.city
                       Where v.event IN ('First Play') and
                       v.date_stamp between '$startdate' and '$enddate'
                       group by  upper(ltrim(rtrim(ref_series_title))),
                       case when s.sbu_new is null then 'NOT TAGGED' else s.sbu_new end ,
                       case when m.language is null then 'NOT TAGGED' else m.language end ,
                       case when v.region is null then 'NOT TAGGED' else v.region end,
                       case when ltrim(rtrim(c.city)) is null then 'NOT TAGGED' else ltrim(rtrim(c.city)) end,
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
                       end) """)

  }
}
