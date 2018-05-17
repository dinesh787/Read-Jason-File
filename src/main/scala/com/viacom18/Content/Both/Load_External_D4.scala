package com.viacom18.Content.Both

import org.apache.spark.sql.{SQLContext, SparkSession}

object Load_External_D4 {

  val spark = SparkSession.builder().appName("LoadintoexternalD4").enableHiveSupport()
    .getOrCreate()

  val sqlcontext: SQLContext = spark.sqlContext
  def main(args: Array[String])
  {
    val date_zero = args(0)
    val Load_type = args(1)
    println("Load into external for date:  ", date_zero)
    Load_data_internal_to_external(date_zero,Load_type)
  }

  def Load_data_internal_to_external(date_zero: String , Load_type : String) = {

    println("Data loading Start : ")
    val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString


    val day1 = sqlcontext.sql(s""" select date_sub('$date_zero',1)""").take(1)(0).get(0).toString
    val day2 = sqlcontext.sql(s""" select date_sub('$date_zero',2)""").take(1)(0).get(0).toString
    val day3 = sqlcontext.sql(s""" select date_sub('$date_zero',3)""").take(1)(0).get(0).toString

    //APP
    sqlcontext.sql(s""" insert overwrite table SPARK_F_Agg_Media_Dly select  project ,date_stamp ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  cac_fe_flag ,  genre ,content_language ,  show_name ,  media_id ,  content_media_name ,  telecast_date ,  episode_no ,  launch_start_date ,  release_year ,  content_duration_sec ,  num_viewers ,  num_views ,  duration_seconds ,  recent_7d ,  recent_14d ,  recent_30d ,  recent_90d  from F_Agg_Media_Dly where date_part_col in('$date_zero','$day1','$day2','$day3') and project_part_col ='APP'""")
    sqlcontext.sql(s""" insert overwrite table  SPARK_F_Agg_Media_Hrly select   project ,  cast(date_stamp as date) ,  hour ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  cac_fe_flag ,  genre ,  content_language ,  show_name ,  media_id ,  content_media_name ,  telecast_date ,  episode_no ,  launch_start_date , release_year ,  num_viewers ,  num_views ,  duration_seconds ,  recent_7d ,  recent_14d ,  recent_30d ,  recent_90d   from F_Agg_Media_Hrly where date_part_col in('$date_zero','$day1','$day2','$day3') and project_part_col ='APP'""")
    sqlcontext.sql(s""" insert overwrite table SPARK_F_Agg_Show_CAC_FE_Dly  select project ,  cast(date_stamp as date) ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  cac_fe_flag ,  genre , show_name ,  content_language ,  num_viewers ,  num_views ,  duration_seconds   from F_Agg_Show_CAC_FE_Dly where date_part_col in('$date_zero','$day1','$day2','$day3') and project_part_col ='APP'""")
    sqlcontext.sql(s""" insert overwrite table SPARK_F_Agg_Show_Dly select   program , cast( date_stamp as date) ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  genre , content_duration_sec ,  show_name ,  num_viewers ,  num_views ,  duration_seconds ,  content_language ,  recent_7d ,  recent_14d ,  recent_30d ,  recent_90d   from F_Agg_Show_Dly where date_part_col in('$date_zero','$day1','$day2','$day3') and project_part_col ='APP'""")
    sqlcontext.sql(s""" insert overwrite table SPARK_F_AGG_GENRE_TSV select 'APP' ,  date_part_col ,  sbu_cluster ,  genre ,  content_language ,  num_viewers ,  num_views ,duration_watched_secs , genre_tsv   from F_AGG_GENRE_TSV where date_part_col in('$date_zero','$day1','$day2','$day3') and project_part_col ='APP' """)

    //WEB
    sqlcontext.sql(s""" insert into table SPARK_F_Agg_Media_Dly select distinct project ,date_stamp ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  cac_fe_flag ,genre ,content_language ,  show_name ,  media_id ,  content_media_name ,  telecast_date ,  episode_no ,  launch_start_date ,  release_year ,  content_duration_sec ,  num_viewers ,  num_views ,  duration_seconds ,  recent_7d ,  recent_14d ,  recent_30d ,  recent_90d  from F_Agg_Media_Dly where date_part_col ='$date_zero' and project_part_col ='WEB'""")
    sqlcontext.sql(s""" insert into table SPARK_F_Agg_Media_Hrly select distinct   project , cast( date_stamp as date) ,  hour ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  cac_fe_flag ,  genre ,  content_language ,  show_name ,  media_id ,  content_media_name ,  telecast_date ,  episode_no ,  launch_start_date , release_year ,  num_viewers ,  num_views ,  duration_seconds ,  recent_7d ,  recent_14d ,  recent_30d ,  recent_90d   from F_Agg_Media_Hrly where date_part_col ='$date_zero' and project_part_col ='WEB'""".stripMargin)
    sqlcontext.sql(s""" insert into table SPARK_F_Agg_Show_CAC_FE_Dly select distinct  project , cast( date_stamp  as date),  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  cac_fe_flag ,  genre , show_name ,  content_language ,  num_viewers ,  num_views ,  duration_seconds   from F_Agg_Show_CAC_FE_Dly where date_part_col ='$date_zero' and project_part_col ='WEB'""")
    sqlcontext.sql(s""" insert into table SPARK_F_Agg_Show_Dly select distinct  program , cast( date_stamp as date) ,  sbu_cluster ,  sbu ,  channel_vendor_name ,  own_bought_flag ,  kids_flag ,  genre , content_duration_sec ,  show_name ,  num_viewers ,  num_views ,  duration_seconds ,  content_language ,  recent_7d ,  recent_14d ,  recent_30d ,  recent_90d   from F_Agg_Show_Dly where date_part_col ='$date_zero' and project_part_col ='WEB'""")

    println("Data loaded END : ")





  }

}
