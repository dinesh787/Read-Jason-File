package com.viacom18.CMS


  import org.apache.spark.sql.SparkSession
  import java.util.concurrent.Executors

  import org.apache.spark.sql.{SQLContext, SparkSession}

  import scala.concurrent.duration.DurationInt
  import scala.concurrent.{Await, Future, _}
  import scala.util.control.NonFatal

  object CMS_bootstrap {

    def main(args: Array[String]) {



      val spark = SparkSession.builder.appName("CMS_bootStrap").enableHiveSupport().getOrCreate()
try{
      print("start Repair table")

      spark.sqlContext.sql(" msck repair table   VOOT_APP_BASE_EVENT               ").take(10).foreach(println)
      spark.sqlContext.sql(" msck repair table   VOOT_WEB_BASE_EVENT               ").take(10).foreach(println)
      spark.sqlContext.sql(" msck repair table   VOOT_APP_BASE                     ").take(10).foreach(println)
      spark.sqlContext.sql(" msck repair table   VOOT_WEB_BASE                     ").take(10).foreach(println)

     // spark.sqlContext.sql(" msck repair table   CMS_VIEWERS_MONTHLY               ").take(10).foreach(println)
      // spark.sqlContext.sql(" msck repair table   CMS_MONTHLY                       ").take(10).foreach(println)

    //  spark.sqlContext.sql(" msck repair table   CMS_VIEWERS_MONTHLY_SIDETOTAL_MID ").take(10).foreach(println)
     // spark.sqlContext.sql(" msck repair table   CMS_VIEWERS_MONTHLY_UPPERTOTAL1   ").take(10).foreach(println)

      print("End  Repair table")



















    } catch   {
      case NonFatal(t) =>
        println("************************-----------ERROR---------***************************")
        println("--")
        println("--")
        println(t.toString)

    }





    println("TestSuccessfull")
    }
  }