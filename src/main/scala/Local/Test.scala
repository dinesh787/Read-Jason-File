package Local
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Test {
  import org.apache.spark.sql.{SQLContext, SparkSession}

    val spark = SparkSession.builder().master("local[*]").appName("Spark_Local").getOrCreate()
    val sqlcontext: SQLContext = spark.sqlContext

    def main(args: Array[String]): Unit = {
 /* val a= args(0);*/
      val st_time = sqlcontext.sql(s""" select CURRENT_TIMESTAMP """).take(1)(0).get(0).toString
      println("ok",st_time)

    }


}
