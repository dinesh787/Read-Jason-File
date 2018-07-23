package Local.ReadJason
import org.apache.spark.sql.{SQLContext, SparkSession}

object ReadJason {


 // import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.sql.{SQLContext, SparkSession}

  val spark = SparkSession.builder().master("local[*]").appName("Spark_Local").getOrCreate()
  val sqlcontext: SQLContext = spark.sqlContext

 // def main(args: Array[String]): Unit = {


    val dfs = sqlcontext.read.json("C:/Users/indian/Desktop/json/example_1.json")

    dfs.printSchema()
    dfs.show()
  }


