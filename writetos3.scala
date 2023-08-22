

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType,IntegerType}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object writetos3 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  

  
  val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "")
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
      
       val data = Seq(("James ","Rose","Smith","36636","M",3000),
      ("Michael","Rose","","40288","M",4000),
      ("Robert","Mary","Williams","42114","M",4000),
      ("Maria","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","1234","F",-1)
    )

    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    df.show()
    df.printSchema()

    df.write
      .parquet("s3a://sparkbyexamples1/parquet/people.parquet")
 

  
}