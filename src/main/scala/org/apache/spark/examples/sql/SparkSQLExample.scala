package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


object SparkSQLExample {
  case class Persion(name: String,age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    import spark.implicits._

    runBasicDataFrameExample(spark)
    runDatasetCreateionExample(spark)
    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)
    spark.stop()
  }

  private  def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("src/main/resources/people.json")
    df.show()

//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//      |  30|   Andy|
//      |  19| Justin|
//      +----+-------+
    import spark.implicits._

    df.printSchema()

//    root
//    |-- age: long (nullable = true)
//    |-- name: string (nullable = true)

    df.select("name").show()

//    +-------+
//    |   name|
//    +-------+
//    |Michael|
//    |   Andy|
//    | Justin|
//    +-------+

    df.select($"name",$"age" + 2).show()
//    +-------+---------+
//    |   name|(age + 2)|
//      +-------+---------+
//    |Michael|     null|
//      |   Andy|       32|
//      | Justin|       21|
//      +-------+---------+

    df.filter($"age" > 21).show()
//    +---+----+
//    |age|name|
//    +---+----+
//    | 30|Andy|
//      +---+----+
    df.groupBy("age").count().show()
//    | age|count|
//      |  19|    1|
//      |null|    1|
//      |  30|    1|
//      +----+-----+

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//      |  30|   Andy|
//      |  19| Justin|
//      +----+-------+

    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//      |  30|   Andy|
//      |  19| Justin|
//      +----+-------+

    spark.newSession().sql("select * from global_temp.people").show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//      |  30|   Andy|
//      |  19| Justin|
//      +----+-------+
  }

  private  def runDatasetCreateionExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val caseClassDS = Seq(Persion("Andy",32)).toDS()
    caseClassDS.show()
//    +----+---+
//    |name|age|
//    +----+---+
//    |Andy| 32|
//      +----+---+

    val primitiveDS = Seq(1,2,3).toDS()
    primitiveDS.map(_ + 1).collect()

    val path = "src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Persion]
    peopleDS.show()
//    +----+-------+
//    | age|   name|
//    +----+-------+
//    |null|Michael|
//      |  30|   Andy|
//      |  19| Justin|
//      +----+-------+
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    //create an rdd of persion objects from a text file.covert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Persion(attributes(0),attributes(1).trim.toInt))
      .toDF()

    //Register the Dataframe as a temporary view
    peopleDF.createOrReplaceTempView("people")

    //SQL statement can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("select name,age from people where age between 13 and 19")

    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
//    +------------+
//    |       value|
//    +------------+
//    |Name: Justin|
//      +------------+

    //or by filed name
    teenagersDF.map(teenager => "Nmae: " + teenager.getAs[String]("name")).show()
//    +------------+
//    |       value|
//    +------------+
//    |Nmae: Justin|
//      +------------+

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))
    // $example off:schema_inferring$
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._

    //create  an RDD
    val peopleRDD = spark.sparkContext.textFile("src/main/resources/people.txt")

    //the schemaString is encoded in a string
    val schemaString = "name age"

    //Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    //Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0),attributes(1).trim))

    //Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD,schema)

    //create a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    //SQL can be run over a temporary view create using DataFrames
    val results = spark.sql("select name from people")

    //The results of SQL queries are DataFrames and support all the normal RDD operations
    //The columes of a row in the result can be accessed by field index or by field name

    results.map(attributes => "Name: " + attributes(0)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
  }


}
