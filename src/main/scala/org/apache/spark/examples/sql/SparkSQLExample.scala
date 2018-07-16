package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession

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
}
