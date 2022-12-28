import org.apache.spark.sql.SparkSession

// This example loads the cleaned real estate
// transactions from parquet files and zip code
// populations from a CSV file.  The two tables
// are joined via an outer left join.  The
// resulting joined table is written to a JSON file
// (text file with one JSON record per line)
object JoinExample {
  // Same definition of main as in Java
  // Takes an Array of strings and returns nothing (Unit)
  def main(args: Array[String]): Unit = {
    // The Spark system is started by creating a SparkSession
    // object.  This object is used to create and register
    // all distributed collections.
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[2]")
      .appName("DataFrameExample")
      .getOrCreate()

    // disable INFO log output from this point forward
    spark.sparkContext.setLogLevel("ERROR")

    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    val zipcodePopulationsDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("zipcode_populations.csv")

    println("The schema is:")
    zipcodePopulationsDF.printSchema()
    println()

    // This is a DataFrame, which is really a type alias
    // for Dataset[Row]
    // A Row is a trait (interface)
    val cleanedTransactionsDF = spark.read
      .parquet("cleaned")

    // make column names consistent
    val renamedColumnDF = cleanedTransactionsDF.withColumnRenamed("zip", "zipcode")

    // perform left outer join using the zipcode column
    val joinedDF = renamedColumnDF.join(zipcodePopulationsDF, Seq("zipcode"), "left_outer")

    // did we lose any records?
    val countBefore = cleanedTransactionsDF.count()
    val countAfter = joinedDF.count()

    println(s"${countAfter} of ${countBefore} rows survived join")
    println()

    // show schema after join
    joinedDF.printSchema()

    // any null values because a zipcode in the real estate table was missing?
    val nullCount = joinedDF.filter("population IS NULL").count()

    println(s"${nullCount} of ${countAfter} rows have a null population size")
    println()

    // by default, Spark will refuse to overwrite
    // files.  Override this behavior so we can
    // re-run the program without manually deleting
    // the output
    joinedDF.write
      .mode("overwrite")
      .json("joined")

    spark.stop()
  }
}
