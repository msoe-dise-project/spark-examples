import org.apache.spark.sql.SparkSession

// This example loads a CSV file of real estate
// transactions as a DataFrame and uses the
// DataFrame API to remove outlier records
// The output is written in parquet format
object DataFrameExample1 {
  // Same definition of main as in Java
  // Takes an Array of strings and returns nothing (Unit)
  def main(args: Array[String]): Unit = {
    // The Spark system is started by creating a SparkSession
    // object.  This object is used to create and register
    // all distributed collections.
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[4]")
      .appName("DataFrameExample")
      .getOrCreate()

    // disable INFO log output from this point forward
    spark.sparkContext.setLogLevel("ERROR")

    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    // This is a DataFrame, which is really a type alias
    // for Dataset[Row]
    // A Row is a trait (interface)
    val df = spark.read
      // CSV File has a header row which specifies the column names
      .option("header", true)
      // infer the types of the columns
      .option("inferSchema", true)
      // perform the actual read
      .csv("Sacramentorealestatetransactions.csv")

    val nRows = df.count()

    println()
    println(s"The data set has ${nRows} rows.")
    println()

    println("The schema is:")
    df.printSchema()
    println()

    println("The first few rows are:")
    df.show()
    println()

    // short the first and last 5 values for each column
    for(columnName <- df.columns) {
      println(s"${columnName} column")
      df.select(df(columnName))
        .orderBy(df(columnName))
        .show(5)

      df.select(df(columnName))
        .orderBy(df(columnName).desc)
        .show(5)

      println()
    }

    // We should see that there are rows with:
    // 0 sq__ft
    // 0 beds
    // 0 baths
    // "Unkown" (misspelled) type
    // prices on the order of thousands
    // let's figure out how many there are of each

    val sqFt0Rows = df.filter("sq__ft = 0").count()
    println(s"There are ${sqFt0Rows} rows with 0 sq ft")
    println()

    val beds0Rows = df.filter("beds = 0").count()
    println(s"There are ${beds0Rows} rows with 0 beds")
    println()

    val baths0Rows = df.filter("baths = 0").count()
    println(s"There are ${baths0Rows} rows with 0 baths")
    println()

    val unknownTypeRows = df.filter("type = 'Unkown'").count()
    println(s"There are ${unknownTypeRows} rows with unknown types")
    println()

    val prices = df.select("price").map(row => row.getInt(0))
    val nDigits = prices.map(p => p.toString.length)

    // print histogram of prices
    nDigits.groupByKey(digits => digits)
      .reduceGroups((currentSum, _) => currentSum + 1)
      .toDF("NumberOfDigits", "Count")
      .orderBy("NumberOfDigits")
      .show()
    println()

    // We want to remove rows with outlier values
    val noOutliersDF = df.filter("price >= 100000")
      .filter("sq__ft > 0")
      .filter("baths > 0")
      .filter("beds > 0")
      .filter("type <> 'Unkown'")

    val countRemaining = noOutliersDF.count()

    println(s"${countRemaining} of ${nRows} rows remain after filtering")

    // parquet keeps column types
    // by default, Spark will refuse to overwrite
    // files.  Override this behavior so we can
    // re-run the program without manually deleting
    // the output
    noOutliersDF.write
      .mode("overwrite")
      .parquet("cleaned")

    spark.stop()
  }
}
