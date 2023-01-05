import org.apache.spark.sql.SparkSession

// This example loads a CSV file of real estate
// transactions as a DataFrame and uses the
// DataFrame API to remove outlier records.
// A second CSV file containing populations by zip code
// is loaded and joined to the original table.
// The output is written in JSON format.
object DataFrameExample2 {
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
    val housingPrices = spark.read
      // CSV File has a header row which specifies the column names
      .option("header", true)
      // infer the types of the columns
      .option("inferSchema", true)
      // perform the actual read
      .csv("Sacramentorealestatetransactions.csv")

    val nRows = housingPrices.count()

    println()
    println(s"The data set has ${nRows} rows.")
    println()

    println("The schema is:")
    housingPrices.printSchema()
    println()

    println("The first few rows are:")
    housingPrices.show()
    println()

    // short the first and last 5 values for each column
    for(columnName <- housingPrices.columns) {
      println(s"${columnName} column")
      housingPrices.select(housingPrices(columnName))
        .orderBy(housingPrices(columnName))
        .show(5)

      housingPrices.select(housingPrices(columnName))
        .orderBy(housingPrices(columnName).desc)
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

    val sqFt0Rows = housingPrices.filter("sq__ft = 0").count()
    println(s"There are ${sqFt0Rows} rows with 0 sq ft")
    println()

    val beds0Rows = housingPrices.filter("beds = 0").count()
    println(s"There are ${beds0Rows} rows with 0 beds")
    println()

    val baths0Rows = housingPrices.filter("baths = 0").count()
    println(s"There are ${baths0Rows} rows with 0 baths")
    println()

    val unknownTypeRows = housingPrices.filter("type = 'Unkown'").count()
    println(s"There are ${unknownTypeRows} rows with unknown types")
    println()

    val prices = housingPrices.select("price").map(row => row.getInt(0))
    val nDigits = prices.map(p => p.toString.length)

    // print histogram of prices
    nDigits.groupByKey(digits => digits)
      .reduceGroups((currentSum, _) => currentSum + 1)
      .toDF("NumberOfDigits", "Count")
      .orderBy("NumberOfDigits")
      .show()
    println()

    // We want to remove rows with outlier values
    val cleanedHousingPrices = housingPrices.filter("price >= 100000")
      .filter("sq__ft > 0")
      .filter("baths > 0")
      .filter("beds > 0")
      .filter("type <> 'Unkown'")

    val countRemaining = cleanedHousingPrices.count()

    println(s"${countRemaining} of ${nRows} rows remain after filtering")

    println()
    println("Reading the zip code population data")

    // read in file with two columns:
    // zip codes and population sizes
    val zipcodePops = spark.read
      // CSV File has a header row which specifies the column names
      .option("header", true)
      // infer the types of the columns
      .option("inferSchema", true)
      // perform the actual read
      .csv("zipcode_populations.csv")

    println("The schema is:")
    zipcodePops.printSchema()
    println()

    println("The first few rows are:")
    zipcodePops.show()
    println()

    println("Joining the two tables")
    val joined = cleanedHousingPrices.join(zipcodePops,
      cleanedHousingPrices("zip") === zipcodePops("zipcode"), "left_outer")

    println("The resulting schema is:")
    joined.printSchema()
    println()

    println("The first few rows are:")
    joined.show()
    println()

    val joinedCount = joined.count()
    println(s"The joined table has ${joinedCount} rows")

    // drop the extraneous column
    println("Dropping the extra zipcode column")
    println()
    val cleanedHousingPricesWithPops = joined.drop("zipcode")

    // check for null values
    // that would occur if a zipcode from the housing table
    // is not present in the populations table

    val nullRows = cleanedHousingPricesWithPops
      .filter(cleanedHousingPricesWithPops("population").isNull ||
        cleanedHousingPricesWithPops("population").isNaN)

    val nullCount = nullRows.count()

    println(s"The joined table has ${nullCount} rows with null or nan populations")
    println()

    val noNulls = cleanedHousingPricesWithPops
      .filter(cleanedHousingPricesWithPops("population").isNotNull &&
        !cleanedHousingPricesWithPops("population").isNaN)

    val nonNullCount = noNulls.count()

    println(s"${nonNullCount} rows remain after dropping rows with null populations")
    println()

    // parquet keeps column types
    // by default, Spark will refuse to overwrite
    // files.  Override this behavior so we can
    // re-run the program without manually deleting
    // the output
    noNulls.write
      .mode("overwrite")
      .json("housing_prices_with_populations")

    spark.stop()
  }
}
