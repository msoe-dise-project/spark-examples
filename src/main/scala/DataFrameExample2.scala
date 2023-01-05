import org.apache.spark.sql.SparkSession

// This example loads a CSV file of real estate
// transactions as a DataFrame and uses the
// DataFrame API to remove outlier records
// The output is written in parquet format
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

    val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    var df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    df.show()

    df.printSchema()

    // convert types
    df = df.withColumn("salary", df("salary").cast("double"))
    df = df.withColumn("bonus", df("bonus").cast("double"))

    df.printSchema()

    // computation on a column
    val bonusPercent = df("bonus") / df("salary") * 100
    df = df.withColumn("bonus_percent_salary", bonusPercent)

    df.show()

    df.filter(df("salary") >= 90000).show()

    println()

    df.filter(!(df("salary") >= 90000)).show()

    println()

    df.filter(df("department") === "Finance").show()

    spark.stop()
  }
}
