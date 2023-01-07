import org.apache.spark.sql.SparkSession

import java.util.Properties

object SQLDatabaseExample {
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

    // database connection details
    val url = "jdbc:postgresql://127.0.0.1:5432/spam_classification"
    val tableName = "labeled_emails"
    val props = new Properties()
    props.setProperty("user", "postgres")
    props.setProperty("password", "admin")

    // read table into DataFrame
    val tableDf = spark.read
      .jdbc(url, tableName, props)

    tableDf.printSchema()

    tableDf.show()

    val count = tableDf.count();

    println(s"There are ${count} rows")

    spark.stop()

  }
}
