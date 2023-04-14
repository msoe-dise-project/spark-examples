import org.apache.spark.sql.SparkSession

// The object keyword is used to define a singleton
// (class that is only ever instantiated once in a program)
object DatasetExample2 {
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
      .appName("DatasetExample")
      .getOrCreate()

    // disable INFO log output from this point forward
    spark.sparkContext.setLogLevel("ERROR")

    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    // Read file and return Dataset[String].
    // The Dataset is a distributed collection with partitions
    // on each worker.  We can manipulate the collection locally
    // in the driver, but the actual computation is performed by the
    // workers.
    val textFile = spark.read.textFile("/usr/share/dict/words")

    // Counts the number of items in each partition
    // returns the sum to the driver
    val numberOfLines = textFile.count()

    println()
    println(s"The file has ${numberOfLines} lines")
    println()

    // Grabs the first five items from the first partition
    // and return them as a local Array[String]
    // Note that the Dataset keeps the Strings in the order
    // they were read so these are first five words in the file
    val head = textFile.head(5)
    println()
    println("The first five words are:")
    for(w <- head) {
      println(s"\t${w}")
    }
    println()

    // Grabs the first five items from the first partition
    // and return them as a local Array[String]
    val tail = textFile.tail(5)
    println()
    println("The last five words are:")
    for (w <- tail) {
      println(s"\t${w}")
    }
    println()

    // Map applies the given anonymous function (lambda) to every object
    // in the Dataset and returns a new Dataset.  Datasets are never modified
    // -- transformations generate new Datasets.
    // Scala doesn't require using () when calling a method with no parameters
    val firstLetters = textFile.map(w => w.toLowerCase)

    // groupByKey turns each object into a pair of (key, value)
    // The value is the previous object, while the key is the result
    // of the anonymous function run on the object.
    // In this case, the pairs will be (firstLetter, word)
    // It then combines all of the pairs by their key.
    // E.g., ("a", Iterator[word])
    // note that this returns a KeyValueGroupedDataset which
    // subclasses Dataset with extra functionality
    val keyValuePairs = firstLetters.groupByKey(w => w.substring(0, 1))

    // Apply the anonymous function (lambda) to each group
    // The function takes a key and an iterator of values
    // We count the number of words and return a pair of (letter, counts)
    val histogram = keyValuePairs.mapGroups((letter, words) => (letter, words.length))

    // returns a local Array[(String, Int)] of the data
    // to the driver
    // generally don't do this unless you are sure
    // that the Dataset is relatively small
    // In our case, our Dataset should only have
    // 26 pairs -- 1 for each letter
    val local = histogram.collect()

    println()
    println(s"Histogram has ${local.length} entries")
    println()

    // Get the first element (the letter) in each pair
    // and use it to sort the histogram
    val sorted = local.sortBy(pair => pair._1)
    for((letter, count) <- sorted) {
      println(s"${count} words start with the letter ${letter}")
    }
    println()

    spark.stop()
  }
}
