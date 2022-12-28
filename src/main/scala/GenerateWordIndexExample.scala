import org.apache.spark.sql.SparkSession

object GenerateWordIndexExample {
  def extractWords(document: String): Seq[String] = {
    document.toLowerCase
      // remove anything that isn't a letter or whitespace
      .map(c => if (c.isLetter || c.isWhitespace) c else ' ')
      // split words on any whitespace
      .split("\\s+")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("GenerateWordIndexExample")
      .getOrCreate()

    // disable INFO log output from this point forward
    spark.sparkContext.setLogLevel("ERROR")

    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    val indexedDocuments = spark
      .read
      .json("documents_with_indices")

      // root
      // |-- documentText: string (nullable = true)
      // |-- documentIndex: long (nullable = false)val documents = spark.read

    val nDocuments = indexedDocuments.count()

    println(s"Read ${nDocuments} documents")

    val wordDocumentCounts = indexedDocuments
      .map(row => extractWords(row.getAs[String]("documentText")))
      // reduces the number of words to 1 copy per document
      .flatMap(seq => seq.toSeq)
      // generates DataSet[(word, (instance1, instance2, etc.)]
      .groupByKey(word => word)
      // replace sequence of word instances with document counts
      // DataSet[(word, count)]
      .count()

    val nUniqueWords = wordDocumentCounts.count()

    println(s"Found ${nUniqueWords} unique words")

    val filteredByDocCount = wordDocumentCounts
      .filter(pair => pair._2 >= 10)

    val nWordsAfter = filteredByDocCount.count()

    println(s"${nWordsAfter} words remain after filtering")

    val wordIndex = filteredByDocCount
      .map(pair => pair._1)
      .rdd
      .zipWithIndex()
      .toDF("word", "index")

    wordIndex.printSchema()

    wordIndex.show(5)

    wordIndex.write
      .mode("overwrite")
      .parquet("word_index")

    spark.stop()
  }
}
