import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

object VectorizeTextExample {
  def extractWords(document: String): Seq[String] = {
    document.toLowerCase
      // remove anything that isn't a letter or whitespace
      .map(c => if (c.isLetter || c.isWhitespace) c else ' ')
      // split words on any whitespace
      .split("\\s+")
  }

  def countWords(docIdx: Long, documentWords: Seq[String]): Seq[(String, Long, Long)] = {
    // Scala has two sets of collections (immutable and mutable)
    // This creates a mutable HashMap
    val map = mutable.HashMap.empty[String, Long]

    for(w <- documentWords) {
      val previousCount = map.getOrElse(w, 0L)
      val updatedCount = previousCount + 1
      map.put(w, updatedCount)
    }

    // convert to sequence of (word, (docIdx, docCount))
    map
      .toList
      .map(pair => (pair._1, docIdx, pair._2))
  }

  def createFeatureVector(docIdx: Long, rows: Iterator[Row]): (Long, Array[Long], Array[Double]) = {
    // wordIdx to count
    val map = mutable.HashMap.empty[Long, Long]

    for(r <- rows) {
      val wordIdx = r.getAs[Long]("wordIdx")
      val previousCount = map.getOrElse(wordIdx, 0L)
      val updatedCount = previousCount + r.getAs[Long]("docCount")
      map.put(wordIdx, updatedCount)
    }

    val sorted = map.toList.sortBy(pair => pair._1)
    val indices = sorted.map(_._1).toArray
    val values = sorted.map(_._2.toDouble).toArray

    (docIdx, indices, values)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("VectorizeTextExample")
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
    // |-- documentIndex: long (nullable = false)

    val nDocuments = indexedDocuments.count()

    println(s"Read ${nDocuments} documents")
    // countWords(row.getAs("documentIndex"),
    val wordDocumentCounts = indexedDocuments
      // convert document into sequence of words
      // pair(documentIndex, seq[String])
      .map(row => (row.getAs[Long]("documentIndex"), extractWords(row.getAs("documentText"))))
      // reduce single word instances into counts
      // Seq[(word: String, docIdx: Long, docCount: Long)]
      .map(pair => countWords(pair._1, pair._2))
      // flatten DS into triplets of (word: String, docIdx: Long, docCount: Long)
      .flatMap(seq => seq)
      .toDF("word", "docIdx", "docCount")

    // wordDocumentCounts.printSchema()

    // root
    // |-- word: string (nullable = true)
    // |-- docIdx: long (nullable = false)
    // |-- docCount: long (nullable = false)

    val wordIndex = spark
      .read
      .parquet("word_index")

    // root
    // |-- word: string (nullable = true)
    // |-- index: long (nullable = true)

    // perform a join so that can we grab the index for each word
    // word column will be duplicated
    val wordsWithIndices = wordDocumentCounts
      .join(wordIndex, wordDocumentCounts("word") === wordIndex("word"), "inner")
      .withColumnRenamed("index", "wordIdx")

    // wordsWithIndices.printSchema()

    // root
    // |-- word: string (nullable = true)
    // |-- docIdx: long (nullable = false)
    // |-- docCount: long (nullable = false)
    // |-- word: string (nullable = true)
    // |-- wordIdx: long (nullable = true)

    val featureVectors = wordsWithIndices
      // group rows by documentId
      .groupByKey(row => row.getAs[Long]("docIdx"))
      // combine all rows for a single document into a single object
      .mapGroups((docIdx, rows) => createFeatureVector(docIdx, rows))
      .toDF("documentIdx", "featureIndices", "featureValues")

    // featureVectors.printSchema()

    // root
    // |-- documentIdx: long (nullable = false)
    // |-- featureIndices: array (nullable = true)
    // | |-- element: long (containsNull = false)
    // |-- featureValues: array (nullable = true)
    // | |-- element: double (containsNull = false)

    featureVectors.show(5)

    featureVectors
      .write
      .mode("overwrite")
      .json("feature_vectors")

    spark.stop()
  }
}
