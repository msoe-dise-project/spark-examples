import org.apache.spark.sql.SparkSession

object AssignDocumentIDsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("AssignDocumentIDsExample")
      .getOrCreate()

    // disable INFO log output from this point forward
    spark.sparkContext.setLogLevel("ERROR")

    // this is needed to allow the map function below
    // by providing an encoder
    import spark.implicits._

    val documents = spark.read
      // read each file as a string instead
      // of each line
      .option("wholetext", true)
      .textFile(
        "20news-18828/alt.atheism",
        "20news-18828/comp.graphics",
        "20news-18828/comp.os.ms-windows.misc",
        "20news-18828/comp.sys.ibm.pc.hardware",
        "20news-18828/comp.sys.mac.hardware",
        "20news-18828/comp.windows.x",
        "20news-18828/misc.forsale",
        "20news-18828/rec.autos",
        "20news-18828/rec.motorcycles",
        "20news-18828/rec.sport.baseball",
        "20news-18828/rec.sport.hockey",
        "20news-18828/sci.crypt",
        "20news-18828/sci.electronics",
        "20news-18828/sci.med",
        "20news-18828/sci.space",
        "20news-18828/soc.religion.christian",
        "20news-18828/talk.politics.guns",
        "20news-18828/talk.politics.mideast",
        "20news-18828/talk.politics.misc",
        "20news-18828/talk.religion.misc"
      )

    val indexedDocuments = documents
      // no zip function on Datasets so use
      // underlying RDD
      .rdd
      // assign a unique index to each document
      // go from String to (String, Long)
      .zipWithIndex()

    indexedDocuments
      // convert to DataFrame so we can write it as a tabular data set
      .toDF("documentText", "documentIndex")
      .write
      // By default, Spark will fail if the output exists
      .mode("overwrite")
      // Create JSON files with one record per line
      .json("documents_with_indices")

    spark.stop()
  }
}
