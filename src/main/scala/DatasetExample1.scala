import org.apache.spark.sql.SparkSession

object DatasetExample1 {
  def extractWords(document: String): Seq[String] = {
    document.toLowerCase
      // remove anything that isn't a letter or whitespace
      .map(c => if (c.isLetter || c.isWhitespace) c else ' ')
      // split words on any whitespace
      .split("\\s+")
  }

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

    // get the number of documents
    // action
    val nDocuments = documents.count()

    println(s"Read ${nDocuments} documents")
    println()

    val smallDocuments = documents
      // transformation
      .filter(doc => doc.length < 100)
      // action
      .head(5)

    println("The first five documents look like:")
    for(doc <- smallDocuments) {
      println(s"${doc}")
      println("----------------------------------------")
    }
    println()

    // Dataset[String]
    // transformations -- no actions
    val documentLengths = documents
      // function takes a String and returns a
      // Seq[String].  Map copies the function
      // to each worker and runs it on the strings
      // in the partitions on each worker.
      // Produces a new Dataset[Seq[String]]
      .map(extractWords)
      // Apply an anonymous function (lambda) to each
      // sequence of strings to get the lengths
      // returns new Dataset[Int]
      .map(words => words.length)

    // transformation
    val sorted = documentLengths.sort($"value")

    // action
    // number of words in the five shorted documents
    println("The five shortest documents have these many words:")
    for(l <- sorted.head(5)) {
      println(s"\t${l}")
    }
    println()

    // action
    // number of words in the five longest documents
    println("The five longest documents have these many words:")
    for (l <- sorted.tail(5)) {
      println(s"\t${l}")
    }
    println()

    val smallestDocuments = documentLengths
      // transformation
      .filter(l => l <= 20)
      // action
      .count()

    println(s"${smallestDocuments} documents have 20 or fewer words")

    val largestDocuments = documentLengths
      // transformation
      .filter(l => l > 10000)
      // action
      .count()

    println(s"${largestDocuments} documents have 10000 or more words")

    spark.stop()
  }
}
