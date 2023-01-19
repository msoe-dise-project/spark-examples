import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, ResultSetMetaData, Types}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

// This examples read a table of data partitioned
// across multiple Sqlite database files.
// The table schema is inferred from the one
// of the files and used to create a DataFrame
// with the appropriate types
object PartitionedSqliteReaderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[4]")
      .appName("SqliteReaderExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val filenames = Files.list(Paths.get("zipcode_sqlite"))
      .iterator()
      .asScala
      .map(filename => filename.toString)
      .filter(filename => filename.endsWith("db"))
      .toSeq

    for(filename <- filenames) {
      println(filename)
    }

    val tableName = "zipcode_populations"

    val firstFile = filenames(0)

    val url = s"jdbc:sqlite:${firstFile}"
    val sql = s"SELECT * FROM ${tableName}"
    val conn = DriverManager.getConnection(url)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(sql)
    val metaData = rs.getMetaData()

    val nColumns = metaData.getColumnCount
    
    val columnDescriptions = (1 to nColumns).map { idx =>
      val columnName = metaData.getColumnName(idx)
      val isNullable = metaData.isNullable(idx) == ResultSetMetaData.columnNullable
      val sparkType: DataType = metaData.getColumnType(idx) match {
        case Types.BOOLEAN => BooleanType
        case Types.DATE => DateType
        case Types.INTEGER => IntegerType
        case Types.FLOAT => DoubleType
        case Types.DOUBLE => DoubleType
        case Types.REAL => DoubleType
        case Types.TIME => TimestampType
        case _ => StringType
      }
      StructField(columnName, sparkType, isNullable)
    }

    val schemaStruct = StructType(columnDescriptions.toArray)

    val rows = spark.sparkContext
      .makeRDD(filenames)
      .map { filename =>
        val url = s"jdbc:sqlite:${filename}"
        val sql = s"SELECT * FROM ${tableName}"
        val conn = DriverManager.getConnection(url)
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery(sql)
        val metaData = rs.getMetaData()

        val seq: ListBuffer[Row] = ListBuffer()
        while(rs.next()) {
          val values = (1 to nColumns).map(idx => rs.getObject(idx))
          val row = Row.fromSeq(values)
          seq += row
        }

        seq.toSeq
      }
      .flatMap(seq => seq)

    val df = spark.createDataFrame(rows, schemaStruct)

    df.printSchema()

    println(df.count())

    df.show(10)

    spark.stop()
  }
}
