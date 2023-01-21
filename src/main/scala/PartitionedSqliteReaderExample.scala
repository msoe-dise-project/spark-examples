import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.nio.file.{Files, Paths}
import java.sql.{DriverManager, ResultSet, ResultSetMetaData, Types}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

// This examples read a table of data partitioned
// across multiple Sqlite database files.
// The table schema is inferred from the one
// of the files and used to create a DataFrame
// with the appropriate types
object PartitionedSqliteReaderExample {
  def convertSchema(rs: ResultSet): StructType = {
    val metaData = rs.getMetaData
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

    StructType(columnDescriptions.toArray)
  }

  def resultSetToRows(rs: ResultSet): Seq[Row] = {
    val metaData = rs.getMetaData()

    val nColumns = metaData.getColumnCount

    val rows: ListBuffer[Row] = ListBuffer()
    while (rs.next()) {
      val values = (1 to nColumns).map(idx => rs.getObject(idx))
      val row = Row.fromSeq(values)
      rows += row
    }

    rows.toSeq
  }

  def readTable(filename: String, tableName: String): ResultSet = {
    val url = s"jdbc:sqlite:${filename}"
    val sql = s"SELECT * FROM ${tableName}"
    val conn = DriverManager.getConnection(url)
    val stmt = conn.createStatement()

    stmt.executeQuery(sql)
  }

  def checkSchemaConsistency(schemas: Array[DatabaseDescription]): Option[(DatabaseDescription, DatabaseDescription)] = {
    if (schemas.isEmpty) {
      return Option.empty
    }
    var previous = schemas(0)
    for(idx <- 1 until schemas.length) {
      val next = schemas(idx)
      if(previous.schema != next.schema) {
        return Option(previous, next)
      }
      previous = next
    }

    Option.empty
  }

  case class DatabaseDescription(filename: String, schema: StructType)

  def readSqlite(filenames: Seq[String], tableName: String)(implicit spark: SparkSession): DataFrame = {
    val tableData = spark.sparkContext
      .makeRDD(filenames)
      .map { path =>
        val rs = readTable(path, tableName)
        val schema = convertSchema(rs)
        val rows = resultSetToRows(rs)

        (path, schema, rows)
      }

    val schemas = tableData.map { tuple =>
      val (path, schema, _) = tuple

      DatabaseDescription(path, schema)
    }.collect()

    val schemasConsistent = checkSchemaConsistency(schemas)

    if(schemasConsistent.isDefined) {
      val (dbDesc1, dbDesc2) = schemasConsistent.get
      println("Found inconsistent schemas.")
      println(s"Database in s${dbDesc1.filename} has schema ${dbDesc1.schema}")
      println(s"Database in s${dbDesc2.filename} has schema ${dbDesc2.schema}")
      println("Merging inconsistent schemas is not currently supported.")

      throw new IllegalStateException("Inconsistent schemas")
    }

    val rows = tableData.map { tuple =>
      val (_, _, rows) = tuple
      rows
    }.flatMap(identity)

    val schema = schemas(0).schema

    spark.createDataFrame(rows, schema)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      // set number of cores to use in []
      .master("local[4]")
      .appName("SqliteReaderExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dbPaths = Files.list(Paths.get("zipcode_sqlite"))
      .iterator()
      .asScala
      .map(filename => filename.toString)
      .filter(filename => filename.endsWith("db"))
      .toSeq

    for(filename <- dbPaths) {
      println(filename)
    }

    val tableName = "zipcode_populations"

    val df = readSqlite(dbPaths, tableName)(spark)

    df.printSchema()
    val count = df.count()
    println(s"Found ${count} records")
    df.show(10)

    spark.stop()
  }
}
