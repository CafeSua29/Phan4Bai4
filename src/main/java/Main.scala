import config.ConfigPropertiesLoader
import hbase.HBaseConnectionFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.{HBaseConfiguration, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, RegexStringComparator, SingleColumnValueFilter}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog.table

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

object Main {
  val spark = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

//  private val personInfoLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("personInfoLogPath")
//  private val personIdListLogPath = ConfigPropertiesLoader.getYamlConfig.getProperty("personIdListLogPath")
//  private val ageAnalysisPath = ConfigPropertiesLoader.getYamlConfig.getProperty("ageAnalysisPath")

  import spark.implicits._

  private def createReplaceTable(connection: Connection, inpNamespace: String, inpTableName: String): Unit = {
    val admin = connection.getAdmin
    val tableName = TableName.valueOf(inpTableName)

    try {
      admin.getNamespaceDescriptor(inpNamespace)
      println(s"Namespace $inpNamespace already exists.")
    } catch {
      case _: Exception =>
        println(s"Namespace $inpNamespace doesn't exist. Creating namespace...")
        val namespaceDescriptor = NamespaceDescriptor.create(inpNamespace).build()
        admin.createNamespace(namespaceDescriptor)
        println(s"Namespace $inpNamespace created.")
    }

    if (admin.tableExists(tableName)) {
      println(s"Table $tableName exists. Deleting...")
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    if (!admin.tableExists(tableName)) {
      val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)

      val producerFamily = ColumnFamilyDescriptorBuilder.newBuilder("producer".getBytes).build()
      val consumerFamily = ColumnFamilyDescriptorBuilder.newBuilder("consumer".getBytes).build()
      val hardwareFamily = ColumnFamilyDescriptorBuilder.newBuilder("hardware".getBytes).build()

      tableDescriptorBuilder.setColumnFamily(producerFamily)
      tableDescriptorBuilder.setColumnFamily(consumerFamily)
      tableDescriptorBuilder.setColumnFamily(hardwareFamily)

      admin.createTable(tableDescriptorBuilder.build())
      println("Bảng đã được tạo thành công!")
    } else {
      println("Bảng đã tồn tại.")
    }
    admin.close()
  }

  private def createDataFrameAndPutToHDFS(): Unit = {
    val schema = StructType(Array(
      StructField("timeCreate", TimestampType, true),
      StructField("cookieCreate", TimestampType, true),
      StructField("browserCode", IntegerType, true),
      StructField("browserVer", StringType, true),
      StructField("osCode", IntegerType, true),
      StructField("osVer", StringType, true),
      StructField("ip", LongType, true),
      StructField("locId", IntegerType, true),
      StructField("domain", StringType, true),
      StructField("siteId", IntegerType, true),
      StructField("cId", IntegerType, true),
      StructField("path", StringType, true),
      StructField("referer", StringType, true),
      StructField("guid", LongType, true),
      StructField("flashVersion", StringType, true),
      StructField("jre", StringType, true),
      StructField("sr", StringType, true),
      StructField("sc", StringType, true),
      StructField("geographic", IntegerType, true),
      StructField("field19", StringType, true),
      StructField("field20", StringType, true),
      StructField("url", StringType, true),
      StructField("field22", StringType, true),
      StructField("category", StringType, true),
      StructField("field24", StringType, true)
    ))

    val df = spark.read
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(schema)
      .csv("sample text")

    df.show()

    df.write
      .mode("overwrite")
      .parquet("hdfs://namenode:9000/Phan4/Bai4")
  }

//  private def readHDFSThenPutToHBase(): Unit = {
//    var df = spark.read.parquet("hdfs://namenode:9000/Phan4/Bai4")
//    df.printSchema()
//
//    df = df.withColumn("day", date_format(col("timeCreate"), "yyyy-MM-dd"))
//
//    def catalog =
//      s"""{
//         |"table":{"namespace":"pageviewlog", "name":"pageviewlog_info"},
//         |"rowkey":"guid",
//         |"columns":{
//         |"guid":{"cf":"rowkey", "col":"guid", "type":"long"},
//         |"timeCreate":{"cf":"consumer", "col":"timeCreate", "type":"string"},
//         |"cookieCreate":{"cf":"consumer", "col":"cookieCreate", "type":"string"},
//         |"browserCode":{"cf":"hardware", "col":"browserCode", "type":"int"},
//         |"browserVer":{"cf":"hardware", "col":"browserVer", "type":"string"},
//         |"osCode":{"cf":"hardware", "col":"osCode", "type":"int"},
//         |"osVer":{"cf":"hardware", "col":"osVer", "type":"string"},
//         |"ip":{"cf":"consumer", "col":"ip", "type":"long"},
//         |"locId":{"cf":"consumer", "col":"locId", "type":"int"},
//         |"domain":{"cf":"producer", "col":"domain", "type":"string"},
//         |"siteId":{"cf":"producer", "col":"siteId", "type":"int"},
//         |"cId":{"cf":"consumer", "col":"cId", "type":"int"},
//         |"path":{"cf":"producer", "col":"path", "type":"string"},
//         |"referer":{"cf":"producer", "col":"referer", "type":"string"},
//         |"flashVersion":{"cf":"hardware", "col":"flashVersion", "type":"string"},
//         |"jre":{"cf":"hardware", "col":"jre", "type":"string"},
//         |"sr":{"cf":"hardware", "col":"sr", "type":"string"},
//         |"sc":{"cf":"hardware", "col":"sc", "type":"string"},
//         |"geographic":{"cf":"consumer", "col":"geographic", "type":"int"},
//         |"url":{"cf":"producer", "col":"url", "type":"string"},
//         |"category":{"cf":"producer", "col":"category", "type":"string"},
//         |"day":{"cf":"consumer", "col":"day", "type":"string"}
//         |}
//         |}""".stripMargin
//
//    df.write
//      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
//      .format("org.apache.hadoop.hbase.spark")
//      .save()
//  }

  private def readHDFSThenPutToHBase(): Unit = {
    var df = spark.read.parquet("hdfs://namenode:9000/Phan4/Bai4")
    df.printSchema()

    df = df
      .withColumn("day", date_format(col("timeCreate"), "yyyy-MM-dd"))
      .repartition(5)

    val batchPutSize = 100

    df.foreachPartition((rows: Iterator[Row]) => {
      val hbaseConnection = HBaseConnectionFactory.createConnection()
      //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      try {
        val table = hbaseConnection.getTable(TableName.valueOf("pageviewlog"))
        val puts = new util.ArrayList[Put]()
        for (row <- rows) {
          val timeCreate = row.getAs[Timestamp]("timeCreate").toString
          val cookieCreate = row.getAs[Timestamp]("cookieCreate").toString
          val browserCode = row.getAs[Int]("browserCode")
          val browserVer = row.getAs[String]("browserVer")
          val osCode = row.getAs[Int]("osCode")
          var osVer = row.getAs[String]("osVer")

          if (osVer == null)
            osVer = "-1"

          val ip = row.getAs[Long]("ip")
          val locId = row.getAs[Int]("locId")
          val domain = row.getAs[String]("domain")
          val siteId = row.getAs[Int]("siteId")
          val cId = row.getAs[Int]("cId")
          val path = row.getAs[String]("path")
          val referer = row.getAs[String]("referer")
          val guid = row.getAs[Long]("guid")
          val flashVersion = row.getAs[String]("flashVersion")
          val jre = row.getAs[String]("jre")
          val sr = row.getAs[String]("sr")
          val sc = row.getAs[String]("sc")
          val geographic = row.getAs[Int]("geographic")
          val url = row.getAs[String]("url")
          val category = row.getAs[String]("category")
          val day = row.getAs[String]("day")

          val put = new Put(Bytes.toBytes(guid))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("timeCreate"), Bytes.toBytes(timeCreate))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("cookieCreate"), Bytes.toBytes(cookieCreate))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("browserCode"), Bytes.toBytes(browserCode))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("browserVer"), Bytes.toBytes(browserVer))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("osCode"), Bytes.toBytes(osCode))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("osVer"), Bytes.toBytes(osVer))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("ip"), Bytes.toBytes(ip))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("locId"), Bytes.toBytes(locId))
          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("domain"), Bytes.toBytes(domain))
          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("siteId"), Bytes.toBytes(siteId))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("cId"), Bytes.toBytes(cId))
          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("path"), Bytes.toBytes(path))
          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("referer"), Bytes.toBytes(referer))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("guid"), Bytes.toBytes(guid))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("flashVersion"), Bytes.toBytes(flashVersion))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("jre"), Bytes.toBytes(jre))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("sr"), Bytes.toBytes(sr))
          put.addColumn(Bytes.toBytes("hardware"), Bytes.toBytes("sc"), Bytes.toBytes(sc))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("geographic"), Bytes.toBytes(geographic))
          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("url"), Bytes.toBytes(url))
          put.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("category"), Bytes.toBytes(category))
          put.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("day"), Bytes.toBytes(day))

          puts.add(put)
          if (puts.size > batchPutSize) {
            table.put(puts)
            puts.clear()
          }
        }
        if (puts.size() > 0) {
          table.put(puts)
        }
      } finally {
        hbaseConnection.close()
        println("done")
      }
    })
  }

  def listUrlsByGuidAndDate(table: Table, guid: Long, date: String): Seq[String] = {
    // Create a scan object with the necessary filters
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("guid"))
    scan.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("domain"))
    scan.addColumn(Bytes.toBytes("producer"), Bytes.toBytes("path"))

    // Add a filter to match the GUID
    val guidFilter = new org.apache.hadoop.hbase.filter.SingleColumnValueFilter(
      Bytes.toBytes("consumer"),
      Bytes.toBytes("guid"),
      org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL,
      Bytes.toBytes(guid)
    )

    scan.setFilter(guidFilter)

    // Scan the table and read URLs
    val urls = scala.collection.mutable.ListBuffer[String]()
    val scanner = table.getScanner(scan)

    var result = scanner.next()
    while (result != null) {
      val domain = Bytes.toString(result.getValue(Bytes.toBytes("producer"), Bytes.toBytes("domain")))
      val path = Bytes.toString(result.getValue(Bytes.toBytes("producer"), Bytes.toBytes("path")))
      urls += s"$domain/$path"
      result = scanner.next()
    }

    scanner.close()
    urls.toList
  }

  def mostUsedIpsByGuid(table: Table, guid: Long): Seq[(String, Int)] = {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("ip"))
    scan.addColumn(Bytes.toBytes("consumer"), Bytes.toBytes("guid"))

    // Add a filter to match the GUID
    val guidFilter = new SingleColumnValueFilter(
      Bytes.toBytes("consumer"),
      Bytes.toBytes("guid"),
      CompareFilter.CompareOp.EQUAL,
      Bytes.toBytes(guid)
    )

    scan.setFilter(guidFilter)

    // Scan the table and collect IPs
    val ipCounts = scala.collection.mutable.Map[String, Int]()
    val scanner = table.getScanner(scan)

    var result = scanner.next()
    while (result != null) {
      val ip = Bytes.toString(result.getValue(Bytes.toBytes("consumer"), Bytes.toBytes("ip")))
      ipCounts(ip) = ipCounts.getOrElse(ip, 0) + 1
      result = scanner.next()
    }

    scanner.close()

    // Sort IPs by occurrence count in descending order
    ipCounts.toSeq.sortBy(-_._2)
  }

  def main(args: Array[String]): Unit = {
    val hbaseConnection = HBaseConnectionFactory.createConnection()
    val table = hbaseConnection.getTable(TableName.valueOf("pageviewlog"))
    val guid: Long = 8813685310712123492L
    val date: String = "2018-08-10"

    //createReplaceTable(connection, inpNamespace, inpTableName)
    createDataFrameAndPutToHDFS()
    readHDFSThenPutToHBase()

    // 4.1 List URLs by GUID and Date
    val urls = listUrlsByGuidAndDate(table, guid, date)
    println(s"URLs accessed by GUID $guid on $date: $urls")

    // 4.2: Most used IPs by GUID
    val ipCounts = mostUsedIpsByGuid(table, guid)
    println(s"Most used IPs by GUID $guid: $ipCounts")

    //readHBaseThenWriteToHDFS()
  }
}
