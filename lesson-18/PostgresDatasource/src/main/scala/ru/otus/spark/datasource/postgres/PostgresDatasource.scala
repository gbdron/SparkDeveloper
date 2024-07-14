package ru.otus.spark.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import scala.collection.JavaConverters._

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName"))
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

case class ConnectionProperties(
    url: String,
    user: String,
    password: String,
    tableName: String,
    partitionSize: Int
)

/** Read */
class PostgresScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PostgresScan(
    ConnectionProperties(
      options.get("url"),
      options.get("user"),
      options.get("password"),
      options.get("tableName"),
      options.getInt("partitionSize", 1)
    )
  )
}

class PostgresPartition(val lowerBound: Long, val upperBound: Long) extends InputPartition

class PostgresScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val connection = DriverManager.getConnection(
      connectionProperties.url,
      connectionProperties.user,
      connectionProperties.password
    )
    val statement = connection.createStatement()
    
    val countResultSet = statement.executeQuery(s"SELECT COUNT(*) FROM ${connectionProperties.tableName}")
    countResultSet.next()
    val totalRows = countResultSet.getLong(1)
    connection.close()

    val partitionSize = connectionProperties.partitionSize
    val numPartitions = Math.ceil(totalRows.toDouble / partitionSize).toInt
    (0 until numPartitions).map { i =>
      val lowerBound = i * partitionSize + 1
      val upperBound = if (i == numPartitions - 1) totalRows else (i + 1) * partitionSize
      new PostgresPartition(lowerBound, upperBound)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = new PostgresPartitionReaderFactory(connectionProperties)
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val postgresPartition = partition.asInstanceOf[PostgresPartition]
    new PostgresPartitionReader(connectionProperties, postgresPartition.lowerBound, postgresPartition.upperBound)
  }
}

class PostgresPartitionReader(connectionProperties: ConnectionProperties, lowerBound: Long, upperBound: Long)
    extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  
  private val statement = connection.prepareStatement(
    s"""
       |SELECT * FROM (
       |  SELECT *, ROW_NUMBER() OVER () AS row_num
       |  FROM ${connectionProperties.tableName}
       |) AS subquery
       |WHERE row_num >= ? AND row_num <= ?
     """.stripMargin
  )
  statement.setLong(1, lowerBound)
  statement.setLong(2, upperBound)
  
  private val resultSet = statement.executeQuery()

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong("user_id"))

  override def close(): Unit = connection.close()
}

/** Write */
class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(
    ConnectionProperties(
      options.get("url"),
      options.get("user"),
      options.get("password"),
      options.get("tableName"),
      options.getInt("partitionSize", 1)
    )
  )
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection: Connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement: PreparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}
