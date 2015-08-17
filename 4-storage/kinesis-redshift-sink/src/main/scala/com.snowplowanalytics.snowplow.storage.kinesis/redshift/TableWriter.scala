package com.snowplowanalytics.snowplow.storage.kinesis.redshift

import java.sql.{Types, Timestamp, PreparedStatement, SQLException, BatchUpdateException}
import java.util.Properties
import javax.sql.DataSource

import org.apache.commons.logging.LogFactory
import org.postgresql.ds.PGPoolingDataSource
import scala.language.implicitConversions

object SQLConverters {
  val log = LogFactory.getLog(classOf[TableWriter])
  class RedshiftOps(s: String) {
    def rsBoolean: Boolean = {
      if (s == null) throw new IllegalArgumentException("boolean string is null")
      s.toLowerCase match {
        case "1" | "true" | "yes" => true
        case _ => false
      }
    }
  }
  implicit def redshiftOps(s: String): RedshiftOps = new RedshiftOps(s)
  import java.math.{BigDecimal => BD}

  def setString(value: String, stat: PreparedStatement, index: Int) =
    stat.setString(index, value)
  def setTimestamp(value: String, stat: PreparedStatement, index: Int) =
    stat.setTimestamp(index, if (value == null) null else Timestamp.valueOf(value))
  def setBoolean(value: String, stat: PreparedStatement, index: Int) =
    if (value == null) stat.setNull(index, Types.BOOLEAN) else stat.setBoolean(index, value.rsBoolean)
  def setInteger(value: String, stat: PreparedStatement, index: Int) =
    if (value == null) stat.setNull(index, Types.INTEGER) else stat.setInt(index, value.toInt)
  def setBigint(value: String, stat: PreparedStatement, index: Int) =
    if (value == null) stat.setNull(index, Types.BIGINT) else stat.setLong(index, value.toLong)
  def setDouble(value: String, stat: PreparedStatement, index: Int) =
    if (value == null) stat.setNull(index, Types.DOUBLE) else stat.setDouble(index, value.toDouble)
  def setDecimal(value: String, stat: PreparedStatement, index: Int) =
    try {
      if (value == null) stat.setNull(index, Types.DECIMAL) else stat.setBigDecimal(index, new BD(value))
    }
    catch {
      case t:Throwable =>
        log.error(s"Invalid decimal: <$value>")
        throw t
    }

}

object TableWriter {
  def flush() = {
    writers.values.foreach(_.foreach(_.finished()))
  }

  val log = LogFactory.getLog(classOf[TableWriter])
  val writers = scala.collection.mutable.Map[String, Option[TableWriter]]()

  def tableExists(tableName: String)(implicit dataSource: DataSource) : Boolean = {
    val conn = dataSource.getConnection
    try {
      val res = conn.getMetaData.getTables(null, tableName.substring(0, tableName.indexOf('.')), tableName.substring(tableName.indexOf('.') + 1), Array("TABLE"))
      try {
        res.next()
      }
      finally {
        res.close()
      }
    }
    finally {
      conn.close()
    }
  }

  def tableForWriter(props:Properties, tableName: String): String = {
    if (tableName.contains(".")) return tableName
    props.getProperty("defaultSchema") + "." + tableName
  }

  def writerByName(schemaName: String, vendor: Option[String], version: Option[String], appId: String)
                  (implicit props: Properties, dataSource:DataSource): Option[TableWriter] = {
    val (dbSchema, dbTable) = if (schemaName.contains(".")) {
      (schemaName.substring(0, schemaName.indexOf(".")), schemaName.substring(schemaName.indexOf(".") + 1))
    } else {
      if (props.containsKey(appId + "_schema")) {
        (props.getProperty(appId + "_schema"), schemaName)
      } else {
        (props.getProperty("defaultSchema"), schemaName)
      }
    }
    val tableName = s"$dbSchema." + ((vendor, version) match {
      case (Some(_vendor), Some(_version)) =>
        s"${_vendor}_${dbTable}_${_version}"
      case (None, Some(_version)) =>
        s"${dbTable}_${_version}"
      case (None, None) =>
        s"$dbTable"
      case (Some(_vendor), None) =>
        s"${vendor}_$dbTable"
    }).replaceAllLiterally(".", "_").replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase

    synchronized {
      if (!writers.contains(tableName)) {
        if (tableExists(tableName)) {
          log.info(s"Creating writer for $tableName")
          writers += tableName -> Some(new TableWriter(dataSource, tableName))
        } else {
          log.error(s"Table does not exist for $tableName")
          writers += tableName -> None
        }
      }
      writers(tableName)
    }
  }
}

/**
 * Created by denismo on 16/07/15.
 */
class TableWriter(dataSource:DataSource, table: String) {
  val DEFAULT_BATCH_SIZE = 200
  val log = LogFactory.getLog(classOf[TableWriter])

  type SQLApplier = (String, PreparedStatement, Int) => Unit

  var names = Array[String]()
  var types = Array[Int]()
  var stat: PreparedStatement = null
  var placeholders: String = ""
  var batchCount: Int = 0
  // [1, 2, 4, 5, 8, 93, -7, 12]
  // 4 Integer
  // 8 Double
  // 1 char
  // 2 numeric
  // 5 smallint
  // 93 timestamp
  // -7 bit
  // 12 varchar
  val converters: Map[Int, SQLApplier] =
    Map(1 -> SQLConverters.setString,
      2 -> SQLConverters.setDecimal,
      4 -> SQLConverters.setInteger,
      5 -> SQLConverters.setInteger,
      8 -> SQLConverters.setDouble,
      93 -> SQLConverters.setTimestamp,
      -7 -> SQLConverters.setBoolean,
      12 -> SQLConverters.setString,
      -5 -> SQLConverters.setBigint
    )
  readMetadata()

  private def readMetadata() = {
    val conn = dataSource.getConnection
    try {
      val dbMetadata = conn.getMetaData
      val tableMetadata = dbMetadata.getColumns(null, table.substring(0, table.indexOf('.')), table.substring(table.indexOf('.') + 1), null)
      while (tableMetadata.next()) {
        names = names ++ Array(tableMetadata.getString(4))
        types = types ++ Array(tableMetadata.getInt(5))
      }
      log.info(s"Table $table has ${names.length} names " + names.zip(types).map(pair => pair._1 + ":" + pair._2).mkString(","))
      placeholders = "?" + (",?" * (names.length - 1))
      tableMetadata.close()
    }
    finally {
      conn.close()
    }
  }

  def write(values: Array[String]) = {
    if (values.length != names.length) throw new SQLException(s"Number of values does not match number of fields: ${values.length} != ${names.length}")
    if (stat == null) {
      val conn = dataSource.getConnection
      stat = conn.prepareStatement(s"insert into $table values ($placeholders)")
    }
    try {
      for (index <- values.indices) {
        converters(types(index))(values(index), stat, index+1)
      }
      stat.addBatch()
      batchCount += 1
      if (batchCount > DEFAULT_BATCH_SIZE) {
        val count = stat.executeBatch().sum
        batchCount = 0
        log.info(s"Inserted $count records into Redshift table $table")
      }
    }
    catch {
      case s:BatchUpdateException =>
        log.info(values.zipWithIndex.map(pair => pair._1 + ":" + pair._2).mkString(","))
        batchCount = 0
        stat.clearBatch()
        log.error("Exception updating batch", s)
        log.error("Nested exception", s.getNextException)
        throw s
      case e:Throwable =>
        log.info(values.zipWithIndex.map(pair => pair._1 + ":" + pair._2).mkString(","))
        batchCount = 0
        stat.clearBatch()
        log.error("Exception updating batch", e)
        throw e
    }
  }

  def finished() = {
    if (stat != null) {
      try {
        val count = stat.executeBatch().sum
        batchCount = 0
        log.info(s"Inserted $count records into Redshift table $table")
      }
      catch {
        case s:BatchUpdateException =>
          log.error("Exception updating batch", s)
          log.error("Nested exception", s.getNextException)
          throw s
        case e:Throwable =>
          log.error("Exception updating batch", e)
          throw e
      }
      finally {
        val conn = stat.getConnection
        stat.close()
        conn.close()
        stat = null
        batchCount = 0
      }
    }
  }
}
