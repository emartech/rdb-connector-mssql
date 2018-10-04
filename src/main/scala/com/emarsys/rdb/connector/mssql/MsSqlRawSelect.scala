package com.emarsys.rdb.connector.mssql

import java.sql.{Connection, ResultSet}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName
import slick.jdbc.SQLServerProfile.api._

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

trait MsSqlRawSelect extends MsSqlStreamingQuery {
  self: MsSqlConnector =>

  import MsSqlWriters._
  import com.emarsys.rdb.connector.common.defaults.SqlWriter._

  override def rawSelect(
      rawSql: String,
      limit: Option[Int],
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = removeEndingSemicolons(rawSql)
    val limitedQuery = limit.fold(query) { l =>
      wrapInLimit(query, l)
    }
    streamingQuery(timeout)(limitedQuery)
  }

  override def validateRawSelect(rawSql: String): ConnectorResponse[Unit] = {
    val query = createShowXmlPlanQuery(rawSql)
    db.run(query)
      .map(_ => Right())
      .recover(eitherErrorHandler())
  }

  private def createShowXmlPlanQuery(rawSql: String) = {
    SimpleDBIO[Seq[Seq[String]]](context => {
      val connection: Connection = context.connection

      setShowQueryPlan(connection, enabled = true)
      try {
        val queryPlanResultSet = connection.createStatement().executeQuery(rawSql)
        getResultWithColumnNames(queryPlanResultSet)
      } finally {
        setShowQueryPlan(connection, enabled = false)
      }
    })
  }

  private def setShowQueryPlan(connection: Connection, enabled: Boolean): Unit = {
    connection.createStatement().execute(s"SET SHOWPLAN_XML ${if (enabled) "ON" else "OFF"}")
  }

  private def getResultWithColumnNames(rs: ResultSet): Seq[Seq[String]] = {
    val columnIndexes = 1 to rs.getMetaData.getColumnCount

    var rows = Seq(columnIndexes.map(rs.getMetaData.getColumnLabel))
    while (rs.next()) rows :+= columnIndexes.map(rs.getString)

    rows
  }

  override def analyzeRawSelect(rawSql: String): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    val query = createShowXmlPlanQuery(rawSql)

    db.run(query)
      .map(result => Right(Source(result.toList)))
      .recover(eitherErrorHandler())

  }

  private def runProjectedSelectWith[R](
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      allowNullFieldValue: Boolean,
      queryRunner: String => R
  ) = {
    val fieldList    = concatenateProjection(fields)
    val projectedSql = wrapInProjectionWithLimit(rawSql, fieldList, limit)
    val query =
      if (!allowNullFieldValue) wrapInCondition(projectedSql, fields)
      else projectedSql

    queryRunner(query)
  }

  override def projectedRawSelect(
      rawSql: String,
      fields: Seq[String],
      limit: Option[Int],
      timeout: FiniteDuration,
      allowNullFieldValue: Boolean
  ): ConnectorResponse[Source[Seq[String], NotUsed]] =
    runProjectedSelectWith(rawSql, fields, limit, allowNullFieldValue, streamingQuery(timeout))

  override def validateProjectedRawSelect(rawSql: String, fields: Seq[String]): ConnectorResponse[Unit] = {
    runProjectedSelectWith(rawSql, fields, None, allowNullFieldValue = true, validateRawSelect)
  }

  private def concatenateProjection(fields: Seq[String]) =
    fields.map("t." + FieldName(_).toSql).mkString(", ")

  private def wrapInLimit(query: String, l: Int) =
    s"SELECT TOP $l * FROM ( $query ) AS query"

  private def wrapInCondition(rawSql: String, fields: Seq[String]): String =
    removeEndingSemicolons(rawSql) + concatenateCondition(fields)

  private def concatenateCondition(fields: Seq[String]) =
    " WHERE " + fields.map("t." + FieldName(_).toSql + " IS NOT NULL ").mkString("AND ")

  private def wrapInProjectionWithLimit(rawSql: String, projection: String, limit: Option[Int]) = {
    val limitSql = limit.map(l => s"TOP $l").getOrElse("")
    s"SELECT $limitSql $projection FROM ( ${removeEndingSemicolons(rawSql)} ) t"
  }

  @tailrec
  private def removeEndingSemicolons(query: String): String = {
    val qTrimmed = query.trim
    if (qTrimmed.last == ';') {
      removeEndingSemicolons(qTrimmed.dropRight(1))
    } else {
      qTrimmed
    }
  }
}
