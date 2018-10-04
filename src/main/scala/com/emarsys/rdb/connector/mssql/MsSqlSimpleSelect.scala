package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import MsSqlWriters._

import scala.concurrent.duration.FiniteDuration

trait MsSqlSimpleSelect extends MsSqlStreamingQuery {
  self: MsSqlConnector =>

  override def simpleSelect(
      select: SimpleSelect,
      timeout: FiniteDuration
  ): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(timeout)(select.toSql)
  }
}
