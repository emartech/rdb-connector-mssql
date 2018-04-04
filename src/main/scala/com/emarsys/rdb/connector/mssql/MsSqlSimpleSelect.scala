package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import MsSqlWriters._

trait MsSqlSimpleSelect extends MsSqlStreamingQuery {
  self: MsSqlConnector =>

  override def simpleSelect(select: SimpleSelect): ConnectorResponse[Source[Seq[String], NotUsed]] = {
    streamingQuery(select.toSql)
  }
}
