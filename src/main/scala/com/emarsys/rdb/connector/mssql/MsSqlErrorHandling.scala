package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors._
import com.microsoft.sqlserver.jdbc.SQLServerException

trait MsSqlErrorHandling {
  self: MsSqlConnector =>

  val MSSQL_STATE_QUERY_CANCELLED     = "HY008"
  val MSSQL_STATE_SYNTAX_ERROR        = "S0001"
  val MSSQL_STATE_PERMISSION_DENIED   = "S0005"
  val MSSQL_STATE_INVALID_OBJECT_NAME = "S0002"

  private def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_QUERY_CANCELLED     => QueryTimeout(ex.getMessage)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_SYNTAX_ERROR        => SqlSyntaxError(ex.getMessage)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_PERMISSION_DENIED   => AccessDeniedError(ex.getMessage)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_INVALID_OBJECT_NAME => TableNotFound(ex.getMessage)
    case ex: SQLServerException                                                      => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
