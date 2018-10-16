package com.emarsys.rdb.connector.mssql

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors._
import com.microsoft.sqlserver.jdbc.SQLServerException

trait MsSqlErrorHandling {

  val MSSQL_STATE_QUERY_CANCELLED     = "HY008"
  val MSSQL_STATE_SYNTAX_ERROR        = "S0001"
  val MSSQL_STATE_PERMISSION_DENIED   = "S0005"
  val MSSQL_STATE_INVALID_OBJECT_NAME = "S0002"

  val MMSQL_BAD_HOST_ERROR = "08S01"

  val connectionErrors = Seq(
    MMSQL_BAD_HOST_ERROR
  )

  protected def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_QUERY_CANCELLED     => QueryTimeout(ex.getMessage)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_SYNTAX_ERROR        => SqlSyntaxError(ex.getMessage)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_PERMISSION_DENIED   => AccessDeniedError(ex.getMessage)
    case ex: SQLServerException if ex.getSQLState == MSSQL_STATE_INVALID_OBJECT_NAME => TableNotFound(ex.getMessage)
    case ex: SQLException if connectionErrors.contains(ex.getSQLState)               => ConnectionError(ex)
    case ex: SQLException                                                            => ErrorWithMessage(s"[${ex.getSQLState}] - ${ex.getMessage}")
    case ex: Exception                                                               => ErrorWithMessage(ex.getMessage)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
