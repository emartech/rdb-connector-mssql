package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError}

trait MsSqlErrorHandling {
  self: MsSqlConnector =>

  private def errorHandler(): PartialFunction[Throwable, ConnectorError] = {
    case ex: Exception => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed

}
