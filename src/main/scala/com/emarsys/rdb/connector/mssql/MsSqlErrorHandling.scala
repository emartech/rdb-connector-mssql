package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError}

trait MsSqlErrorHandling {
  self: MsSqlConnector =>

  protected def errorHandler[T](): PartialFunction[Throwable, Either[ConnectorError,T]] = {
    case ex: Exception => Left(ConnectionError(ex))
  }

}
