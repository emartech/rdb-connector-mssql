package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import slick.jdbc.MySQLProfile.api._

trait MsSqlTestConnection {
  self: MsSqlConnector =>

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int]).map(_ => Right(()))
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }
}