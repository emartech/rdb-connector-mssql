package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.SimpleSelect.{TableName, Value}
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import MsSqlWriters._
import slick.jdbc.SQLServerProfile.api._

import scala.concurrent.Future

trait MsSqlIsOptimized {
  self: MsSqlConnector =>

  override def isOptimized(table: String, fields: Seq[String]): ConnectorResponse[Boolean] = {
    val fieldSet = fields.toSet

    listFields(table).flatMap {
      case Right(_) =>
        db.run(sql"""SELECT ind.name, col.name
                     FROM       sys.indexes ind
                     INNER JOIN sys.index_columns ic ON ind.object_id = ic.object_id and ind.index_id = ic.index_id
                     INNER JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id
                     INNER JOIN sys.tables t ON ind.object_id = t.object_id
                     WHERE t.name = #${Value(table).toSql}
                     ORDER BY ind.name, ic.index_column_id;""".as[(String, String)])
          .map(_.groupBy(_._1).mapValues(_.map(_._2)).values)
          .map(_.exists(indexGroup => indexGroup.toSet == fieldSet || Set(indexGroup.head) == fieldSet))
          .map(Right(_))

      case Left(tableNotFound) => Future.successful(Left(tableNotFound))
    }
  }
}
