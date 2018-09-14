package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.Errors.TableNotFound
import com.emarsys.rdb.connector.common.models.SimpleSelect.Value
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import com.emarsys.rdb.connector.mssql.MsSqlWriters._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

trait MsSqlMetadata {
  self: MsSqlConnector =>


  override def listTables(): ConnectorResponse[Seq[TableModel]] = {

    db.run(sql"SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES".as[(String, String)])
      .map(_.map(parseToTableModel))
      .map(Right(_))
      .recover(eitherErrorHandler())
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    val tableNameAsValue = Value(tableName).toSql
    db.run(
      sql"""SELECT c.name as col_name, t.Name as col_type
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE c.object_id = OBJECT_ID(#$tableNameAsValue) OR OBJECT_NAME(c.object_id) = #$tableNameAsValue""".as[(String, String)])
      .map(_.map(parseToFieldModel))
      .map(result =>
        if (result.isEmpty) Left(TableNotFound(tableName))
        else                Right(result)
      )
      .recover(eitherErrorHandler())
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val futureMap = listAllFields()
    (for {
      tablesE <- listTables()
      map <- futureMap
    } yield tablesE.map(makeTablesWithFields(_, map)))
      .recover(eitherErrorHandler())
  }

  private def listAllFields(): Future[Map[String, Seq[FieldModel]]] = {
    db.run(sql"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS;".as[(String, String, String)])
      .map(_.groupBy(_._1).mapValues(_.map(x => parseToFieldModel(x._2 -> x._3)).toSeq))
  }

  private def makeTablesWithFields(tableList: Seq[TableModel], tableFieldMap: Map[String, Seq[FieldModel]]): Seq[FullTableModel] = {
    tableList
      .map(table => (table, tableFieldMap.get(table.name)))
      .collect {
        case (table, Some(fields)) => FullTableModel(table.name, table.isView, fields)
      }
  }

  private def parseToFieldModel(f: (String, String)): FieldModel = {
    FieldModel(f._1, f._2)
  }

  private def parseToTableModel(t: (String, String)): TableModel = {
    TableModel(t._1, isTableTypeView(t._2))
  }

  private def isTableTypeView(tableType: String): Boolean = tableType match {
    case "VIEW" => true
    case _ => false
  }
}
