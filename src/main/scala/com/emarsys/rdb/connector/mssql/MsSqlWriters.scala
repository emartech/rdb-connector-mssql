package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect.{FieldName, TableName, Value}

trait MsSqlWriters extends DefaultSqlWriters {
  override implicit lazy val tableNameWriter: SqlWriter[TableName] = (tableName: TableName) => msSqlNameWrapper(tableName.t)
  override implicit lazy val fieldNameWriter: SqlWriter[FieldName] = (fieldName: FieldName) => msSqlNameWrapper(fieldName.f)
  override implicit lazy val valueWriter: SqlWriter[Value] = (value: Value) => msSqlValueQuoter(Option(value.v))

  protected def msSqlNameWrapper(name: String): String = {
    s"[$name]"
  }

  protected def msSqlValueQuoter(text: Option[String]): String = {
    text
      .map(_.replace("'", "''"))
      .map(text => s"'$text'")
      .getOrElse("NULL")
  }

}

object MsSqlWriters extends MsSqlWriters

