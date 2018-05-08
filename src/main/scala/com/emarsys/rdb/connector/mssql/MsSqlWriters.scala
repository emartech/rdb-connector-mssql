package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.Value

trait MsSqlWriters extends DefaultSqlWriters {
  override implicit lazy val valueWriter: SqlWriter[Value] = (value: Value) => msSqlValueQuoter(Option(value.v))

  override implicit lazy val simpleSelectWriter: SqlWriter[SimpleSelect] = (ss: SimpleSelect) => {
    val distinct = if(ss.distinct.getOrElse(false)) "DISTINCT " else ""
    val limit = ss.limit.map("TOP " + _ + " ").getOrElse("")
    val head = s"SELECT $distinct$limit${ss.fields.toSql} FROM ${ss.table.toSql}"

    val where = ss.where.map(_.toSql).map(" WHERE " + _).getOrElse("")

    s"$head$where"
  }

  protected def msSqlValueQuoter(text: Option[String]): String = {
    text
      .map(_.replace("'", "''"))
      .map(text => s"'$text'")
      .getOrElse("NULL")
  }

}

object MsSqlWriters extends MsSqlWriters

