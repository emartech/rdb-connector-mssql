package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.InsertItSpec

class MsSqlInsertSpec extends TestKit(ActorSystem()) with InsertItSpec with SelectDbInitHelper {

  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  val simpleSelectExisting = SimpleSelect(AllField, TableName(tableName),
    where = Some(
      EqualToValue(FieldName("A1"), Value("v1"))
    ))
}
