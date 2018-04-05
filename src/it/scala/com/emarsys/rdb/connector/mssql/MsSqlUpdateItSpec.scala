package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test.UpdateItSpec

class MsSqlUpdateItSpec  extends TestKit(ActorSystem()) with UpdateItSpec with SelectDbInitHelper {
  val aTableName: String = tableName
  val bTableName: String = s"temp_$uuid"

  override implicit val materializer: Materializer = ActorMaterializer()

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

}
