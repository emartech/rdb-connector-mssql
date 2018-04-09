package com.emarsys.rdb.connector.mssql

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

class MsSqlAzureConnectorSpec extends WordSpecLike with Matchers with MockitoSugar {

  "MsSqlAzureConnectorSpec" when {

    "#checkAzureUrl" should {

      "return true if end with .database.windows.net" in {
        MsSqlAzureConnector.checkAzureUrl("hello.database.windows.net") shouldBe true
      }

      "return false if end with only database.windows.net" in {
        MsSqlAzureConnector.checkAzureUrl("database.windows.net") shouldBe false
      }

      "return false" in {
        MsSqlAzureConnector.checkAzureUrl("hello.windows.net") shouldBe false
      }

    }
  }
}