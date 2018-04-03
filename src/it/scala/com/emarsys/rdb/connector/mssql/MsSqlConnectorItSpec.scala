package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.mssql.utils.TestHelper
import org.scalatest.{Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MsSqlConnectorItSpec extends WordSpecLike with Matchers {

  "MySqlConnectorItSpec" when {

    val testConnection = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(MsSqlConnector(testConnection)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect fail when wrong host" in {
        val conn = testConnection.copy(host = "wrong")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Left[_,_]]
        connectorEither.left.get shouldBe an [ErrorWithMessage]
      }

      "connect fail when wrong user" in {
        val conn = testConnection.copy(dbUser = "")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Left[_,_]]
        connectorEither.left.get shouldBe an [ErrorWithMessage]
      }

      "connect fail when wrong password" in {
        val conn = testConnection.copy(dbPassword = "")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Left[_,_]]
        connectorEither.left.get shouldBe an [ErrorWithMessage]
      }

    }

    "test connection" should {

      "success" in {
        val connectorEither = Await.result(MsSqlConnector(testConnection)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a [Right[_, _]]

        val connector = connectorEither.right.get

        val result = Await.result(connector.testConnection(), 5.seconds)

        result shouldBe a [Right[_, _]]

        connector.close()
      }

    }

  }
}