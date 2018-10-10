package com.emarsys.rdb.connector.mssql

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors._
import com.emarsys.rdb.connector.mssql.utils.TestHelper
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import slick.util.AsyncExecutor

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MsSqlConnectorItSpec
    extends TestKit(ActorSystem("connector-it-soec"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val mat      = ActorMaterializer()
  override def afterAll = TestKit.shutdownActorSystem(system)

  "MySqlConnectorItSpec" when {

    val testConnection = TestHelper.TEST_CONNECTION_CONFIG

    "create connector" should {

      "connect success" in {
        val connectorEither = Await.result(MsSqlConnector(testConnection)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]

        connectorEither.right.get.close()
      }

      "connect fail when wrong host" in {
        val conn            = testConnection.copy(host = "wrong")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong user" in {
        val conn            = testConnection.copy(dbUser = "")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong password" in {
        val conn            = testConnection.copy(dbPassword = "")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Left[_, _]]
        connectorEither.left.get shouldBe a[ConnectionError]
      }

      "connect fail when wrong certificate" in {
        val conn            = testConnection.copy(certificate = "")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("Wrong SSL cert format"))
      }

      "connect fail when ssl disabled" in {
        val conn            = testConnection.copy(connectionParams = "encrypt=false")
        val connectorEither = Await.result(MsSqlConnector(conn)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe Left(ConnectionConfigError("SSL Error"))
      }

    }

    "test connection" should {

      "success" in {
        val connectorEither = Await.result(MsSqlConnector(testConnection)(AsyncExecutor.default()), 5.seconds)

        connectorEither shouldBe a[Right[_, _]]

        val connector = connectorEither.right.get

        val result = Await.result(connector.testConnection(), 5.seconds)

        result shouldBe a[Right[_, _]]

        connector.close()
      }

    }

    trait QueryRunnerScope {
      lazy val connectionConfig = testConnection
      lazy val queryTimeout     = 1.second

      def runQuery(q: String): ConnectorResponse[Unit] =
        for {
          Right(connector) <- MsSqlConnector(connectionConfig)(AsyncExecutor.default())
          Right(source)    <- connector.rawSelect(q, limit = None, queryTimeout)
          res              <- sinkOrLeft(source)
          _ = connector.close()
        } yield res

      def sinkOrLeft[T](source: Source[T, NotUsed]): ConnectorResponse[Unit] =
        source
          .runWith(Sink.ignore)
          .map[Either[ConnectorError, Unit]](_ => Right(()))
          .recover {
            case e: ConnectorError => Left[ConnectorError, Unit](e)
          }
    }

    "Custom error handling" should {
      "recognize query timeouts" in new QueryRunnerScope {
        val result = Await.result(runQuery("waitfor delay '00:00:02.000'; select 1"), 2.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[QueryTimeout]
      }

      "recognize syntax errors" in new QueryRunnerScope {
        val result = Await.result(runQuery("select from table"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[SqlSyntaxError]
      }

      "recognize access denied errors" in new QueryRunnerScope {
        override lazy val connectionConfig = TestHelper.TEST_CONNECTION_CONFIG.copy(
          dbUser = "unprivileged",
          dbPassword = "unprivileged1!"
        )
        val result = Await.result(runQuery("select * from access_denied"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe an[AccessDeniedError]
      }

      "recognize if a table is not found" in new QueryRunnerScope {
        val result = Await.result(runQuery("select * from a_non_existing_table"), 1.second)

        result shouldBe a[Left[_, _]]
        result.left.get shouldBe a[TableNotFound]
      }
    }
  }
}
