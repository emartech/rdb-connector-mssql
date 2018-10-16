package com.emarsys.rdb.connector.mssql

import java.sql.SQLException

import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, PrivateMethodTester, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class MsSqlErrorHandlingSpec extends WordSpecLike with Matchers with MockitoSugar with PrivateMethodTester {

  "ErrorHandling" should {

    "handle unexpected SqlException" in {

      implicit val executionContext = concurrent.ExecutionContext.Implicits.global

      val eitherErrorHandler =
        PrivateMethod[PartialFunction[Throwable, Either[ConnectorError, String]]]('eitherErrorHandler)
      val handler        = new MsSqlErrorHandling {}
      val unknownFailure = new SQLException("not-handled-message", "not-handled-state", new Exception(""))
      val timeout        = 1.second

      handler invokePrivate eitherErrorHandler()

      Await.result(Future.failed(unknownFailure).recover(handler invokePrivate eitherErrorHandler()), timeout) shouldBe
        Left(ErrorWithMessage("[not-handled-state] - not-handled-message"))
    }
  }
}
