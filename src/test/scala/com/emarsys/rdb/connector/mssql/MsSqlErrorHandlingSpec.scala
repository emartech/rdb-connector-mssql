package com.emarsys.rdb.connector.mssql

import java.sql.SQLException
import java.util.concurrent.RejectedExecutionException

import com.emarsys.rdb.connector.common.models.Errors._
import com.microsoft.sqlserver.jdbc.SQLServerException
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, PrivateMethodTester, WordSpecLike}

class MsSqlErrorHandlingSpec extends WordSpecLike with Matchers with MockitoSugar with PrivateMethodTester {

  "ErrorHandling" should {

    val possibleSQLErrorsCodes = Seq(
      ("HY008", "query cancelled", QueryTimeout("msg")),
      ("S0001", "sql syntax error", SqlSyntaxError("msg")),
      ("S0005", "permission denied", AccessDeniedError("msg")),
      ("S0002", "invalid object name", TableNotFound("msg"))
    )

    val possibleConnectionErrors = Seq(
      ("08S01", "bad host error")
    )

    possibleSQLErrorsCodes.foreach(
      errorWithResponse =>
        s"""convert ${errorWithResponse._2} to ${errorWithResponse._3.getClass.getSimpleName}""" in new MsSqlErrorHandling {
          val error = new SQLServerException("msg", errorWithResponse._1, 0, new Exception())
          eitherErrorHandler().apply(error) shouldEqual Left(errorWithResponse._3)
        }
    )

    possibleConnectionErrors.foreach(
      errorCode =>
        s"""convert ${errorCode._2} to ConnectionError""" in new MsSqlErrorHandling {
          val error = new SQLException("msg", errorCode._1)
          eitherErrorHandler().apply(error) shouldEqual Left(ConnectionError(error))
        }
    )

    "convert RejectedExecutionException to TooManyQueries" in new MsSqlErrorHandling {
      val error = new RejectedExecutionException("msg")
      eitherErrorHandler().apply(error) shouldBe
        Left(TooManyQueries("msg"))
    }

    "handle unexpected SqlException" in new MsSqlErrorHandling {
      val error = new SQLException("not-handled-message", "not-handled-state")
      eitherErrorHandler().apply(error) shouldBe
        Left(ErrorWithMessage("[not-handled-state] - not-handled-message"))
    }

    "handle unexpected exception" in new MsSqlErrorHandling {
      val error = new Exception("not-handled-message")
      eitherErrorHandler().apply(error) shouldBe
        Left(ErrorWithMessage("not-handled-message"))
    }
  }
}
