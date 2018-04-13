package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import com.emarsys.rdb.connector.common.models.{CommonConnectionReadableData, ConnectionConfig}
import com.emarsys.rdb.connector.mssql.MsSqlAzureConnector.{MsSqlAzureConnectionConfig, MsSqlAzureConnectorConfig}
import com.emarsys.rdb.connector.mssql.MsSqlConnector.MsSqlConnectorConfig
import com.microsoft.sqlserver.jdbc.SQLServerException
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.SQLServerProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

object MsSqlAzureConnector extends MsSqlAzureConnectorTrait {

  def apply(config: MsSqlAzureConnectionConfig,
            connectorConfig: MsSqlAzureConnectorConfig = defaultAzureConfig
           )(executor: AsyncExecutor)
           (implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    createMsSqlAzureConnector(config, connectorConfig)(executor)
  }

  case class MsSqlAzureConnectionConfig(
                                         host: String,
                                         dbName: String,
                                         dbUser: String,
                                         dbPassword: String,
                                         connectionParams: String
                                       ) extends ConnectionConfig {

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mssql-azure", s"$host:1433", dbName, dbUser)
    }
  }

  case class MsSqlAzureConnectorConfig(
                                        queryTimeout: FiniteDuration,
                                        streamChunkSize: Int
                                      ) {
    def toMsSqlConnectorConfig = MsSqlConnectorConfig(queryTimeout, streamChunkSize)
  }

}

trait MsSqlAzureConnectorTrait extends MsSqlConnectorTrait {
  protected def createMsSqlAzureConnector(config: MsSqlAzureConnectionConfig,
                     connectorConfig: MsSqlAzureConnectorConfig
                    )(executor: AsyncExecutor)
                    (implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    val poolName = UUID.randomUUID.toString

    if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ErrorWithMessage("SSL Error")))
    } else if(!checkAzureUrl(config.host)) {
      Future.successful(Left(ErrorWithMessage("Wrong Azure SQL host!")))
    } else {
      val db = {

        val url = createUrl(config.host, 1433, config.dbName, config.connectionParams)

        val customDbConf = ConfigFactory.load()
          .withValue("mssqldb.poolName", ConfigValueFactory.fromAnyRef(poolName))
          .withValue("mssqldb.registerMbeans", ConfigValueFactory.fromAnyRef(true))
          .withValue("mssqldb.properties.url", ConfigValueFactory.fromAnyRef(url))
          .withValue("mssqldb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
          .withValue("mssqldb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
          .withValue("mssqldb.properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.SQLServerProfile"))
          .withValue("mssqldb.properties.properties.encrypt", ConfigValueFactory.fromAnyRef("true"))
          .withValue("mssqldb.properties.properties.trustServerCertificate", ConfigValueFactory.fromAnyRef("false"))

        Database.forConfig("mssqldb", customDbConf)
      }

      checkConnection(db).map[Either[ConnectorError, MsSqlConnector]] { _ =>
        Right(new MsSqlConnector(db, connectorConfig.toMsSqlConnectorConfig, poolName))
      }.recover {
        case err =>
          if(err.getCause.isInstanceOf[SQLServerException]) {
            Left(ErrorWithMessage(err.getCause.getMessage))
          } else {
            Left(ErrorWithMessage("Cannot connect to the sql server"))
          }
      }
    }
  }

  val defaultAzureConfig = MsSqlAzureConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  private[mssql] def checkAzureUrl(url: String): Boolean = {
    url.endsWith(".database.windows.net")
  }
}