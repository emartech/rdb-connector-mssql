package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectionError, ConnectorError}
import com.emarsys.rdb.connector.common.models.{CommonConnectionReadableData, ConnectionConfig}
import com.emarsys.rdb.connector.mssql.MsSqlAzureConnector.{MsSqlAzureConnectionConfig, MsSqlAzureConnectorConfig}
import com.emarsys.rdb.connector.mssql.MsSqlConnector.MsSqlConnectorConfig
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.SQLServerProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

object MsSqlAzureConnector extends MsSqlAzureConnectorTrait {

  def apply(
      config: MsSqlAzureConnectionConfig,
      connectorConfig: MsSqlAzureConnectorConfig = defaultAzureConfig,
      configPath: String = "mssqldb"
  )(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    createMsSqlAzureConnector(config, configPath, connectorConfig)(executor)
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

  case class MsSqlAzureConnectorConfig(queryTimeout: FiniteDuration, streamChunkSize: Int) {
    def toMsSqlConnectorConfig = MsSqlConnectorConfig(queryTimeout, streamChunkSize)
  }
}

trait MsSqlAzureConnectorTrait extends MsSqlConnectorTrait {
  protected def createMsSqlAzureConnector(
      config: MsSqlAzureConnectionConfig,
      configPath: String,
      connectorConfig: MsSqlAzureConnectorConfig
  )(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    val poolName = UUID.randomUUID.toString

    if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    } else if (!checkAzureUrl(config.host)) {
      Future.successful(Left(ConnectionConfigError("Wrong Azure SQL host!")))
    } else {
      val db = {
        val url = createUrl(config.host, 1433, config.dbName, config.connectionParams)
        val customDbConf = ConfigFactory
          .load()
          .getConfig(configPath)
          .withValue("poolName", ConfigValueFactory.fromAnyRef(poolName))
          .withValue("registerMbeans", ConfigValueFactory.fromAnyRef(true))
          .withValue("properties.url", ConfigValueFactory.fromAnyRef(url))
          .withValue("properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
          .withValue("properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
          .withValue("properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.SQLServerProfile"))
          .withValue("properties.properties.encrypt", ConfigValueFactory.fromAnyRef("true"))
          .withValue("properties.properties.trustServerCertificate", ConfigValueFactory.fromAnyRef("false"))

        Database.forConfig("", customDbConf)
      }

      checkConnection(db)
        .map[Either[ConnectorError, MsSqlConnector]] { _ =>
          Right(new MsSqlConnector(db, connectorConfig.toMsSqlConnectorConfig, poolName))
        }
        .recover {
          case ex => Left(ConnectionError(ex))
        }
        .map {
          case Left(e) =>
            db.shutdown
            Left(e)
          case r => r
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
