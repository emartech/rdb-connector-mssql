package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectorError}
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mssql.MsSqlConnector.{MsSqlConnectionConfig, MsSqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.jdbc.SQLServerProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class MsSqlConnector(
    protected val db: Database,
    protected val connectorConfig: MsSqlConnectorConfig,
    protected val poolName: String
)(
    implicit val executionContext: ExecutionContext
) extends Connector
    with MsSqlErrorHandling
    with MsSqlTestConnection
    with MsSqlMetadata
    with MsSqlSimpleSelect
    with MsSqlRawSelect
    with MsSqlIsOptimized
    with MsSqlRawDataManipulation {

  override protected val fieldValueConverters = MsSqlFieldValueConverters

  override def close(): Future[Unit] = db.shutdown

  override def innerMetrics(): String = {
    import java.lang.management.ManagementFactory

    import com.zaxxer.hikari.HikariPoolMXBean
    import javax.management.{JMX, ObjectName}
    Try {
      val mBeanServer    = ManagementFactory.getPlatformMBeanServer
      val poolObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy      = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

      s"""{
         |"activeConnections": ${poolProxy.getActiveConnections},
         |"idleConnections": ${poolProxy.getIdleConnections},
         |"threadAwaitingConnections": ${poolProxy.getThreadsAwaitingConnection},
         |"totalConnections": ${poolProxy.getTotalConnections}
         |}""".stripMargin
    }.getOrElse(super.innerMetrics)
  }

}

object MsSqlConnector extends MsSqlConnectorTrait {

  def apply(
      config: MsSqlConnectionConfig,
      connectorConfig: MsSqlConnectorConfig = defaultConfig,
      configPath: String = "mssqldb"
  )(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    createMsSqlConnector(config, configPath, connectorConfig)(executor)
  }

  case class MsSqlConnectionConfig(
      host: String,
      port: Int,
      dbName: String,
      dbUser: String,
      dbPassword: String,
      certificate: String,
      connectionParams: String
  ) extends ConnectionConfig {

    override def toCommonFormat: CommonConnectionReadableData = {
      CommonConnectionReadableData("mssql", s"$host:$port", dbName, dbUser)
    }
  }

  case class MsSqlConnectorConfig(
      queryTimeout: FiniteDuration,
      streamChunkSize: Int
  )

}

trait MsSqlConnectorTrait extends ConnectorCompanion with MsSqlErrorHandling {

  protected def createMsSqlConnector(
      config: MsSqlConnectionConfig,
      configPath: String,
      connectorConfig: MsSqlConnectorConfig
  )(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    val keystoreFilePathO = CertificateUtil.createKeystoreTempFileFromCertificateString(config.certificate)
    val poolName          = UUID.randomUUID.toString

    if (keystoreFilePathO.isEmpty) {
      Future.successful(Left(ConnectionConfigError("Wrong SSL cert format")))
    } else if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    } else {
      val keystoreFilePath = keystoreFilePathO.get

      val db = {
        val url = createUrl(config.host, config.port, config.dbName, config.connectionParams)
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
          .withValue("properties.properties.trustStore", ConfigValueFactory.fromAnyRef(keystoreFilePath))

        Database.forConfig("", customDbConf)
      }

      checkConnection(db)
        .map[Either[ConnectorError, MsSqlConnector]] { _ =>
          Right(new MsSqlConnector(db, connectorConfig, poolName))
        }
        .recover(eitherErrorHandler())
        .map {
          case Left(e) =>
            db.shutdown
            Left(e)
          case r => r
        }
    }
  }

  val defaultConfig = MsSqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  protected def checkConnection(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[(String)]).map(_ => {})
  }

  private[mssql] def createUrl(host: String, port: Int, database: String, connectionParams: String) = {
    s"jdbc:sqlserver://$host:$port;databaseName=$database${safeConnectionParams(connectionParams)}"
  }

  private[mssql] def checkSsl(connectionParams: String): Boolean = {
    !connectionParams.matches(".*encrypt=false.*") &&
    !connectionParams.matches(".*trustServerCertificate=true.*") &&
    !connectionParams.matches(".*trustStore=.*")
  }

  private[mssql] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith(";") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s";$connectionParams"
    }
  }

  override def meta(): MetaData = MetaData("\"", "'", "'")
}
