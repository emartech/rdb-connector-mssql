package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionConfigError, ConnectionError, ConnectorError}
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
      val mBeanServer = ManagementFactory.getPlatformMBeanServer
      val poolObjectName = new ObjectName(s"com.zaxxer.hikari:type=Pool ($poolName)")
      val poolProxy = JMX.newMXBeanProxy(mBeanServer, poolObjectName, classOf[HikariPoolMXBean])

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

  def apply(config: MsSqlConnectionConfig,
            connectorConfig: MsSqlConnectorConfig = defaultConfig
           )(executor: AsyncExecutor)
           (implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    createMsSqlConnector(config, connectorConfig)(executor)
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

trait MsSqlConnectorTrait extends ConnectorCompanion {

  protected def createMsSqlConnector(config: MsSqlConnectionConfig,
            connectorConfig: MsSqlConnectorConfig
           )(executor: AsyncExecutor)
           (implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {

    val keystoreFilePathO = CertificateUtil.createKeystoreTempFileFromCertificateString(config.certificate)
    val poolName = UUID.randomUUID.toString

    if (keystoreFilePathO.isEmpty) {
      Future.successful(Left(ConnectionConfigError("Wrong SSL cert format")))
    } else if (!checkSsl(config.connectionParams)) {
      Future.successful(Left(ConnectionConfigError("SSL Error")))
    } else {
      val keystoreFilePath = keystoreFilePathO.get

      val db = {
        val url = createUrl(config.host, config.port, config.dbName, config.connectionParams)
        val customDbConf = ConfigFactory.load()
          .withValue("mssqldb.poolName", ConfigValueFactory.fromAnyRef(poolName))
          .withValue("mssqldb.registerMbeans", ConfigValueFactory.fromAnyRef(true))
          .withValue("mssqldb.properties.url", ConfigValueFactory.fromAnyRef(url))
          .withValue("mssqldb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
          .withValue("mssqldb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
          .withValue("mssqldb.properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.SQLServerProfile"))
          .withValue("mssqldb.properties.properties.encrypt", ConfigValueFactory.fromAnyRef("true"))
          .withValue("mssqldb.properties.properties.trustServerCertificate", ConfigValueFactory.fromAnyRef("false"))
          .withValue("mssqldb.properties.properties.trustStore", ConfigValueFactory.fromAnyRef(keystoreFilePath))

        Database.forConfig("mssqldb", customDbConf)
      }

      checkConnection(db).map[Either[ConnectorError, MsSqlConnector]] { _ =>
        Right(new MsSqlConnector(db, connectorConfig, poolName))
      }.recover {
        case ex => Left(ConnectionError(ex))
      }.map {
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

  protected def checkConnection(db: Database)(
    implicit executionContext: ExecutionContext): Future[Unit] = {
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