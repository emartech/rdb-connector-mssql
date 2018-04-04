package com.emarsys.rdb.connector.mssql

import java.util.UUID

import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage}
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models._
import com.emarsys.rdb.connector.mssql.MsSqlConnector.{MsSqlConnectionConfig, MsSqlConnectorConfig}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import slick.util.AsyncExecutor
import slick.jdbc.SQLServerProfile.api._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

class MsSqlConnector(
                      protected val db: Database,
                      protected val connectorConfig: MsSqlConnectorConfig,
                      protected val poolName: String
                    )(
                      implicit val executionContext: ExecutionContext
                    ) extends Connector
  with MsSqlTestConnection
  with MsSqlMetadata
  with MsSqlSimpleSelect {

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

  case class MsSqlConnectionConfig(
                                    host: String,
                                    port: Int,
                                    dbName: String,
                                    dbUser: String,
                                    dbPassword: String,
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

  def apply(config: MsSqlConnectionConfig,
            connectorConfig: MsSqlConnectorConfig = defaultConfig
           )(executor: AsyncExecutor)
           (implicit executionContext: ExecutionContext): ConnectorResponse[MsSqlConnector] = {
    val poolName = UUID.randomUUID.toString


    val db = {

      val url = createUrl(config)

      val customDbConf = ConfigFactory.load()
        .withValue("mssqldb.poolName", ConfigValueFactory.fromAnyRef(poolName))
        .withValue("mssqldb.registerMbeans", ConfigValueFactory.fromAnyRef(true))
        .withValue("mssqldb.properties.url", ConfigValueFactory.fromAnyRef(url))
        .withValue("mssqldb.properties.user", ConfigValueFactory.fromAnyRef(config.dbUser))
        .withValue("mssqldb.properties.password", ConfigValueFactory.fromAnyRef(config.dbPassword))
        .withValue("mssqldb.properties.driver", ConfigValueFactory.fromAnyRef("slick.jdbc.SQLServerProfile"))

      Database.forConfig("mssqldb", customDbConf)
    }

    checkConnection(db).map[Either[ConnectorError, MsSqlConnector]] { _ =>
      Right(new MsSqlConnector(db, connectorConfig, poolName))
    }.recover {
      case _ => Left(ErrorWithMessage("Cannot connect to the sql server"))
    }
  }


  val defaultConfig = MsSqlConnectorConfig(
    queryTimeout = 20.minutes,
    streamChunkSize = 5000
  )

  private def checkConnection(db: Database)(
    implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[(String)]).map(_ => {})
  }

  private[mssql] def createUrl(config: MsSqlConnectionConfig) = {
    s"jdbc:sqlserver://${config.host}:${config.port};databaseName=${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private[mssql] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith(";") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s";$connectionParams"
    }
  }

  override def meta(): MetaData = ???
}