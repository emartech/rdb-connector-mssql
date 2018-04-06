package com.emarsys.rdb.connector.mssql.utils
import java.util.Properties

import com.emarsys.rdb.connector.mssql.CertificateUtil
import com.emarsys.rdb.connector.mssql.MsSqlConnector.{MsSqlConnectionConfig, createUrl}
import slick.jdbc.MySQLProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.Future

object TestHelper {
  import com.typesafe.config.ConfigFactory
  lazy val config = ConfigFactory.load()

  lazy val TEST_CONNECTION_CONFIG = MsSqlConnectionConfig(
    host = config.getString("dbconf.host"),
    port= config.getString("dbconf.port").toInt,
    dbName= config.getString("dbconf.dbName"),
    dbUser= config.getString("dbconf.user"),
    dbPassword= config.getString("dbconf.password"),
    certificate= config.getString("dbconf.certificate"),
    connectionParams= config.getString("dbconf.connectionParams")
  )

  private lazy val executor = AsyncExecutor.default()
  private lazy val db: Database = {

    val certPath = CertificateUtil.createKeystoreTempFileFromCertificateString(TEST_CONNECTION_CONFIG.certificate)

    val prop = new Properties()
    prop.setProperty("encrypt", "true")
    prop.setProperty("trustServerCertificate", "false")
    prop.setProperty("trustStore", certPath.get)

    val url = createUrl(TEST_CONNECTION_CONFIG)

    Database.forURL(
      url = url,
      driver = "slick.jdbc.SQLServerProfile",
      user = TEST_CONNECTION_CONFIG.dbUser,
      password = TEST_CONNECTION_CONFIG.dbPassword,
      prop = prop,
      executor = executor
    )
  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }
}