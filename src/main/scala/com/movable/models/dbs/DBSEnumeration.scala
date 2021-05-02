package com.movable.models.dbs

object DBSType extends Enumeration {
  val SQLServer = "sqlserver"
  val MySQL = "mysql"
  val PostgreSQL = "postgresql"
}

object DBSDriver extends Enumeration{
  val MysqlDriver = "com.mysql.jdbc.Driver"
  val PsqlDriver = "org.postgresql.Driver"
  val SqlServerDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}