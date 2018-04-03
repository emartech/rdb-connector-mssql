FROM hseeberger/scala-sbt:8u141-jdk_2.12.3_0.13.16

ADD build.sbt /rdb-connector-mssql/build.sbt
ADD project /rdb-connector-mssql/project
ADD src /rdb-connector-mssql/src

WORKDIR /rdb-connector-mssql

RUN sbt clean compile