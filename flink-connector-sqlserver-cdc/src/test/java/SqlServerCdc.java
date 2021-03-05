/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * SQLServer 官网
 * https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server?view=sql-server-2017#change-data-capture-agent-jobs
 * <p>
 * https://docs.microsoft.com/zh-cn/sql/relational-databases/track-changes/about-change-data-capture-sql-server?view=sql-server-2017#change-data-capture-agent-jobs
 * https://docs.microsoft.com/zh-cn/sql/relational-databases/system-stored-procedures/sys-sp-cdc-start-job-transact-sql?view=sql-server-2017
 * <p>
 * <p>
 * SQLServer 启用CDC：
 * 第一步：数据库开启CDC：
 * USE MyDB
 * GO
 * EXEC sys.sp_cdc_enable_db
 * GO
 * <p>
 * 第二步：表开启CDC
 * -- USE MyDB
 * -- GO
 * -- EXEC sys.sp_cdc_enable_table
 * --  @source_schema = N'dbo',
 * --  @source_name   = N'MyTable',
 * --  @role_name     = N'MyRole',
 * --  @filegroup_name = N'MyDB_CT',
 * -- @supports_net_changes = 0
 * -- GO
 * <p>
 * 第三步：开启SQLServer Agent
 * USE test; -- 数据库
 * GO
 * EXEC sys.sp_cdc_start_job;
 * GO
 *
 *
 * <p>
 * SQLServer 修改了表结构：
 * debezium 解决办法：https://debezium.io/documentation/reference/1.4/connectors/sqlserver.html#sqlserver-schema-evolution
 * <p>
 * 这里只写了SQL执行步骤，具体的 online 和 offline 参见官方链接和注意事项
 * -- SQL 手动干预方案SQL
 * use test;
 * <p>
 * GO
 * -- 第一步：先将针对这个表旧的 capture_instance 创建一个新的 capture_instance
 * -- EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sqlserver_cdc', @role_name = NULL, @supports_net_changes = 0, @capture_instance = 'dbo_sqlserver_cdc_v2';
 * -- 第二步：确认debezium 能继续消费
 * -- 第三步：能继续消费后，disable 旧的 capture_instance
 * -- EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'sqlserver_cdc', @capture_instance = 'dbo_sqlserver_cdc';
 * GO
 *
 * <p>
 * -- 查询SQLServer 开启的 capture_instance
 * GO
 * EXEC sys.sp_cdc_help_change_data_capture
 * GO
 */
public class SqlServerCdc {
    private Executor executor = Executors.newSingleThreadExecutor();

    private final EmbeddedEngine engine;

    public SqlServerCdc() {
        // Define the configuration for the embedded and MySQL connector ...
        Configuration config = Configuration.create()
                /* begin engine properties */
                .with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "/Users/noone/IdeaProjects/flink-cdc-connectors/flink-connector-sqlserver-cdc/src/test/offset/offset.dat")
                .with("offset.flush.interval.ms", 60000)
                /* begin connector properties */
                .with("name", "my-sql-connector")
                .with("database.hostname", "10.0.8.69")
                .with("database.port", 1433)
                .with("database.user", "sa")
                .with("database.password", "Cdsf@119")
                .with("database.dbname", "test")
                .with("table.include.list", "dbo.sqlserver_cdc")
                .with("database.server.name", "sqlserver-cdc-8.65")
                .with("snapshot.mode", "initial")
                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename", "/Users/noone/IdeaProjects/flink-cdc-connectors/flink-connector-sqlserver-cdc/src/test/offset/dbhistory.dat")
                .build();

        // Create the engine with this configuration ...
        engine = EmbeddedEngine.create()
                .using(config)
                .notifying(this::handleEvent)
                .build();

        // Run the engine asynchronously ...
        executor = Executors.newSingleThreadExecutor();
    }

    public void start() {
        this.executor.execute(engine);
    }

    public void stop() {
        if (this.engine != null) {
            this.engine.stop();
        }
    }


    public void handleEvent(SourceRecord sourceRecord) {
        Struct sourceRecordValue = (Struct) sourceRecord.value();

        System.out.println(sourceRecord);
    }

    public static void main(String[] args) {
        SqlServerCdc mysqlCdc = new SqlServerCdc();
        mysqlCdc.start();
    }

}

/*
-- SQLServer 建表
CREATE TABLE test.dbo.sqlserver_cdc (
	id int NOT NULL,
	name varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	description varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	weight varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	create_time datetime NULL,
	CONSTRAINT sqlserver_cdc_PK PRIMARY KEY (id)
) GO


CREATE TABLE test.dbo.sqlserver_all_type_cdc (
	id int NOT NULL,
	bigint_2 bigint NULL,
	bit_3 bit NULL,
	char_4 char(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	date_5 date NULL,
	datetime_6 datetime NULL,
	datetime2_7 datetime2(7) NULL,
	decimai_8 decimal(18,0) NULL,
	float_9 float NULL,
	money_10 money NULL,
	nchar_11 nchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	ntext_12 ntext(1073741823) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	numeric_13 numeric(18,0) NULL,
	nvarchar_14 nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	real_15 real NULL,
	samlldatatime_16 smalldatetime NULL,
	text_17 text(2147483647) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	time_18 time NULL,
	tinyint_20 tinyint NULL,
	varchar_21 varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	xml_22 xml NULL,
	timestamp_19 timestamp(8) NULL,
	CONSTRAINT sqlserver_all_type_cdc_PK PRIMARY KEY (id)
) GO


-- MySQL 建表
CREATE TABLE `mysql_sqlserver_cdc` (
  `id` int(11) NOT NULL,
  `name` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `weight` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin


CREATE TABLE `mysql_sqlserver_all_type_cdc` (
  `id` int(11) NOT NULL,
  `bigint_2` bigint(20) DEFAULT NULL,
  `bit_3` int(11) DEFAULT NULL,
  `char_4` char(100) COLLATE utf8_bin DEFAULT NULL,
  `date_5` date DEFAULT NULL,
  `datetime_6` datetime DEFAULT NULL,
  `datetime2_7` datetime DEFAULT NULL,
  `decimai_8` decimal(18,0) DEFAULT NULL,
  `float_9` float DEFAULT NULL,
  `money_10` decimal(18,0) DEFAULT NULL,
  `nchar_11` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `ntext_12` text COLLATE utf8_bin,
  `numeric_13` decimal(18,0) DEFAULT NULL,
  `nvarchar_14` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `real_15` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `samlldatatime_16` datetime DEFAULT NULL,
  `text_17` text COLLATE utf8_bin,
  `time_18` bigint(20) DEFAULT NULL,
  `tinyint_20` int(11) DEFAULT NULL,
  `varchar_21` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `xml_22` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  `timestamp_19` varchar(100) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin


-- flink 语句，SQLServer timestamp 要映射为 String 类型
 tableEnv.executeSql("CREATE TABLE sqlserver_all_type_cdc (" +
                        " id INT NOT NULL," +
                        " bigint_2 INT," +
                        " bit_3 BOOLEAN," +
                        " char_4 STRING," +
                        " date_5 DATE," +
                        " datetime_6 TIMESTAMP," +
                        " datetime2_7 TIMESTAMP," +
                        " decimai_8 DOUBLE," +
                        " float_9 DOUBLE," +
                        " money_10 DOUBLE," +
                        " nchar_11 STRING," +
                        " ntext_12 STRING," +
                        " numeric_13 DOUBLE," +
                        " nvarchar_14 STRING," +
                        " real_15 STRING," +
                        " samlldatatime_16 TIMESTAMP," +
                        " text_17 STRING," +
                        " time_18 INT NULL," +
                        " tinyint_20 int NULL," +
                        " varchar_21 STRING," +
                        " xml_22 STRING," +
                        " timestamp_19 STRING " +
                ") WITH (" +
                        " 'connector' = 'sqlserver-cdc'," +
                        " 'hostname' = '10.0.8.69'," +
                        " 'port' = '1433'," +
                        " 'username' = 'sa'," +
                        " 'password' = 'Cdsf@119'," +
                        " 'database-name' = 'test'," +
                        " 'table-name' = 'dbo.sqlserver_all_type_cdc'" +
                        ")");

        TableResult sinkMysql = tableEnv.executeSql("CREATE TABLE mysql_sqlserver_all_type_cdc (" +
                " id INT NOT NULL," +
                " bigint_2 INT," +
                " bit_3 BOOLEAN," +
                " char_4 STRING," +
                " date_5 DATE," +
                " datetime_6 TIMESTAMP," +
                " datetime2_7 TIMESTAMP," +
                " decimai_8 DOUBLE," +
                " float_9 DOUBLE," +
                " money_10 DOUBLE," +
                " nchar_11 STRING," +
                " ntext_12 STRING," +
                " numeric_13 DOUBLE," +
                " nvarchar_14 STRING," +
                " real_15 STRING," +
                " samlldatatime_16 TIMESTAMP," +
                " text_17 STRING," +
                " time_18 INT NULL," +
                " tinyint_20 int NULL," +
                " varchar_21 STRING," +
                " xml_22 STRING," +
                " timestamp_19 STRING, " +
                " PRIMARY KEY (`id`) NOT ENFORCED " +
                ") WITH (" +
                " 'connector' = 'jdbc'," +
                " 'url' = 'jdbc:mysql://10.0.8.45:3306/test_etl'," +
                " 'username' = 'root', " +
                " 'password' = 'Cdsf@119', " +
                " 'table-name' = 'mysql_sqlserver_all_type_cdc' " +
                ")");

 */
