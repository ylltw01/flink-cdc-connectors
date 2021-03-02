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
