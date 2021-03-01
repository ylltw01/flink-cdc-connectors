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

package com.alibaba.ververica.cdc.connectors.sqlserver.table;


import com.alibaba.ververica.cdc.connectors.sqlserver.SQLServerSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class SQLServerTableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final int port;
    private final String hostname;
    private final String database;
    private final String username;
    private final String password;
    private final String tableName;
    private final Properties dbzProperties;

    public SQLServerTableSource(
            TableSchema physicalSchema,
            int port,
            String hostname,
            String database,
            String tableName,
            String username,
            String password,
            Properties dbzProperties) {
            this.physicalSchema = physicalSchema;
            this.port = port;
            this.hostname = checkNotNull(hostname);
            this.database = checkNotNull(database);
            this.tableName = checkNotNull(tableName);
            this.username = checkNotNull(username);
            this.password = checkNotNull(password);
            this.dbzProperties = dbzProperties;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(physicalSchema.toRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer = new RowDataDebeziumDeserializeSchema(
                rowType,
                typeInfo,
                ((rowData, rowKind) -> {
                }), ZoneId.of("UTC"));
        SQLServerSource.Builder<RowData> builder = SQLServerSource.<RowData>builder()
                .hostname(hostname)
                .port(port)
                .databaseList(database)
                .tableList(tableName)
                .username(username)
                .password(password)
                .debeziumProperties(dbzProperties)
                .deserializer(deserializer);
        DebeziumSourceFunction<RowData> sourceFunction = builder.build();
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SQLServerTableSource(
                physicalSchema,
                port,
                hostname,
                database,
                tableName,
                username,
                password,
                dbzProperties
        );
    }

    @Override
    public String asSummaryString() {
        return "SQLServer-CDC";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SQLServerTableSource that = (SQLServerTableSource) o;
        return port == that.port &&
                Objects.equals(physicalSchema, that.physicalSchema) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(database, that.database) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(dbzProperties, that.dbzProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(physicalSchema, port, hostname, database, username, password, tableName, dbzProperties);
    }

}
