/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.scaling.core.config;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.database.metadata.DataSourceMetaData;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;

import java.util.Map;


/**
 * Sharding Destination JDBC data source configuration.
 */
@Getter
@Setter
@EqualsAndHashCode(exclude = {"databaseType"})
public class ShardingTargetDataSourceConfiguration implements DataSourceConfiguration {

    private Map<String, JDBCDataSourceConfiguration> dataSources;

    private ShardingRuleConfiguration shardingRule;

    private DatabaseType databaseType;

    private DataSourceMetaData dataSourceMetaData;

    public ShardingTargetDataSourceConfiguration(final Map<String, JDBCDataSourceConfiguration> dataSources, final ShardingRuleConfiguration shardingRule) {
        this.dataSources = dataSources;
        this.shardingRule = shardingRule;
        JDBCDataSourceConfiguration configuration = getFirstOrNull(this.dataSources);
        this.databaseType = getFirstOrNull(this.dataSources) != null ? configuration.getDatabaseType() : null;
        this.dataSourceMetaData = this.databaseType != null ? databaseType.getDataSourceMetaData(configuration.getJdbcUrl(), configuration.getUsername()) : null;
    }

    @Override
    public DataSourceMetaData getDataSourceMetaData() {
        return this.dataSourceMetaData;
    }

    private JDBCDataSourceConfiguration getFirstOrNull(final Map<String, JDBCDataSourceConfiguration> map) {
        JDBCDataSourceConfiguration result = null;
        for (Map.Entry<String, JDBCDataSourceConfiguration> entry : map.entrySet()) {
            result = entry.getValue();
            if (result != null) {
                break;
            }
        }
        return result;
    }
}
