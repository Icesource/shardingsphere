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

package org.apache.shardingsphere.scaling.core.datasource;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;
import org.apache.shardingsphere.scaling.core.config.DataSourceConfiguration;
import org.apache.shardingsphere.scaling.core.config.JDBCDataSourceConfiguration;
import org.apache.shardingsphere.scaling.core.config.ShardingJDBCDataSourceConfiguration;
import org.apache.shardingsphere.scaling.core.config.ShardingTargetDataSourceConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Data source factory.
 */
public final class DataSourceFactory {
    
    /**
     * New instance data source.
     *
     * @param dataSourceConfiguration data source configuration
     * @return new data source
     */
    public DataSource newInstance(final DataSourceConfiguration dataSourceConfiguration) {
        if (dataSourceConfiguration instanceof JDBCDataSourceConfiguration) {
            return newInstanceDataSourceByJDBC((JDBCDataSourceConfiguration) dataSourceConfiguration);
        }
        if (dataSourceConfiguration instanceof ShardingJDBCDataSourceConfiguration) {
            return newInstanceDataSourceByShardingJDBC((ShardingJDBCDataSourceConfiguration) dataSourceConfiguration);
        }
        if (dataSourceConfiguration instanceof ShardingTargetDataSourceConfiguration) {
            return newInstanceDataSourceByShardingJDBC((ShardingTargetDataSourceConfiguration) dataSourceConfiguration);
        }
        throw new UnsupportedOperationException("Unsupported data source configuration");
    }
    
    private DataSource newInstanceDataSourceByJDBC(final JDBCDataSourceConfiguration dataSourceConfiguration) {
        HikariDataSource result = new HikariDataSource();
        result.setJdbcUrl(dataSourceConfiguration.getJdbcUrl());
        result.setUsername(dataSourceConfiguration.getUsername());
        result.setPassword(dataSourceConfiguration.getPassword());
        return result;
    }
    
    private DataSource newInstanceDataSourceByShardingJDBC(final ShardingJDBCDataSourceConfiguration dataSourceConfiguration) {
        HikariDataSource dataSource1 = new HikariDataSource();
        dataSource1.setJdbcUrl(dataSourceConfiguration.getJdbcUrl());
        dataSource1.setUsername(dataSourceConfiguration.getUsername());
        dataSource1.setPassword(dataSourceConfiguration.getPassword());
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        dataSourceMap.put("ds0", dataSource1);
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        try {
            return ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, Collections.singleton(shardingRuleConfig), new Properties());
        } catch (SQLException ex) {
            throw new UnsupportedOperationException("Failed to create shardingJDBC data source");
        }
    }
    
    private DataSource newInstanceDataSourceByShardingJDBC(final ShardingTargetDataSourceConfiguration dataSourceConfiguration) {
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        Map<String, JDBCDataSourceConfiguration> datasources = dataSourceConfiguration.getDataSources();
        for (Map.Entry<String, JDBCDataSourceConfiguration> entry: datasources.entrySet()) {
            HikariDataSource dataSource = new HikariDataSource();
            JDBCDataSourceConfiguration eachDataSourceConfiguration = entry.getValue();
            dataSource.setJdbcUrl(eachDataSourceConfiguration.getJdbcUrl());
            dataSource.setUsername(eachDataSourceConfiguration.getUsername());
            dataSource.setPassword(eachDataSourceConfiguration.getPassword());
            dataSourceMap.put(entry.getKey(), dataSource);
        }
        ShardingRuleConfiguration shardingRuleConfig = dataSourceConfiguration.getShardingRule();
        try {
            return ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, Collections.singleton(shardingRuleConfig), new Properties());
        } catch (SQLException ex) {
            throw new UnsupportedOperationException("Failed to create shardingJDBC data source");
        }
    }
}
