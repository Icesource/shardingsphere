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
import org.apache.shardingsphere.driver.jdbc.core.datasource.ShardingSphereDataSource;
import org.apache.shardingsphere.infra.yaml.config.YamlRootRuleConfigurations;
import org.apache.shardingsphere.infra.yaml.swapper.YamlRuleConfigurationSwapperEngine;
import org.apache.shardingsphere.scaling.core.config.DataSourceConfiguration;
import org.apache.shardingsphere.scaling.core.config.JDBCDataSourceConfiguration;
import org.apache.shardingsphere.scaling.core.config.ShardingTargetDataSourceConfiguration;
import org.apache.shardingsphere.scaling.core.exception.PrepareFailedException;

import java.sql.SQLException;

/**
 * Data source factory.
 */
public final class DataSourceFactory {
    
    private static final YamlRuleConfigurationSwapperEngine SWAPPER_ENGINE = new YamlRuleConfigurationSwapperEngine();
    
    /**
     * New instance data source.
     *
     * @param dataSourceConfiguration data source configuration
     * @return new data source
     */
    public DataSourceWrapper newInstance(final DataSourceConfiguration dataSourceConfiguration) {
        if (dataSourceConfiguration instanceof JDBCDataSourceConfiguration) {
            return newInstanceDataSourceByJDBC((JDBCDataSourceConfiguration) dataSourceConfiguration);
        }
        if (dataSourceConfiguration instanceof ShardingTargetDataSourceConfiguration) {
            return newInstanceDataSourceByShardingJDBC((ShardingTargetDataSourceConfiguration) dataSourceConfiguration);
        }
        throw new PrepareFailedException("Unsupported data source configuration");
    }
    
    private DataSourceWrapper newInstanceDataSourceByJDBC(final JDBCDataSourceConfiguration dataSourceConfiguration) {
        HikariDataSource result = new HikariDataSource();
        result.setJdbcUrl(dataSourceConfiguration.getJdbcUrl());
        result.setUsername(dataSourceConfiguration.getUsername());
        result.setPassword(dataSourceConfiguration.getPassword());
        return new HikariDataSourceWrapper(result);
    }
    
    private DataSourceWrapper newInstanceDataSourceByShardingJDBC(final ShardingTargetDataSourceConfiguration dataSourceConfiguration) {
        YamlRootRuleConfigurations configurations = dataSourceConfiguration.getConfigurations();
        try {
            return new ShardingJDBCDataSourceWrapper(
                    (ShardingSphereDataSource) ShardingSphereDataSourceFactory.createDataSource(configurations.getDataSources(), SWAPPER_ENGINE.swapToRuleConfigurations(configurations.getRules()), configurations.getProps())
            );
        } catch (SQLException ex) {
            throw new PrepareFailedException("Failed to create shardingJDBC data source");
        }
    }
}
