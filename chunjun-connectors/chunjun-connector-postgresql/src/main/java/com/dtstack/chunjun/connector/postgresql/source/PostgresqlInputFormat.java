/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.postgresql.source;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.util.SqlUtil;
import com.dtstack.chunjun.util.ExceptionUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class PostgresqlInputFormat extends JdbcInputFormat {

    @Override
    protected void queryPollingWithOutStartLocation() throws SQLException {
        // In PostgreSQL, if resultCursorType is FORWARD_ONLY
        // , the query will report an error after the method
        // #setFetchDirection(ResultSet.FETCH_REVERSE) is called.
        String querySql =
                jdbcConf.getQuerySql() + SqlUtil.buildOrderSql(jdbcConf, jdbcDialect, "ASC");
        ps =
                dbConn.prepareStatement(
                        querySql, ResultSet.TYPE_SCROLL_INSENSITIVE, resultSetConcurrency);
        ps.setFetchSize(jdbcConf.getFetchSize());
        ps.setQueryTimeout(jdbcConf.getQueryTimeOut());
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();

        try {
            // 间隔轮询一直循环，直到查询到数据库中的数据为止
            while (!hasNext) {
                TimeUnit.MILLISECONDS.sleep(jdbcConf.getPollingInterval());
                resultSet.close();
                // 如果事务不提交 就会导致数据库即使插入数据 也无法读到数据
                dbConn.commit();
                resultSet = ps.executeQuery();
                hasNext = resultSet.next();
                // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
                if ((System.currentTimeMillis() - startTime) % 300000
                        <= jdbcConf.getPollingInterval()) {
                    LOG.info(
                            "no record matched condition in database, execute query sql = {}, startLocation = {}",
                            jdbcConf.getQuerySql(),
                            endLocationAccumulator.getLocalValue());
                }
            }
        } catch (InterruptedException e) {
            LOG.warn(
                    "interrupted while waiting for polling, e = {}",
                    ExceptionUtil.getErrorMessage(e));
        }

        // 查询到数据，更新querySql
        StringBuilder builder = new StringBuilder(128);
        builder.append(jdbcConf.getQuerySql());
        if (jdbcConf.getQuerySql().contains("WHERE")) {
            builder.append(" AND ");
        } else {
            builder.append(" WHERE ");
        }
        builder.append(jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()))
                .append(" > ? ORDER BY ")
                .append(jdbcDialect.quoteIdentifier(jdbcConf.getIncreColumn()))
                .append(" ASC");
        jdbcConf.setQuerySql(builder.toString());
        ps =
                dbConn.prepareStatement(
                        jdbcConf.getQuerySql(),
                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                        resultSetConcurrency);
        ps.setFetchSize(jdbcConf.getFetchSize());
        ps.setQueryTimeout(jdbcConf.getQueryTimeOut());
        LOG.info("update querySql, sql = {}", jdbcConf.getQuerySql());
    }
}
