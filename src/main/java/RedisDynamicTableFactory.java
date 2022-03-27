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


import static redis.options.RedisOptions.HOSTNAME;
import static redis.options.RedisOptions.LOOKUP_CACHE_MAX_ROWS;
import static redis.options.RedisOptions.LOOKUP_CACHE_TTL;
import static redis.options.RedisOptions.LOOKUP_MAX_RETRIES;
import static redis.options.RedisOptions.PORT;
import static redis.options.RedisWriteOptions.BATCH_SIZE;
import static redis.options.RedisWriteOptions.IS_BATCH_MODE;
import static redis.options.RedisWriteOptions.WRITE_MODE;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import redis.options.RedisLookupOptions;
import redis.options.RedisOptions;
import redis.source.RedisDynamicTableSource;

/**
 * Factory for creating configured instances of {@link RedisDynamicTableSource}.
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        // discover a suitable decoding format
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);

        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();

        final RedisLookupOptions redisLookupOptions = RedisOptions.getRedisLookupOptions(options);

        TableSchema schema = context.getCatalogTable().getSchema();

        Configuration c = (Configuration) context.getConfiguration();

        boolean isDimBatchMode = c.getBoolean("is.dim.batch.mode", false);

        return new RedisDynamicTableSource(
                schema.toPhysicalRowDataType()
                , decodingFormat
                , redisLookupOptions
                , isDimBatchMode);
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        options.add(LOOKUP_CACHE_MAX_ROWS);
        options.add(LOOKUP_CACHE_TTL);
        options.add(LOOKUP_MAX_RETRIES);
        options.add(WRITE_MODE);
        options.add(IS_BATCH_MODE);
        options.add(BATCH_SIZE);
        return options;
    }
}
