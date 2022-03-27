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

package redis.source;

import static redis.options.RedisOptions.createValueFormatProjection;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import redis.mapper.LookupRedisMapper;
import redis.options.RedisLookupOptions;

public class RedisDynamicTableSource implements LookupTableSource {
    /**
     * Data type to configure the formats.
     */
    protected final DataType physicalDataType;

    /**
     * Optional format for decoding keys from Kafka.
     */
    protected final @Nullable
    DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    protected final RedisLookupOptions redisLookupOptions;

    private final boolean isDimBatchMode;

    public RedisDynamicTableSource(
            DataType physicalDataType
            , DecodingFormat<DeserializationSchema<RowData>> decodingFormat
            , RedisLookupOptions redisLookupOptions
            , boolean isDimBatchMode) {

        // Format attributes
        this.physicalDataType =
                Preconditions.checkNotNull(
                        physicalDataType, "Physical data type must not be null.");
        this.decodingFormat = decodingFormat;
        this.redisLookupOptions = redisLookupOptions;

        this.isDimBatchMode = isDimBatchMode;
    }


    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {

        FlinkJedisConfigBase flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder()
                .setHost(this.redisLookupOptions.getHostname())
                .setPort(this.redisLookupOptions.getPort())
                .build();

        LookupRedisMapper lookupRedisMapper = new LookupRedisMapper(
                this.createDeserialization(context, this.decodingFormat, createValueFormatProjection(this.physicalDataType)));

        if (isDimBatchMode) {
            return TableFunctionProvider.of(new RedisRowDataBatchLookupFunction(
                    flinkJedisConfigBase
                    , lookupRedisMapper
                    , this.redisLookupOptions));
//            return TableFunctionProvider.of(new RedisRowDataLookupFunction(
//                    flinkJedisConfigBase
//                    , lookupRedisMapper
//                    , this.redisLookupOptions));
        } else {
            return TableFunctionProvider.of(new RedisRowDataLookupFunction(
                    flinkJedisConfigBase
                    , lookupRedisMapper
                    , this.redisLookupOptions));
        }
    }

    private @Nullable
    DeserializationSchema<RowData> createDeserialization(
            Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
            int[] projection) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                DataTypeUtils.projectRow(this.physicalDataType, projection);
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
