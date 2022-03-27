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

package redis.mapper;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.calcite.shaded.com.google.common.base.Joiner;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

public class LookupRedisMapper extends AbstractDeserializationSchema<RowData> implements SerializationSchema<Object[]> {


    private DeserializationSchema<RowData> valueDeserializationSchema;

    public LookupRedisMapper(DeserializationSchema<RowData> valueDeserializationSchema) {

        this.valueDeserializationSchema = valueDeserializationSchema;

    }

    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.GET);
    }

    @Override
    public RowData deserialize(byte[] message) {
        try {
            return this.valueDeserializationSchema.deserialize(message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(Object[] element) {
        return Joiner.on(":").join(element).getBytes();
    }
}
