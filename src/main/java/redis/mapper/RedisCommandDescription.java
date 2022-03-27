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

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

public class RedisCommandDescription {

    private static final long serialVersionUID = 1L;

    private RedisCommand redisCommand;

    private String additionalKey;

    public RedisCommandDescription(RedisCommand redisCommand, String additionalKey) {

        this.redisCommand = redisCommand;
        this.additionalKey = additionalKey;

        if (redisCommand.getRedisDataType() == RedisDataType.HASH) {
            if (additionalKey == null) {
                throw new IllegalArgumentException("Hash should have additional key");
            }
        }
    }

    public RedisCommandDescription(RedisCommand redisCommand) {

        this(redisCommand, null);
    }

    public RedisCommand getRedisCommand() {
        return redisCommand;
    }

    public String getAdditionalKey() {
        return additionalKey;
    }
}
