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
package redis.container;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Objects;

import redis.clients.jedis.JedisPool;

public class RedisCommandsContainerBuilder {

    public static RedisCommandsContainer build(FlinkJedisConfigBase flinkJedisConfigBase) {
        if (flinkJedisConfigBase instanceof FlinkJedisPoolConfig) {
            FlinkJedisPoolConfig flinkJedisPoolConfig = (FlinkJedisPoolConfig) flinkJedisConfigBase;
            return RedisCommandsContainerBuilder.build(flinkJedisPoolConfig);
        }

//        else if (flinkJedisConfigBase instanceof FlinkJedisClusterConfig) {
//            FlinkJedisClusterConfig flinkJedisClusterConfig = (FlinkJedisClusterConfig) flinkJedisConfigBase;
//            return RedisCommandsContainerBuilder.build(flinkJedisClusterConfig);
//        } else if (flinkJedisConfigBase instanceof FlinkJedisSentinelConfig) {
//            FlinkJedisSentinelConfig flinkJedisSentinelConfig = (FlinkJedisSentinelConfig) flinkJedisConfigBase;
//            return RedisCommandsContainerBuilder.build(flinkJedisSentinelConfig);
//        }

        else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    public static RedisCommandsContainer build(FlinkJedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());

        JedisPool jedisPool = new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(),
                jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(),
                jedisPoolConfig.getDatabase());
        return new RedisContainer(jedisPool);
    }

}
