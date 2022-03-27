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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;

public class RedisContainer implements RedisCommandsContainer, Closeable {

    private static final long serialVersionUID = 1L;

    private transient JedisPool jedisPool;
    private transient JedisSentinelPool jedisSentinelPool;

    private static final Logger
            LOG = LoggerFactory.getLogger(RedisContainer.class);


    public RedisContainer(JedisPool jedisPool) {

        this.jedisPool = jedisPool;
        this.jedisSentinelPool = null;
    }

    public RedisContainer(JedisSentinelPool sentinelPool) {

        this.jedisPool = null;
        this.jedisSentinelPool = sentinelPool;
    }

    private Jedis getInstance() {
        if (jedisSentinelPool != null) {
            return jedisSentinelPool.getResource();
        } else {
            return jedisPool.getResource();
        }
    }

    private void releaseInstance(final Jedis jedis) {
        if (jedis == null) {
            return;
        }
        try {
            jedis.close();
        } catch (Exception e) {
            LOG.error("Failed to close (return) instance to pool", e);
        }
    }

    @Override
    public void open() throws Exception {
        getInstance().echo("Test");
    }

    @Override
    public List<Object> multiGet(List<byte[]> key) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            Pipeline pipeline = jedis.pipelined();
            key.forEach(pipeline::get);
            return pipeline.syncAndReturnAll();
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command GET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.get(key);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command GET to key {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public byte[] hget(byte[] key, byte[] hashField) {
        Jedis jedis = null;
        try {
            jedis = getInstance();
            return jedis.hget(key, hashField);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HGET to key {} hashField {} error message {}",
                        key, hashField, e.getMessage());
            }
            throw e;
        } finally {
            releaseInstance(jedis);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.jedisPool != null) {
            this.jedisPool.close();
        }
        if (this.jedisSentinelPool != null) {
            this.jedisSentinelPool.close();
        }
    }
}
