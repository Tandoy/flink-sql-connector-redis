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

import org.apache.flink.annotation.Internal;
import org.apache.flink.shaded.com.google.common.cache.Cache;
import org.apache.flink.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.container.RedisCommandsContainer;
import redis.container.RedisCommandsContainerBuilder;
import redis.mapper.LookupRedisMapper;
import redis.mapper.RedisCommand;
import redis.mapper.RedisCommandDescription;
import redis.options.RedisLookupOptions;

/**
 * The RedisRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 */
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(
            RedisRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;
    protected final RedisLookupOptions redisLookupOptions;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean isBatchMode;
    private final int batchSize;
    private final int batchMinTriggerDelayMs;
    private String additionalKey;
    private LookupRedisMapper lookupRedisMapper;
    private RedisCommand redisCommand;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;
    private transient Cache<Object, RowData> cache;

    private transient Consumer<Object[]> evaler;

    public RedisRowDataLookupFunction(
            FlinkJedisConfigBase flinkJedisConfigBase
            , LookupRedisMapper lookupRedisMapper,
            RedisLookupOptions redisLookupOptions) {

        this.flinkJedisConfigBase = flinkJedisConfigBase;

        this.lookupRedisMapper = lookupRedisMapper;
        this.redisLookupOptions = redisLookupOptions;
        RedisCommandDescription redisCommandDescription = lookupRedisMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getRedisCommand();
        this.additionalKey = redisCommandDescription.getAdditionalKey();

        this.cacheMaxSize = this.redisLookupOptions.getCacheMaxSize();
        this.cacheExpireMs = this.redisLookupOptions.getCacheExpireMs();
        this.maxRetryTimes = this.redisLookupOptions.getMaxRetryTimes();

        this.isBatchMode = this.redisLookupOptions.isBatchMode();

        this.batchSize = this.redisLookupOptions.getBatchSize();

        this.batchMinTriggerDelayMs = this.redisLookupOptions.getBatchMinTriggerDelayMs();
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param redisKey the lookup key. Currently only support single rowkey.
     */
    public void eval(Object... redisKey) throws IOException {
        // 首先取缓存中数据
        if (this.cache != null) {
            RowData cacheRowData = (RowData) this.cache.getIfPresent(redisKey);
            if (cacheRowData != null) {
                this.collect(cacheRowData);
                return;
            }
        }

        // 读取 redis
        this.evaler = in -> {
            // fetch result
            byte[] key = lookupRedisMapper.serialize(in);

            byte[] value = null;

            switch (redisCommand) {
                case GET:
                    value = this.redisCommandsContainer.get(key); // 这里可以理解为 Jedis
                    break;
                case HGET:
                    value = this.redisCommandsContainer.hget(key, this.additionalKey.getBytes());
                    break;
                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + redisCommand);
            }

            RowData rowData = this.lookupRedisMapper.deserialize(value);

            collect(rowData);

            // 放入缓存
            if (null != rowData) {
                cache.put(key, rowData);
            }
        };

        // 收集
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                // fetch result
                this.evaler.accept(redisKey);
                break;
            } catch (Exception e) {
                LOG.error(String.format("Redis lookup error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of Redis lookup failed.", e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }


    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");

        try {
            //  构建有jedis连接池的容器对象
            this.redisCommandsContainer =
                    RedisCommandsContainerBuilder
                            .build(this.flinkJedisConfigBase);
            // 从容器中得到一个 jedis 对象
            this.redisCommandsContainer.open();

            // guava 缓存
            this.cache = this.cacheMaxSize > 0L && this.cacheExpireMs > 0L ? CacheBuilder.newBuilder().recordStats().expireAfterWrite(this.cacheExpireMs, TimeUnit.MILLISECONDS).maximumSize(this.cacheMaxSize).build() : null;
            if (this.cache != null) {
                context.getMetricGroup().gauge("lookupCacheHitRate", () -> {
                    return this.cache.stats().hitRate();
                });
            }
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw new RuntimeException(e);
        }

        LOG.info("end open.");
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (redisCommandsContainer != null) {
            try {
                redisCommandsContainer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("end close.");
    }
}
