package com.redislabs.riot;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.redis.support.DataType;

import java.util.*;

@Slf4j
@Builder
public class DataGenerator implements Runnable {

    @NonNull
    private BaseRedisAsyncCommands<String, String> commands;
    @Builder.Default
    private int batchSize = 50;
    @Builder.Default
    private int start = 0;
    @Builder.Default
    private int end = 1000;
    private long sleep;
    private long expire;
    @Builder.Default
    private int collectionModulo = 10;
    @Builder.Default
    private int zsetScoreModulo = 3;
    @NonNull
    @Builder.Default
    private List<DataType> dataTypes = Arrays.asList(DataType.values());
    private final List<RedisFuture<?>> futures = new ArrayList<>();

    @Override
    public void run() {
        for (int index = start; index < end; index++) {
            commands.setAutoFlushCommands(false);
            execute(index);
            if (futures.size() > batchSize) {
                flush();
            }
            if (sleep > 0) {
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    log.debug("Interrupted");
                    return;
                }
            }
        }
        flush();
    }

    private void flush() {
        try {
            commands.flushCommands();
            LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
        } finally {
            futures.clear();
            commands.setAutoFlushCommands(true);
        }
    }

    private void execute(int index) {
        if (dataTypes.contains(DataType.STRING)) {
            String stringKey = "string:" + index;
            futures.add(((RedisStringAsyncCommands<String, String>) commands).set(stringKey, "value:" + index));
            if (expire > 0) {
                futures.add(((RedisKeyAsyncCommands<String, String>) commands).expireat(stringKey, expire));
            }
        }
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value" + index);
        hash.put("field2", "value" + index);
        if (dataTypes.contains(DataType.HASH)) {
            futures.add(((RedisHashAsyncCommands<String, String>) commands).hset("hash:" + index, hash));
        }
        if (dataTypes.contains(DataType.SET)) {
            futures.add(((RedisSetAsyncCommands<String, String>) commands).sadd("set:" + (index % collectionModulo), "member:" + index));
        }
        if (dataTypes.contains(DataType.ZSET)) {
            futures.add(((RedisSortedSetAsyncCommands<String, String>) commands).zadd("zset:" + (index % collectionModulo), index % zsetScoreModulo, "member:" + index));
        }
        if (dataTypes.contains(DataType.STREAM)) {
            futures.add(((RedisStreamAsyncCommands<String, String>) commands).xadd("stream:" + (index % collectionModulo), hash));
        }
        if (dataTypes.contains(DataType.LIST)) {
            futures.add(((RedisListAsyncCommands<String, String>) commands).lpush("list:" + (index % collectionModulo), "member:" + index));
        }
    }
}
