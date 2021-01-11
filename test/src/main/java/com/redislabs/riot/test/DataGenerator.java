package com.redislabs.riot.test;

import io.lettuce.core.api.sync.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.redis.support.DataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Builder
public class DataGenerator implements Runnable {

    @NonNull
    private BaseRedisCommands<String, String> commands;
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

    @Override
    public void run() {
        for (int index = start; index < end; index++) {
            if (dataTypes.contains(DataType.STRING)) {
                String stringKey = "string:" + index;
                ((RedisStringCommands<String, String>) commands).set(stringKey, "value:" + index);
                if (expire > 0) {
                    ((RedisKeyCommands<String, String>) commands).expireat(stringKey, expire);
                }
            }
            Map<String, String> hash = new HashMap<>();
            hash.put("field1", "value" + index);
            hash.put("field2", "value" + index);
            if (dataTypes.contains(DataType.HASH)) {
                ((RedisHashCommands<String, String>) commands).hmset("hash:" + index, hash);
            }
            if (dataTypes.contains(DataType.SET)) {
                ((RedisSetCommands<String, String>) commands).sadd("set:" + (index % collectionModulo), "member:" + index);
            }
            if (dataTypes.contains(DataType.ZSET)) {
                ((RedisSortedSetCommands<String, String>) commands).zadd("zset:" + (index % collectionModulo), index % zsetScoreModulo, "member:" + index);
            }
            if (dataTypes.contains(DataType.STREAM)) {
                ((RedisStreamCommands<String, String>) commands).xadd("stream:" + (index % collectionModulo), hash);
            }
            if (dataTypes.contains(DataType.LIST)) {
                ((RedisListCommands<String,String>)commands).lpush("list:" + (index % collectionModulo), "member:" + index);
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
    }
}
