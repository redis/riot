package com.redislabs.riot.test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
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
    private RedisClient client;
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
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisCommands<String, String> commands = connection.sync();
        for (int index = start; index < end; index++) {
            if (dataTypes.contains(DataType.STRING)) {
                String stringKey = "string:" + index;
                commands.set(stringKey, "value:" + index);
                if (expire > 0) {
                    commands.expireat(stringKey, expire);
                }
            }
            Map<String, String> hash = new HashMap<>();
            hash.put("field1", "value" + index);
            hash.put("field2", "value" + index);
            if (dataTypes.contains(DataType.HASH)) {
                commands.hmset("hash:" + index, hash);
            }
            if (dataTypes.contains(DataType.SET)) {
                commands.sadd("set:" + (index % collectionModulo), "member:" + index);
            }
            if (dataTypes.contains(DataType.ZSET)) {
                commands.zadd("zset:" + (index % collectionModulo), index % zsetScoreModulo, "member:" + index);
            }
            if (dataTypes.contains(DataType.STREAM)) {
                commands.xadd("stream:" + (index % collectionModulo), hash);
            }
            if (dataTypes.contains(DataType.LIST)) {
                commands.lpush("list:" + (index % collectionModulo), "member:" + index);
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
        connection.close();
    }
}
