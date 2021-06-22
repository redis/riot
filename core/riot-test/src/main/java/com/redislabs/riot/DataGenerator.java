package com.redislabs.riot;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.*;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DataGenerator implements Callable<Long> {

    private final StatefulConnection<String, String> connection;
    private final int start;
    private final int end;
    private final long sleep;
    private final int maxExpire;
    private final int batchSize;
    private final Set<DataType> dataTypes;

    public DataGenerator(StatefulConnection<String, String> connection, int start, int end, long sleep, int maxExpire, int batchSize, Set<DataType> dataTypes) {
        this.connection = connection;
        this.start = start;
        this.end = end;
        this.sleep = sleep;
        this.maxExpire = maxExpire;
        this.batchSize = batchSize;
        this.dataTypes = dataTypes;
    }

    public static DataGeneratorBuilder connection(StatefulConnection<String,String> connection) {
        return new DataGeneratorBuilder(connection);
    }

    public static class DataGeneratorBuilder {

        private static final int DEFAULT_START = 0;
        private static final int DEFAULT_END = 1000;
        private static final long DEFAULT_SLEEP = 0;
        private static final int DEFAULT_BATCH_SIZE = 50;
        private static final int DEFAULT_MAX_EXPIRE = 100000;

        private final StatefulConnection<String, String> connection;
        private int start = DEFAULT_START;
        private int end = DEFAULT_END;
        private long sleep = DEFAULT_SLEEP;
        private int maxExpire = DEFAULT_MAX_EXPIRE;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private Set<DataType> dataTypes = new HashSet<>(Arrays.asList(DataType.values()));

        public DataGeneratorBuilder(StatefulConnection<String, String> connection) {
            this.connection = connection;
        }

        public DataGenerator build() {
            return new DataGenerator(connection, start, end, sleep, maxExpire, batchSize, dataTypes);
        }

        public DataGeneratorBuilder dataTypes(DataType... dataTypes) {
            this.dataTypes = new HashSet<>(Arrays.asList(dataTypes));
            return this;
        }

        public DataGeneratorBuilder start(int start) {
            this.start = start;
            return this;
        }

        public DataGeneratorBuilder end(int end) {
            this.end = end;
            return this;
        }

        public DataGeneratorBuilder sleep(long sleep) {
            this.sleep = sleep;
            return this;
        }

    }

    @Override
    public Long call() throws Exception {
        BaseRedisAsyncCommands<String, String> commands = async();
        commands.setAutoFlushCommands(false);
        long count = 0;
        CommandExecutor executor = new CommandExecutor(commands);
        try {
            count += executor.call();
        } finally {
            commands.setAutoFlushCommands(true);
        }
        return count;
    }

    private BaseRedisAsyncCommands<String, String> async() {
        if (connection instanceof StatefulRedisClusterConnection) {
            return ((StatefulRedisClusterConnection<String, String>) connection).async();
        }
        return ((StatefulRedisConnection<String, String>) connection).async();
    }


    @SuppressWarnings("unchecked")
    private class CommandExecutor implements Callable<Long> {

        private final Random random = new Random();
        private final BaseRedisAsyncCommands<String, String> commands;
        private final List<RedisFuture<?>> futures = new ArrayList<>();

        public CommandExecutor(BaseRedisAsyncCommands<String, String> commands) {
            this.commands = commands;
        }

        @Override
        public Long call() throws InterruptedException {
            long count = 0;
            for (int index = start; index < end; index++) {
                if (contains(DataType.STRING)) {
                    String stringKey = "string:" + index;
                    futures.add(((RedisStringAsyncCommands<String, String>) commands).set(stringKey, "value:" + index));
                    if (maxExpire > 0) {
                        futures.add(((RedisKeyAsyncCommands<String, String>) commands).expireat(stringKey, System.currentTimeMillis() + random.nextInt(maxExpire)));
                    }
                }
                Map<String, String> hash = new HashMap<>();
                hash.put("field1", "value" + index);
                hash.put("field2", "value" + index);
                String member = "member:" + index;
                int collectionIndex = index % 10;
                if (contains(DataType.HASH)) {
                    futures.add(((RedisHashAsyncCommands<String, String>) commands).hset("hash:" + index, hash));
                }
                if (contains(DataType.SET)) {
                    futures.add(((RedisSetAsyncCommands<String, String>) commands).sadd("set:" + collectionIndex, member));
                }
                if (contains(DataType.ZSET)) {
                    futures.add(((RedisSortedSetAsyncCommands<String, String>) commands).zadd("zset:" + collectionIndex, index % 3, member));
                }
                if (contains(DataType.STREAM)) {
                    futures.add(((RedisStreamAsyncCommands<String, String>) commands).xadd("stream:" + collectionIndex, hash));
                }
                if (contains(DataType.LIST)) {
                    futures.add(((RedisListAsyncCommands<String, String>) commands).lpush("list:" + collectionIndex, member));
                }
                if (futures.size() >= batchSize) {
                    count += flush();
                }
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
            count += flush();
            return count;
        }

        private int flush() {
            commands.flushCommands();
            LettuceFutures.awaitAll(60, TimeUnit.SECONDS, futures.toArray(new RedisFuture[0]));
            try {
                return futures.size();
            } finally {
                futures.clear();
            }
        }
    }

    private boolean contains(DataType type) {
        return dataTypes.contains(type);
    }

}
