package com.redis.riot.core.test;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.support.ListItemWriter;

import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.PredicateItemProcessor;
import com.redis.riot.core.RedisOptions;
import com.redis.riot.core.RedisUriOptions;
import com.redis.riot.core.Replication;
import com.redis.riot.core.ReplicationType;
import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ReaderMode;
import com.redis.spring.batch.common.KeyComparisonItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.reader.StructItemReader;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.cluster.SlotHash;

public abstract class ReplicationTests extends AbstractTargetTestBase {

    public static final String BEERS_JSON_URL = "https://storage.googleapis.com/jrx/beers.json";

    public static final int BEER_CSV_COUNT = 2410;

    public static final int BEER_JSON_COUNT = 216;

    protected static String name(Map<String, String> beer) {
        return beer.get("name");
    }

    protected static String style(Map<String, String> beer) {
        return beer.get("style");
    }

    protected static double abv(Map<String, String> beer) {
        return Double.parseDouble(beer.get("abv"));
    }

    private PrintWriter printWriter = new PrintWriter(System.out);

    protected void execute(Replication replication, TestInfo info) {
        replication.setName(name(info));
        replication.setRedisOptions(redisOptions(getRedisServer()));
        replication.setTargetRedisOptions(redisOptions(getTargetRedisServer()));
        replication.run();
    }

    private RedisOptions redisOptions(RedisServer redis) {
        RedisOptions options = new RedisOptions();
        RedisUriOptions uriOptions = new RedisUriOptions();
        uriOptions.setUri(redis.getRedisURI());
        options.setUriOptions(uriOptions);
        options.setCluster(redis.isCluster());
        return options;
    }

    @Test
    void replicate(TestInfo info) throws Throwable {
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 0);
        Replication replication = new Replication(printWriter);
        execute(replication, info);
        Assertions.assertTrue(compare(info).isEmpty());
    }

    @Test
    void keyProcessor(TestInfo info) throws Throwable {
        String key1 = "key1";
        String value1 = "value1";
        commands.set(key1, value1);
        Replication replication = new Replication(printWriter);
        replication.setType(ReplicationType.STRUCT);
        replication.setProcessorOptions(processorOptions("#{type.getString()}:#{key}"));
        execute(replication, info);
        Assertions.assertEquals(value1, targetCommands.get("string:" + key1));
    }

    private KeyValueProcessorOptions processorOptions(String keyExpression) {
        KeyValueProcessorOptions options = new KeyValueProcessorOptions();
        options.setKeyExpression(RiotUtils.parseTemplate(keyExpression));
        return options;
    }

    @Test
    void keyProcessorWithDate(TestInfo info) throws Throwable {
        String key1 = "key1";
        String value1 = "value1";
        commands.set(key1, value1);
        Replication replication = new Replication(printWriter);
        replication.setProcessorOptions(
                processorOptions(String.format("#{#date.parse('%s').getTime()}:#{key}", "2010-05-10T00:00:00.000+0000")));
        execute(replication, info);
        Assertions.assertEquals(value1, targetCommands.get("1273449600000:" + key1));
    }

    protected KeyComparisonItemReader comparisonReader(TestInfo info) {
        StructItemReader<String, String> sourceReader = RedisItemReader.struct(client);
        configureReader(testInfo(info, "source"), sourceReader);
        StructItemReader<String, String> targetReader = RedisItemReader.struct(targetClient);
        configureReader(testInfo(info, "target"), targetReader);
        KeyComparisonItemReader comparator = new KeyComparisonItemReader(sourceReader, targetReader);
        comparator.setTtlTolerance(Duration.ofMillis(100));
        return comparator;
    }

    @Test
    void filterKeySlot(TestInfo info) throws Exception {
        enableKeyspaceNotifications(client);
        RedisItemReader<String, String, KeyValue<String>> reader = RedisItemReader.struct(client);
        configureReader(info, reader);
        reader.setMode(ReaderMode.LIVE);
        Range range = Range.to(8000);
        reader.setKeyProcessor(new PredicateItemProcessor<>(k -> range.contains(SlotHash.getSlot(k))));
        ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
        FlushingStepBuilder<KeyValue<String>, KeyValue<String>> step = flushingStep(info, reader, writer);
        Executors.newSingleThreadScheduledExecutor().execute(() -> {
            awaitUntil(reader::isOpen);
            int count = 100;
            GeneratorItemReader gen = generator(count);
            try {
                generate(info, gen);
            } catch (JobExecutionException e) {
                throw new RuntimeException("Could not execute data gen", e);
            }
        });
        run(job(info).start(step.build()).build());
        awaitUntilFalse(reader::isOpen);
        Assertions.assertFalse(writer.getWrittenItems().stream().map(KeyValue::getKey).map(SlotHash::getSlot)
                .anyMatch(s -> s < 0 || s > 8000));
    }

}
