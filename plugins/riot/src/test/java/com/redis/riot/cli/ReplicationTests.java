package com.redis.riot.cli;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyComparison;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.cluster.SlotHash;

abstract class ReplicationTests extends RiotTests {

    @Test
    void replicate(TestInfo info) throws Throwable {
        String filename = "replicate";
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 0);
        execute(filename);
    }

    @Test
    void replicateDryRun(TestInfo info) throws Throwable {
        String filename = "replicate-dry-run";
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 0);
        execute(filename);
        Assertions.assertEquals(0, targetCommands.dbsize());
    }

    @Test
    void replicateHLL(TestInfo info) throws Throwable {
        String key = "crawled:20171124";
        String value = "http://www.google.com/";
        commands.pfadd(key, value);
        Assertions.assertEquals(0, execute("replicate-hll"));
        Assertions.assertTrue(compare(info).isEmpty());
    }

    @Test
    void replicateKeyProcessor(TestInfo info) throws Throwable {
        String filename = "replicate-key-processor";
        GeneratorItemReader gen = generator(1, DataType.HASH);
        generate(info, gen);
        Long sourceSize = commands.dbsize();
        Assertions.assertTrue(sourceSize > 0);
        execute(filename);
        Assertions.assertEquals(sourceSize, targetCommands.dbsize());
        Assertions.assertEquals(commands.hgetall("gen:1"), targetCommands.hgetall("0:gen:1"));
    }

    @Test
    void replicateKeyExclude(TestInfo info) throws Throwable {
        String filename = "replicate-key-exclude";
        int goodCount = 200;
        GeneratorItemReader gen = generator(goodCount, DataType.HASH);
        generate(info, gen);
        int badCount = 100;
        GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
        generator2.setKeyspace("bad");
        generate(testInfo(info, "2"), generator2);
        Assertions.assertEquals(badCount, commands.keys("bad:*").size());
        execute(filename);
        Assertions.assertEquals(goodCount, targetCommands.keys("gen:*").size());
    }

    @Test
    void replicateLiveKeyExclude(TestInfo info) throws Throwable {
        int goodCount = 200;
        int badCount = 100;
        String filename = "replicate-live-key-exclude";
        enableKeyspaceNotifications(client);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(() -> {
            GeneratorItemReader generator = generator(goodCount, DataType.HASH);
            GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
            generator2.setKeyspace("bad");
            try {
                generate(testInfo(info, "gen1"), generator);
                generate(testInfo(info, "gen2"), generator2);
            } catch (Exception e) {
                log.error("Could not generate data", e);
            }
        }, 500, TimeUnit.MILLISECONDS);
        execute(filename);
        Assertions.assertEquals(badCount, commands.keys("bad:*").size());
        Assertions.assertEquals(goodCount, targetCommands.keys("gen:*").size());
    }

    @Test
    void replicateLive(TestInfo info) throws Exception {
        runLiveReplication(info, "replicate-live");
        List<KeyComparison> diffs = compare(info);
        logDiffs(diffs);
        Assertions.assertTrue(diffs.isEmpty());
    }

    @Test
    void replicateLiveMultiThreaded(TestInfo info) throws Exception {
        runLiveReplication(info, "replicate-live-threads");
        Assertions.assertTrue(compare(info).isEmpty());
    }

    @Test
    void replicateLiveStruct(TestInfo info) throws Exception {
        runLiveReplication(info, "replicate-live-struct");
        List<KeyComparison> diffs = compare(info);
        logDiffs(diffs);
        Assertions.assertTrue(diffs.isEmpty());
    }

    @Test
    void replicateLiveKeySlot(TestInfo info) throws Exception {
        String filename = "replicate-live-keyslot";
        enableKeyspaceNotifications(client);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        GeneratorItemReader generator = generator(300);
        executor.schedule(() -> {
            TestInfo genInfo = testInfo(info, filename, "gen-live");
            try {
                run(genInfo, step(genInfo, 1, generator, null, RedisItemWriter.struct(client)));
            } catch (Exception e) {
                log.error("Could not generate data", e);
            }
        }, 500, TimeUnit.MILLISECONDS);
        execute(filename);
        List<String> keys = targetCommands.keys("*");
        for (String key : keys) {
            int slot = SlotHash.getSlot(key);
            Assertions.assertTrue(slot >= 0 && slot <= 8000);
        }
    }

    @Test
    void replicateStruct(TestInfo info) throws Throwable {
        String filename = "replicate-struct";
        GeneratorItemReader generator = generator(12000);
        generate(testInfo(info, "gen"), generator);
        Assertions.assertTrue(commands.dbsize() > 0);
        execute(filename);
    }

    protected void runLiveReplication(TestInfo info, String filename) throws Exception {
        enableKeyspaceNotifications(client);
        GeneratorItemReader gen = generator(3000, DataType.HASH, DataType.STRING, DataType.LIST, DataType.ZSET);
        generate(testInfo(info, "liveReplicationGen"), gen);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(() -> {
            GeneratorItemReader generator = generator(3500, DataType.HASH, DataType.STRING, DataType.LIST, DataType.ZSET);
            generator.setCurrentItemCount(3000);
            StructItemWriter<String, String> writer = RedisItemWriter.struct(client);
            TestInfo stepInfo = testInfo(info, filename, "gen-live");
            SimpleStepBuilder<KeyValue<String>, KeyValue<String>> step = step(stepInfo, 1, generator, null, writer);
            try {
                run(testInfo(info, filename, "gen-live"), step);
            } catch (Exception e) {
                log.error("Could not generate data", e);
            }
        }, 500, TimeUnit.MILLISECONDS);
        execute(filename);
    }

}
