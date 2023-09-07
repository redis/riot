package com.redis.riot.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.expression.Expression;

import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.spring.batch.util.KeyComparisonItemReader;

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

    protected void execute(AbstractJobExecutable executable, TestInfo info) {
        executable.setName(name(info));
        executable.execute();
    }

    @Test
    void testMapProcessor(TestInfo info) throws JobExecutionException {
        MapProcessorOptions options = new MapProcessorOptions();
        Map<String, Expression> expressions = new LinkedHashMap<>();
        expressions.put("field1", SpelUtils.parse("'value1'"));
        expressions.put("field2", SpelUtils.parse("field1"));
        expressions.put("field3", SpelUtils.parse("1"));
        expressions.put("field4", SpelUtils.parse("2"));
        expressions.put("field5", SpelUtils.parse("field3+field4"));
        options.setExpressions(expressions);
        List<Map<String, Object>> list = new ArrayList<>();
        for (int index = 0; index < 10; index++) {
            list.add(new HashMap<>());
        }
        ListItemReader<Map<String, Object>> reader = new ListItemReader<>(list);
        ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor();
        ListItemWriter<Map<String, Object>> writer = new ListItemWriter<>();
        run(job(info).start(step(info, reader, writer).processor(processor).build()).build());
        assertEquals(10, writer.getWrittenItems().size());
        for (Map<String, Object> item : writer.getWrittenItems()) {
            assertEquals(5, item.size());
            assertEquals("value1", item.get("field1"));
            assertEquals("value1", item.get("field2"));
            assertEquals(3, item.get("field5"));
        }
    }

    @Test
    void testMapProcessorFilter(TestInfo info) throws JobExecutionException {
        MapProcessorOptions options = new MapProcessorOptions();
        options.setFilter(SpelUtils.parse("index<10"));
        List<Map<String, Object>> list = new ArrayList<>();
        for (int index = 0; index < 100; index++) {
            Map<String, Object> map = new HashMap<>();
            map.put("index", index);
            list.add(map);
        }
        ListItemReader<Map<String, Object>> reader = new ListItemReader<>(list);
        ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor();
        ListItemWriter<Map<String, Object>> writer = new ListItemWriter<>();
        run(job(info).start(step(info, reader, writer).processor(processor).build()).build());
        assertEquals(10, writer.getWrittenItems().size());
    }

    @Test
    void replicate(TestInfo info) throws Throwable {
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 0);
        Replication replicate = new Replication(client, targetClient, printWriter);
        replicate.execute();
        Assertions.assertTrue(compare(info));
    }

    @Test
    void keyProcessor(TestInfo info) throws Throwable {
        String key1 = "key1";
        String value1 = "value1";
        commands.set(key1, value1);
        Replication replication = new Replication(client, targetClient, printWriter);
        replication.setProcessorOptions(operatorOptions("#{type}:#{key}"));
        execute(replication, info);
        Assertions.assertEquals(value1, targetCommands.get("string:" + key1));
    }

    private KeyValueProcessorOptions operatorOptions(String keyExpression) {
        KeyValueProcessorOptions operatorOptions = new KeyValueProcessorOptions();
        operatorOptions.setKeyExpression(SpelUtils.parseTemplate(keyExpression));
        return operatorOptions;
    }

    @Test
    void keyProcessorWithDate(TestInfo info) throws Throwable {
        String key1 = "key1";
        String value1 = "value1";
        commands.set(key1, value1);
        Replication replication = new Replication(client, targetClient, printWriter);
        replication.setProcessorOptions(
                operatorOptions(String.format("#{#date.parse('%s').getTime()}:#{key}", "2010-05-10T00:00:00.000+0000")));
        execute(replication, info);
        Assertions.assertEquals(value1, targetCommands.get("1273449600000:" + key1));
    }

    protected KeyComparisonItemReader comparisonReader(TestInfo info) {
        KeyComparisonItemReader comparator = new KeyComparisonItemReader(structReader(info, client),
                structReader(info, targetClient));
        comparator.setTtlTolerance(Duration.ofMillis(100));
        return comparator;
    }

}
