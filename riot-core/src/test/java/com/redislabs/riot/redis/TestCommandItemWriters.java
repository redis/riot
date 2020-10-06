package com.redislabs.riot.redis;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.util.Assert;

import io.lettuce.core.api.StatefulRedisConnection;

public class TestCommandItemWriters {
	

//
//    @Test
//    public void testStringItemWriter() throws Exception {
//        run("string-item-writer", beerReader(), CommandItemWriters.Set.<Map<String, String>>builder().redisURI(sourceRedisURI).keyConverter(m -> m.get(Beers.FIELD_ID)).valueConverter(m -> m.get(Beers.FIELD_NAME)).build());
//        assertSize(sourceRedisClient);
//        StatefulRedisConnection<String, String> sourceConnection = sourceRedisClient.connect();
//        Assertions.assertEquals("Redband Stout", sourceConnection.sync().get("371"));
//        sourceConnection.close();
//    }
//
//    @Test
//    public void testSetItemWriter() throws Exception {
//        run("set-item-writer", beerReader(), CommandItemWriters.Sadd.<Map<String, String>>builder().redisURI(sourceRedisURI).keyConverter(m -> "beers").memberIdConverter(m -> m.get(Beers.FIELD_ID)).build());
//        StatefulRedisConnection<String, String> sourceConnection = sourceRedisClient.connect();
//        Assert.assertEquals(Beers.SIZE, (long) sourceConnection.sync().scard("beers"));
//        sourceConnection.close();
//    }
//
//    @Test
//    public void testStreamItemWriter() throws Exception {
//        CommandItemWriters.Xadd<String, String, Map<String, String>> writer = CommandItemWriters.Xadd.<Map<String, String>>builder().redisURI(sourceRedisURI).keyConverter(m -> "beers").bodyConverter(m -> m).build();
//        run("stream-item-writer", beerReader(), writer);
//        StatefulRedisConnection<String, String> sourceConnection = sourceRedisClient.connect();
//        Assert.assertEquals(Beers.SIZE, (long) sourceConnection.sync().xlen("beers"));
//        sourceConnection.close();
//    }
//
//    private ItemReader<Map<String, String>> beerReader() throws IOException {
//        return new ListItemReader<>(Beers.load());
//    }
//
//    @Test
//    public void testRedisWriter() throws Exception {
//        redisWriter("redis-writer");
//        assertSize(sourceRedisClient);
//    }

}
