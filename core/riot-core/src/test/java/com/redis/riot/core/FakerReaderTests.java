package com.redis.riot.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;

import com.redis.riot.core.faker.FakerItemReader;
import com.redis.spring.batch.util.BatchUtils;

class FakerReaderTests {

    @SuppressWarnings("deprecation")
    @Test
    void fakerReader() throws Exception {
        int count = 100;
        FakerItemReader reader = new FakerItemReader();
        Map<String, String> fields = new LinkedHashMap<String, String>();
        fields.put("index", "index");
        fields.put("firstName", "name.firstName");
        fields.put("lastName", "name.lastName");
        fields.put("thread", "thread.id");
        reader.setStringFields(fields);
        reader.setMaxItemCount(count);
        reader.open(new ExecutionContext());
        List<Map<String, Object>> items = BatchUtils.readAll(reader);
        reader.close();
        Assertions.assertEquals(count, items.size());
        Assertions.assertEquals(1, items.get(0).get("index"));
        Assertions.assertEquals(Thread.currentThread().getId(), ((Long) items.get(0).get("thread")).longValue());
    }

}
