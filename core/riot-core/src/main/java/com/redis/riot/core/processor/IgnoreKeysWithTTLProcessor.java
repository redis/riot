package com.redis.riot.core.processor;

import com.redis.spring.batch.common.KeyValue;
import org.springframework.batch.item.ItemProcessor;

public class IgnoreKeysWithTTLProcessor implements ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> {
    @Override
    public KeyValue<byte[]> process( KeyValue<byte[]> item) throws Exception {
        if ( item.getTtl() > 0L ) {
            return null;
        }
        return item;
    }
}
