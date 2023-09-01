package com.redis.riot.core.replicate;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.springframework.batch.core.ItemWriteListener;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.ByteArrayCodec;

public class KeyValueWriteListener implements ItemWriteListener<KeyValue<byte[]>> {

    private final Logger log;

    private final Function<byte[], String> toStringKeyFunction = CodecUtils.toStringKeyFunction(ByteArrayCodec.INSTANCE);

    public KeyValueWriteListener(Logger log) {
        this.log = log;
    }

    @Override
    public void beforeWrite(List<? extends KeyValue<byte[]>> items) {
        log.debug("Writing keys {}", keys(items));
    }

    private List<String> keys(List<? extends KeyValue<byte[]>> items) {
        return items.stream().map(this::toString).collect(Collectors.toList());
    }

    @Override
    public void afterWrite(List<? extends KeyValue<byte[]>> items) {
        // do nothing
    }

    @Override
    public void onWriteError(Exception exception, List<? extends KeyValue<byte[]>> items) {
        // do nothing
    }

    private String toString(KeyValue<byte[]> keyValue) {
        String key = toStringKeyFunction.apply(keyValue.getKey());
        if (KeyValue.isString(keyValue)) {
            byte[] value = keyValue.getValue();
            if (value.length > 1000) {
                return key + " (" + value.length + ")";
            }
            return "";
        }
        return key;
    }

}
