package com.redis.riot.meesho;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import io.lettuce.core.codec.RedisCodec;
import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import java.util.function.Function;

public class MCacheProcessor<K, T extends KeyValue<K>> implements ItemProcessor<T, T> {
    private final Function<K, String> keyToString;
    private final Logger log;
    RedisCodec<K, ?> codec;

    private String mCacheId = "";

    private boolean isKeyPrefixNeeded = false;

    private byte[] keyPrefix = new byte[0];

    private boolean isByteOverHeadNeeded = false;

    private byte byteOverHead = 0;

    public MCacheProcessor(RedisCodec<K, ?> codec, Logger log, String mCacheId
            , boolean isKeyPrefixNeeded,
                           boolean isByteOverHeadNeeded, int byteOverHead) {
        this.codec = codec;
        this.keyToString = BatchUtils.toStringKeyFunction(codec);
        this.log = log;
        this.mCacheId = mCacheId;
        this.keyPrefix = KeyPrefixUtil.intTo2ByteArray(Integer.valueOf(mCacheId));
        this.byteOverHead = (byte) byteOverHead;
        this.isByteOverHeadNeeded = isByteOverHeadNeeded;
        this.isKeyPrefixNeeded = isKeyPrefixNeeded;
    }

//
//    public MCacheProcessor(RedisCodec<K, ?> codec, Logger log, String keyPrefix, boolean alreadyHasPrefix) {
//        this.keyPrefix = keyPrefix;
//        this.alreadyHasPrefix = alreadyHasPrefix;
//        this.codec = codec;
//        this.keyToString = BatchUtils.toStringKeyFunction(codec);
//        this.log = log;
//    }

    @Override
    public T process(T item) throws Exception {

        K key = item.getKey();
//        String modifiedKey = keyPrefix + string(key);
//        key = (K) modifiedKey.getBytes(StandardCharsets.UTF_8);
//        item.setKey(key);
//

        if (isKeyPrefixNeeded) {
            byte[] originalKey = (byte[]) key;
            byte[] newKey = new byte[originalKey.length + keyPrefix.length];
            System.arraycopy(keyPrefix, 0, newKey, 0, keyPrefix.length);
            System.arraycopy(originalKey, 0, newKey, keyPrefix.length, originalKey.length);
            key = (K) newKey;
            item.setKey(key);
        }


//        if (alreadyHasPrefix && "string".equals(item.getType()) && item.getValue() instanceof byte[]) {
//            byte[] originalValue = (byte[]) item.getValue();
//            log.info("First byte is : {}", originalValue[0]);
//            if (originalValue.length > 1) {
//                byte[] newValue = new byte[originalValue.length - 1];
//                System.arraycopy(originalValue, 1, newValue, 0, newValue.length);
//                item.setValue(newValue);
//
//                log.info("New value is : {}", string((K) item.getValue()));
//            } else {
//                throw new IllegalArgumentException("Array must contain more than one byte.");
//            }
//        }


        if (isByteOverHeadNeeded && "string".equals(item.getType()) && item.getValue() instanceof byte[] originalValue) {
            byte[] newValue = new byte[originalValue.length + 1];
            newValue[0] = byteOverHead;
            System.arraycopy(originalValue, 0, newValue, 1, originalValue.length);
            item.setValue(newValue);
        }

        return item;
    }

    private String string(K key) {
        return keyToString.apply(key);
    }
}
