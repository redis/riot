package com.redis.riot.core;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.unit.DataSize;

import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.util.CodecUtils;

import io.lettuce.core.codec.RedisCodec;

public class BlockedKeyItemWriter<K, T extends KeyValue<K>> implements ItemWriter<T> {

	private final Set<String> blockedKeys;

	private final Function<K, String> toStringKeyFunction;

	private final Predicate<T> predicate;

	public BlockedKeyItemWriter(RedisCodec<K, ?> codec, DataSize memoryUsageLimit, Set<String> blockedKeys) {
		this.toStringKeyFunction = CodecUtils.toStringKeyFunction(codec);
		this.predicate = new BlockedKeyPredicate<>(memoryUsageLimit);
		this.blockedKeys = blockedKeys;
	}

	@Override
	public void write(Chunk<? extends T> items) throws Exception {
		StreamSupport.stream(items.spliterator(), false).filter(predicate).map(KeyValue::getKey)
				.map(toStringKeyFunction).forEach(blockedKeys::add);
	}

	private static class BlockedKeyPredicate<K, T extends KeyValue<K>> implements Predicate<T> {

		private final long memLimit;

		public BlockedKeyPredicate(DataSize memoryUsageLimit) {
			this.memLimit = memoryUsageLimit.toBytes();
		}

		@Override
		public boolean test(T t) {
			if (t == null) {
				return false;
			}
			return t.getMemoryUsage() > memLimit;
		}

	}

}
