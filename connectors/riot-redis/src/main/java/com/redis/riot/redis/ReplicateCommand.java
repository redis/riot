package com.redis.riot.redis;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a source Redis DB to a target Redis DB using DUMP+RESTORE")
public class ReplicateCommand extends AbstractReplicateCommand<KeyValue<byte[], byte[]>> {

	@Override
	protected RedisItemWriter.Builder<byte[], byte[], KeyValue<byte[], byte[]>> writer(TargetCommandContext context) {
		return RedisItemWriter.keyDump(context.getTargetRedisClient(), ByteArrayCodec.INSTANCE);
	}

	@Override
	protected RedisItemReader.Builder<byte[], byte[], KeyValue<byte[], byte[]>> reader(TargetCommandContext context) {
		return RedisItemReader.keyDump(context.getRedisClient(), ByteArrayCodec.INSTANCE);
	}

}
