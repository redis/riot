package com.redis.riot.redis;

import com.redis.riot.JobCommandContext;
import com.redis.spring.batch.DataStructure;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.Command;

@Command(name = "replicate-ds", description = "Replicate a source Redis DB to a target Redis DB using DUMP+RESTORE")
public class ReplicateDSCommand extends AbstractReplicateCommand<DataStructure<byte[]>> {

	@Override
	protected RedisItemWriter.Builder<byte[], byte[], DataStructure<byte[]>> writer(TargetCommandContext context) {
		return RedisItemWriter.dataStructure(context.getTargetRedisClient(), ByteArrayCodec.INSTANCE, Xadd.identity());
	}

	@Override
	protected RedisItemReader.Builder<byte[], byte[], DataStructure<byte[]>> reader(JobCommandContext context) {
		return RedisItemReader.dataStructure(context.getRedisClient(), ByteArrayCodec.INSTANCE);
	}

}
