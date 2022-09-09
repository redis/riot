package com.redis.riot.redis;

import com.redis.riot.JobCommandContext;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.LiveReaderOptions;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ScanReaderOptions;
import com.redis.spring.batch.writer.WriterOptions;
import com.redis.spring.batch.writer.operation.Xadd;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.Command;

@Command(name = "replicate-ds", description = "Replicate a source Redis DB to a target Redis DB using type-specific commands")
public class ReplicateDSCommand extends AbstractReplicateCommand<DataStructure<byte[]>> {

	@Override
	protected LiveRedisItemReader<byte[], DataStructure<byte[]>> liveReader(JobCommandContext context,
			String keyPattern, LiveReaderOptions options) {
		return RedisItemReader.liveDataStructure(context.pool(ByteArrayCodec.INSTANCE), context.getJobRunner(),
				context.pubSubConnection(ByteArrayCodec.INSTANCE), ByteArrayCodec.INSTANCE,
				context.getRedisURI().getDatabase(), keyPattern).options(options).build();
	}

	@Override
	protected RedisItemReader<byte[], DataStructure<byte[]>> scanReader(JobCommandContext context,
			ScanReaderOptions options) {
		return RedisItemReader.dataStructure(context.pool(ByteArrayCodec.INSTANCE), context.getJobRunner())
				.options(options).build();
	}

	@Override
	protected RedisItemWriter<byte[], byte[], DataStructure<byte[]>> writer(TargetCommandContext context,
			WriterOptions options) {
		return RedisItemWriter.dataStructure(context.targetPool(ByteArrayCodec.INSTANCE), Xadd.identity())
				.options(options).build();
	}

}
