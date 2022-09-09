package com.redis.riot.redis;

import com.redis.riot.JobCommandContext;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.reader.LiveReaderOptions;
import com.redis.spring.batch.reader.LiveRedisItemReader;
import com.redis.spring.batch.reader.ScanReaderOptions;
import com.redis.spring.batch.writer.WriterOptions;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a source Redis DB to a target Redis DB using DUMP+RESTORE")
public class ReplicateCommand extends AbstractReplicateCommand<KeyDump<byte[]>> {

	@Override
	protected LiveRedisItemReader<byte[], KeyDump<byte[]>> liveReader(JobCommandContext context, String keyPattern,
			LiveReaderOptions options) {
		return RedisItemReader.liveKeyDump(context.pool(ByteArrayCodec.INSTANCE), context.getJobRunner(),
				context.pubSubConnection(ByteArrayCodec.INSTANCE), ByteArrayCodec.INSTANCE,
				context.getRedisURI().getDatabase(), keyPattern).options(options).build();
	}

	@Override
	protected RedisItemReader<byte[], KeyDump<byte[]>> scanReader(JobCommandContext context,
			ScanReaderOptions options) {
		return RedisItemReader.keyDump(context.pool(ByteArrayCodec.INSTANCE), context.getJobRunner()).options(options)
				.build();
	}

	@Override
	protected RedisItemWriter<byte[], byte[], KeyDump<byte[]>> writer(TargetCommandContext context,
			WriterOptions options) {
		return RedisItemWriter.keyDump(context.targetPool(ByteArrayCodec.INSTANCE)).options(options).build();
	}

}
