package com.redis.riot.redis;

import com.redis.riot.JobCommandContext;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.LiveReaderBuilder;
import com.redis.spring.batch.reader.ScanReaderBuilder;
import com.redis.spring.batch.writer.WriterBuilder;
import com.redis.spring.batch.writer.operation.Xadd;

import picocli.CommandLine.Command;

@Command(name = "replicate-ds", description = "Replicate a source Redis DB to a target Redis DB using type-specific commands")
public class ReplicateDSCommand extends AbstractReplicateCommand<DataStructure<byte[]>> {

	@Override
	protected LiveReaderBuilder<byte[], byte[], DataStructure<byte[]>> liveReader(JobCommandContext context,
			String keyPattern) {
		return RedisItemReader.liveDataStructure(context.pool(CODEC), context.getJobRunner(),
				context.pubSubConnection(CODEC), CODEC, context.getRedisURI().getDatabase(), keyPattern);
	}

	@Override
	protected ScanReaderBuilder<byte[], byte[], DataStructure<byte[]>> scanReader(JobCommandContext context) {
		return RedisItemReader.dataStructure(context.pool(CODEC), context.getJobRunner());
	}

	@Override
	protected WriterBuilder<byte[], byte[], DataStructure<byte[]>> writer(TargetCommandContext context) {
		return RedisItemWriter.dataStructure(context.targetPool(CODEC), Xadd.identity());
	}

}
