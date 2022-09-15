package com.redis.riot.redis;

import com.redis.riot.JobCommandContext;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.reader.LiveReaderBuilder;
import com.redis.spring.batch.reader.ScanReaderBuilder;
import com.redis.spring.batch.writer.WriterBuilder;

import picocli.CommandLine.Command;

@Command(name = "replicate", description = "Replicate a source Redis DB to a target Redis DB using DUMP+RESTORE")
public class ReplicateCommand extends AbstractReplicateCommand<KeyDump<byte[]>> {

	@Override
	protected LiveReaderBuilder<byte[], byte[], KeyDump<byte[]>> liveReader(JobCommandContext context,
			String keyPattern) {
		return RedisItemReader.liveKeyDump(context.pool(CODEC), context.getJobRunner(), context.pubSubConnection(CODEC),
				CODEC, context.getRedisURI().getDatabase(), keyPattern);
	}

	@Override
	protected ScanReaderBuilder<byte[], byte[], KeyDump<byte[]>> scanReader(JobCommandContext context) {
		return RedisItemReader.keyDump(context.pool(CODEC), context.getJobRunner());
	}

	@Override
	protected WriterBuilder<byte[], byte[], KeyDump<byte[]>> writer(TargetCommandContext context) {
		return RedisItemWriter.keyDump(context.targetPool(CODEC));
	}

}
