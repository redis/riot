package com.redis.riot;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.ItemReaderBuilder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.OperationItemWriterBuilder;
import com.redis.spring.batch.support.ScanSizeEstimator;
import com.redis.spring.batch.support.ScanSizeEstimator.ScanSizeEstimatorBuilder;

import picocli.CommandLine;

public abstract class AbstractTransferCommand extends AbstractRiotCommand {

	@CommandLine.Mixin
	protected TransferOptions transferOptions = new TransferOptions();

	protected <I, O> RiotStepBuilder<I, O> riotStep(String name, String taskName) throws Exception {
		return new RiotStepBuilder<I, O>(getJobRunner().step(name), transferOptions).taskName(taskName);
	}

	protected ItemReaderBuilder reader(RedisOptions redisOptions) {
		if (redisOptions.isCluster()) {
			return RedisItemReader.client(redisOptions.clusterClient());
		}
		return RedisItemReader.client(redisOptions.client());
	}

	protected OperationItemWriterBuilder<String, String> writer(RedisOptions redisOptions) {
		if (redisOptions.isCluster()) {
			return RedisItemWriter.client(redisOptions.clusterClient());
		}
		return RedisItemWriter.client(redisOptions.client());
	}

	protected ScanSizeEstimatorBuilder estimator() {
		RedisOptions redisOptions = getRedisOptions();
		if (redisOptions.isCluster()) {
			return ScanSizeEstimator.client(redisOptions.clusterClient());
		}
		return ScanSizeEstimator.client(redisOptions.client());
	}

}
