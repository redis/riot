package com.redis.riot;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.OperationBuilder;
import com.redis.spring.batch.RedisScanSizeEstimator;
import com.redis.spring.batch.RedisScanSizeEstimator.ScanSizeEstimatorBuilder;

import picocli.CommandLine;

public abstract class AbstractTransferCommand extends AbstractRiotCommand {

	@CommandLine.Mixin
	protected TransferOptions transferOptions = new TransferOptions();

	protected <I, O> RiotStepBuilder<I, O> riotStep(String name, String taskName) throws Exception {
		return new RiotStepBuilder<I, O>(getJobRunner().step(name), transferOptions).taskName(taskName);
	}

	protected Builder reader(RedisOptions redisOptions) {
		if (redisOptions.isCluster()) {
			return RedisItemReader.client(redisOptions.redisModulesClusterClient());
		}
		return RedisItemReader.client(redisOptions.redisModulesClient());
	}

	protected OperationBuilder<String, String> writer(RedisOptions redisOptions) {
		if (redisOptions.isCluster()) {
			return RedisItemWriter.client(redisOptions.redisModulesClusterClient());
		}
		return RedisItemWriter.client(redisOptions.redisModulesClient());
	}

	protected ScanSizeEstimatorBuilder estimator() {
		RedisOptions redisOptions = getRedisOptions();
		if (redisOptions.isCluster()) {
			return RedisScanSizeEstimator.client(redisOptions.redisModulesClusterClient());
		}
		return RedisScanSizeEstimator.client(redisOptions.redisModulesClient());
	}

}
