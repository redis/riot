package com.redis.riot;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.Builder;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.RedisItemWriter.OperationBuilder;
import com.redis.spring.batch.RedisScanSizeEstimator;
import com.redis.spring.batch.RedisScanSizeEstimator.ScanSizeEstimatorBuilder;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine;

public abstract class AbstractTransferCommand extends AbstractRiotCommand {

	@CommandLine.Mixin
	protected TransferOptions transferOptions = new TransferOptions();

	protected <I, O> RiotStepBuilder<I, O> riotStep(String name, String taskName) throws Exception {
		return new RiotStepBuilder<I, O>(getJobRunner().step(name), transferOptions).taskName(taskName);
	}

	protected Builder<String, String> stringReader(RedisOptions redisOptions) {
		return reader(redisOptions, StringCodec.UTF8);
	}

	protected <K, V> Builder<K, V> reader(RedisOptions redisOptions, RedisCodec<K, V> codec) {
		if (redisOptions.isCluster()) {
			return RedisItemReader.client(redisOptions.redisModulesClusterClient(), codec);
		}
		return RedisItemReader.client(redisOptions.redisModulesClient(), codec);
	}

	protected <K, V> OperationBuilder<K, V> writer(RedisOptions redisOptions, RedisCodec<K, V> codec) {
		if (redisOptions.isCluster()) {
			return RedisItemWriter.client(redisOptions.redisModulesClusterClient(), codec);
		}
		return RedisItemWriter.client(redisOptions.redisModulesClient(), codec);
	}

	protected ScanSizeEstimatorBuilder estimator() {
		RedisOptions redisOptions = getRedisOptions();
		if (redisOptions.isCluster()) {
			return RedisScanSizeEstimator.client(redisOptions.redisModulesClusterClient());
		}
		return RedisScanSizeEstimator.client(redisOptions.redisModulesClient());
	}

}
