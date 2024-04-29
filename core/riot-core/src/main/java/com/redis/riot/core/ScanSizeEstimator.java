package com.redis.riot.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.util.StringUtils;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.OperationExecutor;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;

public class ScanSizeEstimator implements LongSupplier {

	public static final long UNKNOWN_SIZE = -1;
	public static final int DEFAULT_SAMPLES = 100;

	private final AbstractRedisClient client;

	private int samples = DEFAULT_SAMPLES;
	private String keyPattern;
	private String keyType;

	public ScanSizeEstimator(AbstractRedisClient client) {
		this.client = client;
	}

	public String getKeyPattern() {
		return keyPattern;
	}

	public void setKeyPattern(String keyPattern) {
		this.keyPattern = keyPattern;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public int getSamples() {
		return samples;
	}

	public void setSamples(int samples) {
		this.samples = samples;
	}

	/**
	 * Estimates the number of keys that match the given pattern and type.
	 * 
	 * @return Estimated number of keys matching the given pattern and type. Returns
	 *         null if database is empty or any error occurs
	 * @throws IOException if script execution exception happens during estimation
	 */
	@Override
	public long getAsLong() {
		try (StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			Long dbsize = connection.sync().dbsize();
			if (dbsize == null) {
				return UNKNOWN_SIZE;
			}
			if (!StringUtils.hasLength(keyPattern) && !StringUtils.hasLength(keyType)) {
				return dbsize;
			}
			RedisModulesAsyncCommands<String, String> commands = connection.async();
			try {
				connection.setAutoFlushCommands(false);
				List<RedisFuture<String>> keyFutures = new ArrayList<>();
				for (int index = 0; index < samples; index++) {
					keyFutures.add(commands.randomkey());
				}
				connection.flushCommands();
				List<String> keys = OperationExecutor.getAll(connection.getTimeout(), keyFutures);
				List<RedisFuture<String>> typeFutures = keys.stream().map(commands::type).collect(Collectors.toList());
				connection.flushCommands();
				List<String> types = OperationExecutor.getAll(connection.getTimeout(), typeFutures);
				Predicate<String> matchPredicate = matchPredicate();
				Predicate<String> typePredicate = typePredicate();
				int total = 0;
				int matchCount = 0;
				Iterator<String> keyIterator = keys.iterator();
				Iterator<String> typeIterator = types.iterator();
				while (keyIterator.hasNext()) {
					String key = keyIterator.next();
					if (!typeIterator.hasNext()) {
						throw new IllegalStateException("Could not find type for key " + key);
					}
					String type = typeIterator.next();
					total++;
					if (matchPredicate.test(key) && typePredicate.test(type)) {
						matchCount++;
					}
				}
				double matchRate = total == 0 ? 0 : (double) matchCount / total;
				return Math.round(dbsize * matchRate);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				// Ignore and return unknown size
			} finally {
				connection.setAutoFlushCommands(true);
			}
		}
		return UNKNOWN_SIZE;
	}

	private Predicate<String> matchPredicate() {
		if (StringUtils.hasLength(keyPattern)) {
			MatchingEngine engine = GlobPattern.compile(keyPattern);
			return engine::matches;
		}
		return s -> true;
	}

	private Predicate<String> typePredicate() {
		if (StringUtils.hasLength(keyType)) {
			return keyType::equalsIgnoreCase;
		}
		return s -> true;
	}

}
