package com.redis.riot.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionInvocationTargetException;
import org.springframework.util.Assert;

import com.redis.riot.RedisOptions;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>>, ItemStream {

	private static final Logger log = Logger.getLogger(SpelProcessor.class.getName());

	private final RedisOptions redisOptions;
	private final EvaluationContext context;
	private final Map<String, Expression> expressions;
	private StatefulConnection<String, String> connection;
	private AtomicLong index;

	public SpelProcessor(RedisOptions redisOptions, EvaluationContext context, Map<String, Expression> expressions) {
		Assert.notNull(context, "A SpEL evaluation context is required.");
		Assert.notEmpty(expressions, "At least one field is required.");
		this.redisOptions = redisOptions;
		this.context = context;
		this.expressions = expressions;
	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		this.connection = redisOptions.connect();
		this.context.setVariable("redis", sync(connection));
		this.index = new AtomicLong();
		this.context.setVariable("index", index);
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		// do nothing
	}

	@Override
	public void close() throws ItemStreamException {
		if (connection != null) {
			connection.close();
		}
	}

	private static BaseRedisCommands<String, String> sync(StatefulConnection<String, String> connection) {
		if (connection instanceof StatefulRedisClusterConnection) {
			return ((StatefulRedisClusterConnection<String, String>) connection).sync();
		}
		return ((StatefulRedisConnection<String, String>) connection).sync();
	}

	@Override
	public Map<String, Object> process(Map<String, Object> item) {
		Map<String, Object> map = new HashMap<>(item);
		synchronized (context) {
			for (Entry<String, Expression> entry : expressions.entrySet()) {
				try {
					Object value = entry.getValue().getValue(context, map);
					if (value == null) {
						map.remove(entry.getKey());
					} else {
						map.put(entry.getKey(), value);
					}
				} catch (ExpressionInvocationTargetException e) {
					log.log(Level.SEVERE, e, () -> "Error while evaluating field " + entry.getKey());
					throw e;
				}
			}
			index.incrementAndGet();
		}
		return map;
	}

}
