package com.redislabs.recharge.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.redis.RedisCommandProperties.StringFormat;
import com.redislabs.recharge.redis.writer.AbstractCollectionRedisWriter;
import com.redislabs.recharge.redis.writer.AbstractRedisCommandWriter;
import com.redislabs.recharge.redis.writer.AbstractRedisWriter;
import com.redislabs.recharge.redis.writer.GeoWriter;
import com.redislabs.recharge.redis.writer.HashIncrByWriter;
import com.redislabs.recharge.redis.writer.HashWriter;
import com.redislabs.recharge.redis.writer.StreamWriter;
import com.redislabs.recharge.redis.writer.StringWriter;
import com.redislabs.recharge.redis.writer.ZSetWriter;

import io.lettuce.core.RedisFuture;

@Configuration
@EnableConfigurationProperties(RedisCommandProperties.class)
@ConditionalOnProperty("keyspace")
public class RedisCommandConfig {

	@Bean
	@StepScope
	public RedisReader redisReader(StatefulRediSearchConnection<String, String> connection,
			RedisCommandProperties props) {
		RedisReader reader = new RedisReader();
		reader.setConnection(connection);
		reader.setLimit(props.getLimit());
		reader.setMatch(props.getMatch());
		return reader;
	}

	@Bean
	@StepScope
	@ConditionalOnProperty("keyspace")
	public AbstractRedisWriter redisWriter(GenericObjectPool<StatefulRediSearchConnection<String, String>> pool,
			RedisCommandProperties props) {
		AbstractRedisCommandWriter writer = writer(props);
		writer.setKeyspace(props.getKeyspace());
		writer.setKeys(props.getKeys());
		writer.setPool(pool);
		if (writer instanceof AbstractCollectionRedisWriter) {
			AbstractCollectionRedisWriter collectionWriter = (AbstractCollectionRedisWriter) writer;
			collectionWriter.setFields(props.getFields());
		}
		return writer;
	}

	private AbstractRedisCommandWriter writer(RedisCommandProperties props) {
		switch (props.getType()) {
		case Geo:
			return geoWriter(props);
		case List:
			return listWriter(props);
		case Set:
			return setWriter();
		case Zset:
			return zsetWriter(props);
		case Stream:
			return streamWriter(props);
		case String:
			return stringWriter(props);
		default:
			return hashWriter(props);
		}
	}

	private StreamWriter streamWriter(RedisCommandProperties props) {
		StreamWriter writer = new StreamWriter();
		writer.setApproximateTrimming(props.isApproximateTrimming());
		writer.setIdField(props.getIdField());
		writer.setMaxlen(props.getMaxlen());
		return writer;
	}

	private ZSetWriter zsetWriter(RedisCommandProperties props) {
		ZSetWriter writer = new ZSetWriter();
		writer.setDefaultScore(props.getDefaultScore());
		writer.setScoreField(props.getScoreField());
		return writer;
	}

	private AbstractCollectionRedisWriter setWriter() {
		return new AbstractCollectionRedisWriter() {

			@Override
			protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
					RediSearchAsyncCommands<String, String> commands) {
				return commands.sadd(key, member);
			}
		};
	}

	private AbstractCollectionRedisWriter listWriter(RedisCommandProperties props) {
		if (props.isRight()) {
			return new AbstractCollectionRedisWriter() {

				@Override
				protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
						RediSearchAsyncCommands<String, String> commands) {
					return commands.rpush(key, member);
				}
			};
		}
		return new AbstractCollectionRedisWriter() {

			@Override
			protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
					RediSearchAsyncCommands<String, String> commands) {
				return commands.lpush(key, member);
			}
		};
	}

	private AbstractRedisCommandWriter hashWriter(RedisCommandProperties props) {
		if (props.getIncrementField() != null) {
			HashIncrByWriter hashIncrByWriter = new HashIncrByWriter();
			hashIncrByWriter.setField(props.getField());
			hashIncrByWriter.setIncrementField(props.getIncrementField());
			hashIncrByWriter.setDefaultIncrement(props.getDefaultIncrement());
			return hashIncrByWriter;
		}
		return new HashWriter();
	}

	private AbstractRedisCommandWriter geoWriter(RedisCommandProperties props) {
		GeoWriter writer = new GeoWriter();
		writer.setLatitudeField(props.getLatitudeField());
		writer.setLongitudeField(props.getLongitudeField());
		return writer;
	}

	private StringWriter stringWriter(RedisCommandProperties props) {
		StringWriter writer = new StringWriter();
		writer.setObjectWriter(objectWriter(props));
		return writer;
	}

	private ObjectWriter objectWriter(RedisCommandProperties props) {
		if (props.getFormat() == StringFormat.Xml) {
			return new XmlMapper().writer().withRootName(props.getRoot());
		}
		return new ObjectMapper().writer();
	}

}
