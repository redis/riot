package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.JedisWriter;
import com.redislabs.riot.redis.LettuSearchWriter;
import com.redislabs.riot.redis.LettuceWriter;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.CollectionItemWriter;
import com.redislabs.riot.redis.writer.EvalshaItemWriter;
import com.redislabs.riot.redis.writer.ExpireItemWriter;
import com.redislabs.riot.redis.writer.GeoaddItemWriter;
import com.redislabs.riot.redis.writer.HmsetItemWriter;
import com.redislabs.riot.redis.writer.LpushItemWriter;
import com.redislabs.riot.redis.writer.RedisItemWriter;
import com.redislabs.riot.redis.writer.RpushItemWriter;
import com.redislabs.riot.redis.writer.SaddWriter;
import com.redislabs.riot.redis.writer.SetFieldItemWriter;
import com.redislabs.riot.redis.writer.SetItemWriter;
import com.redislabs.riot.redis.writer.SetObjectItemWriter;
import com.redislabs.riot.redis.writer.XaddIdItemWriter;
import com.redislabs.riot.redis.writer.XaddIdMaxlenItemWriter;
import com.redislabs.riot.redis.writer.XaddItemWriter;
import com.redislabs.riot.redis.writer.XaddMaxlenItemWriter;
import com.redislabs.riot.redis.writer.ZaddItemWriter;

import io.lettuce.core.ScriptOutputType;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisWriterOptions {

	@Option(names = "--key-separator", description = "Redis key separator (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String separator = ":";
	@Option(names = { "-s", "--keyspace" }, description = "Redis keyspace prefix", paramLabel = "<string>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];
	@Option(names = { "-c",
			"--command" }, description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<command>")
	private RedisCommand command = RedisCommand.hmset;
	@Option(names = "--zset-score", description = "Name of the field to use for sorted set scores", paramLabel = "<field>")
	private String scoreField;
	@Option(names = "--zset-default-score", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<float>")
	private double defaultScore = 1d;
	@Option(names = { "-f",
			"--fields" }, arity = "1..*", description = "Names of fields composing member ids in collection data structures (list, geo, set, zset)", paramLabel = "<names>")
	private String[] fields = new String[0];
	@Option(names = "--expire-default-timeout", description = "Default timeout in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<seconds>")
	private long defaultTimeout = 60;
	@Option(names = "--expire-timeout", description = "Field to get the timeout value from", paramLabel = "<field>")
	private String timeoutField;
	@Option(names = "--geo-lon", description = "Longitude field", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--geo-lat", description = "Latitude field", paramLabel = "<field>")
	private String latitudeField;
	@Option(names = "--eval-sha", description = "SHA1 digest of the Lua script", paramLabel = "<string>")
	private String sha;
	@Option(names = "--eval-output", description = "Lua script output type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;
	@Option(names = "--eval-keys", arity = "1..*", description = "Fields for Lua script keys", paramLabel = "<field1,field2,...>")
	private String[] evalKeys = new String[0];
	@Option(names = "--eval-args", arity = "1..*", description = "Fields for Lua script args", paramLabel = "<field1,field2,...>")
	private String[] evalArgs = new String[0];
	@Option(names = "--xadd-trim", description = "Apply efficient trimming for capped streams using the ~ flag")
	private boolean trim;
	@Option(names = "--xadd-maxlen", description = "Limit stream to maxlen entries", paramLabel = "<integer>")
	private Long maxlen;
	@Option(names = "--xadd-id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String idField;
	@Option(names = "--string-format", description = "Serialization format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private StringFormat format = StringFormat.json;
	@Option(names = "--string-root", description = "XML root element name", paramLabel = "<name>")
	private String root;
	@Option(names = "--string-value", description = "Field to use for value when using raw format", paramLabel = "<field>")
	private String field;
	@ArgGroup(exclusive = false, heading = "RediSearch writer options%n")
	private RediSearchWriterOptions search = new RediSearchWriterOptions();

	public RedisCommand getCommand() {
		return command;
	}

	private ZaddItemWriter zaddWriter() {
		ZaddItemWriter writer = new ZaddItemWriter();
		writer.setDefaultScore(defaultScore);
		writer.setScoreField(scoreField);
		return writer;
	}

	private SetItemWriter setWriter() {
		switch (format) {
		case raw:
			SetFieldItemWriter fieldWriter = new SetFieldItemWriter();
			fieldWriter.setField(field);
			return fieldWriter;
		case xml:
			SetObjectItemWriter xmlWriter = new SetObjectItemWriter();
			xmlWriter.setObjectWriter(objectWriter(new XmlMapper()));
			return xmlWriter;
		default:
			SetObjectItemWriter jsonWriter = new SetObjectItemWriter();
			jsonWriter.setObjectWriter(objectWriter(new ObjectMapper()));
			return jsonWriter;
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(root);
	}

	private RedisItemWriter xaddWriter() {
		if (idField == null) {
			if (maxlen == null) {
				return new XaddItemWriter();
			}
			XaddMaxlenItemWriter writer = new XaddMaxlenItemWriter();
			writer.setApproximateTrimming(trim);
			writer.setMaxlen(maxlen);
			return writer;
		}
		if (maxlen == null) {
			XaddIdItemWriter writer = new XaddIdItemWriter();
			writer.setIdField(idField);
			return writer;
		}
		XaddIdMaxlenItemWriter writer = new XaddIdMaxlenItemWriter();
		writer.setApproximateTrimming(trim);
		writer.setIdField(idField);
		writer.setMaxlen(maxlen);
		return writer;
	}

	private EvalshaItemWriter evalshaWriter() {
		EvalshaItemWriter luaWriter = new EvalshaItemWriter();
		luaWriter.setArgs(evalArgs);
		luaWriter.setKeys(evalKeys);
		luaWriter.setOutputType(outputType);
		luaWriter.setSha(sha);
		return luaWriter;
	}

	private ExpireItemWriter expireWriter() {
		ExpireItemWriter expireWriter = new ExpireItemWriter();
		expireWriter.setDefaultTimeout(defaultTimeout);
		expireWriter.setTimeoutField(timeoutField);
		return expireWriter;
	}

	private GeoaddItemWriter geoWriter() {
		GeoaddItemWriter writer = new GeoaddItemWriter();
		writer.setLatitudeField(latitudeField);
		writer.setLongitudeField(longitudeField);
		return writer;
	}

	public AbstractRedisItemWriter itemWriter() {
		AbstractRedisItemWriter redisItemWriter = redisItemWriter();
		redisItemWriter.setConverter(new RedisConverter(separator, keyspace, keys));
		if (redisItemWriter instanceof CollectionItemWriter) {
			((CollectionItemWriter) redisItemWriter).setFields(fields);
		}
		return redisItemWriter;
	}

	private AbstractRedisItemWriter redisItemWriter() {
		switch (command) {
		case expire:
			return expireWriter();
		case geoadd:
			return geoWriter();
		case lpush:
			return new LpushItemWriter();
		case rpush:
			return new RpushItemWriter();
		case evalsha:
			return evalshaWriter();
		case sadd:
			return new SaddWriter();
		case xadd:
			return xaddWriter();
		case set:
			return setWriter();
		case zadd:
			return zaddWriter();
		default:
			return new HmsetItemWriter();
		}
	}

	public ItemWriter<Map<String, Object>> writer(RedisConnectionOptions connection) {
		if (search.isSet()) {
			return new LettuSearchWriter(connection.rediSearchClient(), connection.poolConfig(), search.itemWriter());
		}
		if (connection.getDriver() == RedisDriver.jedis) {
			return new JedisWriter(connection.jedisPool(), itemWriter());
		}
		return new LettuceWriter(connection.redisClient(), connection.poolConfig(), itemWriter());
	}

}