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
	@Option(names = { "-k", "--keyspace" }, description = "Redis keyspace prefix", paramLabel = "<string>")
	private String keyspace;
	@Option(names = { "-f", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];
	@Option(names = { "-r",
			"--type" }, description = "Redis type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private RedisType redisType = RedisType.hash;
	@Option(names = "--zset-score", description = "Name of the field to use for sorted set scores", paramLabel = "<field>")
	private String zsetScore;
	@Option(names = "--zset-default-score", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<float>")
	private double zsetDefaultScore = 1d;
	@Option(names = { "-m",
			"--members" }, arity = "1..*", description = "Names of fields composing member ids in collection data structures (list, geo, set, zset)", paramLabel = "<names>")
	private String[] members = new String[0];
	@Option(names = "--expire-default-timeout", description = "Default timeout in seconds (default: ${DEFAULT-VALUE})", paramLabel = "<seconds>")
	private long expireDefaultTimeout = 60;
	@Option(names = "--expire-timeout", description = "Field to get the timeout value from", paramLabel = "<field>")
	private String expireTimeout;
	@Option(names = "--geo-lon", description = "Longitude field", paramLabel = "<field>")
	private String geoLon;
	@Option(names = "--geo-lat", description = "Latitude field", paramLabel = "<field>")
	private String geoLat;
	@Option(names = "--eval-sha", description = "SHA1 digest of the Lua script", paramLabel = "<string>")
	private String evalSha;
	@Option(names = "--eval-output", description = "Lua script output type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;
	@Option(names = "--eval-keys", arity = "1..*", description = "Fields for Lua script keys", paramLabel = "<field1,field2,...>")
	private String[] evalKeys = new String[0];
	@Option(names = "--eval-args", arity = "1..*", description = "Fields for Lua script args", paramLabel = "<field1,field2,...>")
	private String[] evalArgs = new String[0];
	@Option(names = "--list-direction", description = "List direction: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private ListPushDirection listDirection = ListPushDirection.left;
	@Option(names = "--xadd-trim", description = "Apply efficient trimming for capped streams using the ~ flag")
	private boolean xaddTrim;
	@Option(names = "--xadd-maxlen", description = "Limit stream to maxlen entries", paramLabel = "<int>")
	private Long xaddMaxlen;
	@Option(names = "--xadd-id", description = "Field used for stream entry IDs", paramLabel = "<field>")
	private String xaddId;
	@Option(names = "--string-format", description = "Serialization format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private StringFormat stringFormat = StringFormat.json;
	@Option(names = "--string-root", description = "XML root element name", paramLabel = "<name>")
	private String stringRoot;
	@Option(names = "--string-value", description = "Field to use for value when using raw format", paramLabel = "<field>")
	private String stringValue;
	@ArgGroup(exclusive = false, heading = "RediSearch writer options%n")
	private RediSearchWriterOptions search = new RediSearchWriterOptions();

	public RedisType getCommand() {
		return redisType;
	}

	private ZaddItemWriter zaddWriter() {
		ZaddItemWriter writer = new ZaddItemWriter();
		writer.setDefaultScore(zsetDefaultScore);
		writer.setScoreField(zsetScore);
		return writer;
	}

	private SetItemWriter setWriter() {
		switch (stringFormat) {
		case raw:
			SetFieldItemWriter fieldWriter = new SetFieldItemWriter();
			fieldWriter.setField(stringValue);
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
		return mapper.writer().withRootName(stringRoot);
	}

	private RedisItemWriter xaddWriter() {
		if (xaddId == null) {
			if (xaddMaxlen == null) {
				return new XaddItemWriter();
			}
			XaddMaxlenItemWriter writer = new XaddMaxlenItemWriter();
			writer.setApproximateTrimming(xaddTrim);
			writer.setMaxlen(xaddMaxlen);
			return writer;
		}
		if (xaddMaxlen == null) {
			XaddIdItemWriter writer = new XaddIdItemWriter();
			writer.setIdField(xaddId);
			return writer;
		}
		XaddIdMaxlenItemWriter writer = new XaddIdMaxlenItemWriter();
		writer.setApproximateTrimming(xaddTrim);
		writer.setIdField(xaddId);
		writer.setMaxlen(xaddMaxlen);
		return writer;
	}

	private EvalshaItemWriter evalshaWriter() {
		EvalshaItemWriter luaWriter = new EvalshaItemWriter();
		luaWriter.setArgs(evalArgs);
		luaWriter.setKeys(evalKeys);
		luaWriter.setOutputType(outputType);
		luaWriter.setSha(evalSha);
		return luaWriter;
	}

	private ExpireItemWriter expireWriter() {
		ExpireItemWriter expireWriter = new ExpireItemWriter();
		expireWriter.setDefaultTimeout(expireDefaultTimeout);
		expireWriter.setTimeoutField(expireTimeout);
		return expireWriter;
	}

	private GeoaddItemWriter geoWriter() {
		GeoaddItemWriter writer = new GeoaddItemWriter();
		writer.setLatitudeField(geoLat);
		writer.setLongitudeField(geoLon);
		return writer;
	}

	public AbstractRedisItemWriter itemWriter() {
		AbstractRedisItemWriter redisItemWriter = redisItemWriter();
		redisItemWriter.setConverter(new RedisConverter(separator, keyspace, keys));
		if (redisItemWriter instanceof CollectionItemWriter) {
			((CollectionItemWriter) redisItemWriter).setFields(members);
		}
		return redisItemWriter;
	}

	private AbstractRedisItemWriter redisItemWriter() {
		switch (redisType) {
		case ex:
			return expireWriter();
		case geo:
			return geoWriter();
		case list:
			if (listDirection == ListPushDirection.left) {
				return new LpushItemWriter();
			}
			return new RpushItemWriter();
		case lua:
			return evalshaWriter();
		case set:
			return new SaddWriter();
		case stream:
			return xaddWriter();
		case string:
			return setWriter();
		case zset:
			return zaddWriter();
		default:
			return new HmsetItemWriter();
		}
	}

	public ItemWriter<Map<String, Object>> writer(RedisConnectionOptions connection) {
		if (redisType == RedisType.search) {
			return new LettuSearchWriter(connection.rediSearchClient(), connection.poolConfig(),
					search.searchItemWriter());
		}
		if (redisType == RedisType.suggest) {
			return new LettuSearchWriter(connection.rediSearchClient(), connection.poolConfig(),
					search.suggestItemWriter());
		}
		if (connection.getDriver() == RedisDriver.jedis) {
			return new JedisWriter(connection.jedisPool(), itemWriter());
		}
		return new LettuceWriter(connection.redisClient(), connection.poolConfig(), itemWriter());
	}

}