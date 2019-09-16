package com.redislabs.riot.cli.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.cli.RedisCommand;
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
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import picocli.CommandLine.Option;

public class RedisCommandOptions {

	@Option(names = "--zset-score", description = "Name of the field to use for sorted set scores", paramLabel = "<field>")
	private String zsetScore;
	@Option(names = "--zset-default-score", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<float>")
	private double zsetDefaultScore = 1d;
	@Option(names = "--members", arity = "1..*", description = "Names of fields composing member ids in collection data structures (list, geo, set, zset)", paramLabel = "<names>")
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

	private RedisItemWriter<RedisStreamAsyncCommands<String, String>> xaddWriter() {
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

	public AbstractRedisItemWriter<?> writer(RedisCommand command) {
		AbstractRedisItemWriter<?> redisItemWriter = redisItemWriter(command);
		if (redisItemWriter instanceof CollectionItemWriter) {
			((CollectionItemWriter<?>) redisItemWriter).setFields(members);
		}
		return redisItemWriter;
	}

	private AbstractRedisItemWriter<?> redisItemWriter(RedisCommand command) {
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

}