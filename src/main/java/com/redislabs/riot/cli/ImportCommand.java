package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.CommandWriter;
import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.map.AbstractCollectionMapCommandWriter;
import com.redislabs.riot.redis.writer.map.AbstractKeyMapCommandWriter;
import com.redislabs.riot.redis.writer.map.Hmset;
import com.redislabs.riot.redis.writer.map.Lpush;
import com.redislabs.riot.redis.writer.map.Noop;
import com.redislabs.riot.redis.writer.map.Rpush;
import com.redislabs.riot.redis.writer.map.Sadd;
import com.redislabs.riot.redis.writer.map.Set.Format;
import com.redislabs.riot.redis.writer.map.SetField;
import com.redislabs.riot.redis.writer.map.SetObject;
import com.redislabs.riot.redis.writer.map.Xadd;
import com.redislabs.riot.redis.writer.map.XaddId;
import com.redislabs.riot.redis.writer.map.XaddIdMaxlen;
import com.redislabs.riot.redis.writer.map.XaddMaxlen;
import com.redislabs.riot.redis.writer.map.Zadd;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(sortOptions = false)
public abstract class ImportCommand extends TransferCommand<Map<String, Object>, Map<String, Object>> {

	public enum Command {
		EVALSHA, EXPIRE, GEOADD, FTADD, FTSEARCH, FTAGGREGATE, FTSUGADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD,
		ZADD
	}

	@Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private Command command = Command.HMSET;
	@Option(names = "--member-space", description = "Prefix for member IDs", paramLabel = "<str>")
	private String memberKeyspace;
	@Option(names = "--members", arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
	private String[] memberIds = new String[0];
	@Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = KeyBuilder.DEFAULT_KEY_SEPARATOR;
	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
	private String[] keys = new String[0];
	@Option(names = "--keep-keys", description = "Keep key fields in data structure")
	private boolean keepKeyFields;
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String score;
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double defaultScore = 1d;
	@Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private Format format = Format.JSON;
	@Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String root;
	@Option(names = "--value", description = "String raw value field", paramLabel = "<field>")
	private String value;
	@Option(names = "--trim", description = "Stream efficient trimming (~ flag)")
	private boolean trim;
	@Option(names = "--maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long maxlen;
	@Option(names = "--stream-id", description = "Stream entry ID field", paramLabel = "<field>")
	private String id;
	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private MapProcessorOptions mapProcessor = new MapProcessorOptions();
	@ArgGroup(exclusive = false, heading = "EVALSHA options%n")
	private EvalshaOptions evalsha = new EvalshaOptions();
	@ArgGroup(exclusive = false, heading = "EXPIRE options%n")
	private ExpireOptions expire = new ExpireOptions();
	@ArgGroup(exclusive = false, heading = "GEOADD options%n")
	private GeoaddOptions geoadd = new GeoaddOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch options%n")
	private RediSearchOptions search = new RediSearchOptions();

	private KeyBuilder memberIdBuilder() {
		return KeyBuilder.builder().separator(separator).prefix(memberKeyspace).fields(memberIds).build();
	}

	private CommandWriter<Map<String, Object>> mapCommandWriter(Command command) {
		switch (command) {
		case EVALSHA:
			return evalsha.builder().keys(keys).build();
		case EXPIRE:
			return expire.builder().build();
		case FTADD:
			if (search.hasPayload()) {
				return search.ftAddPayload().score(score).defaultScore(defaultScore).build();
			}
			return search.ftAdd().score(score).defaultScore(defaultScore).build();
		case FTAGGREGATE:
			return search.aggregate().build();
		case FTSEARCH:
			return search.search().build();
		case FTSUGADD:
			if (search.hasPayload()) {
				return search.sugaddPayload().score(score).defaultScore(defaultScore).build();
			}
			return search.sugadd().score(score).defaultScore(defaultScore).build();
		case GEOADD:
			return geoadd.geoadd();
		case HMSET:
			return Hmset.builder().build();
		case LPUSH:
			return Lpush.builder().build();
		case NOOP:
			return Noop.<Map<String, Object>>builder().build();
		case RPUSH:
			return Rpush.builder().build();
		case SADD:
			return Sadd.builder().build();
		case SET:
			switch (format) {
			case RAW:
				return SetField.builder().field(value).build();
			case XML:
				return SetObject.builder().objectWriter(new XmlMapper().writer().withRootName(root)).build();
			default:
				return SetObject.builder().objectWriter(new ObjectMapper().writer().withRootName(root)).build();
			}
		case XADD:
			if (id == null) {
				if (maxlen == null) {
					return Xadd.builder().build();
				}
				return XaddMaxlen.builder().approximateTrimming(trim).maxlen(maxlen).build();
			}
			if (maxlen == null) {
				XaddId.builder().id(id).build();
			}
			return XaddIdMaxlen.builder().approximateTrimming(trim).maxlen(maxlen).id(id).build();
		case ZADD:
			return Zadd.builder().defaultScore(defaultScore).score(score).build();
		}
		throw new IllegalArgumentException("Command " + command + " not supported");
	}

	@Override
	protected AbstractRedisItemWriter<Map<String, Object>> writer() throws Exception {
		CommandWriter<Map<String, Object>> writer = mapCommandWriter(command);
		if (writer instanceof AbstractKeyMapCommandWriter) {
			AbstractKeyMapCommandWriter keyWriter = (AbstractKeyMapCommandWriter) writer;
			if (keyspace == null && keys.length == 0) {
				log.warn("No keyspace nor key fields specified; using empty key (\"\")");
			}
			keyWriter.setKeyBuilder(KeyBuilder.builder().separator(separator).prefix(keyspace).fields(keys).build());
			keyWriter.setKeepKeyFields(keepKeyFields);
			if (writer instanceof AbstractCollectionMapCommandWriter) {
				((AbstractCollectionMapCommandWriter) writer).setMemberIdBuilder(memberIdBuilder());
			}
		}
		return writer(redisOptions(), writer);
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		return mapProcessor.processor();
	}

}
