package com.redislabs.riot.cli;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.CommandWriter;
import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.map.AbstractCollectionMapCommandWriter;
import com.redislabs.riot.redis.writer.map.AbstractKeyMapCommandWriter;
import com.redislabs.riot.redis.writer.map.AbstractMapCommandWriter;
import com.redislabs.riot.redis.writer.map.AbstractSearchMapCommandWriter;
import com.redislabs.riot.redis.writer.map.Evalsha;
import com.redislabs.riot.redis.writer.map.Expire;
import com.redislabs.riot.redis.writer.map.FtAdd;
import com.redislabs.riot.redis.writer.map.FtAdd.FtAddPayload;
import com.redislabs.riot.redis.writer.map.FtSugadd;
import com.redislabs.riot.redis.writer.map.FtSugadd.FtSugaddPayload;
import com.redislabs.riot.redis.writer.map.Geoadd;
import com.redislabs.riot.redis.writer.map.Hmset;
import com.redislabs.riot.redis.writer.map.Lpush;
import com.redislabs.riot.redis.writer.map.Noop;
import com.redislabs.riot.redis.writer.map.Rpush;
import com.redislabs.riot.redis.writer.map.Sadd;
import com.redislabs.riot.redis.writer.map.Set;
import com.redislabs.riot.redis.writer.map.Set.Format;
import com.redislabs.riot.redis.writer.map.Set.SetField;
import com.redislabs.riot.redis.writer.map.Set.SetObject;
import com.redislabs.riot.redis.writer.map.Xadd;
import com.redislabs.riot.redis.writer.map.Xadd.XaddId;
import com.redislabs.riot.redis.writer.map.Xadd.XaddIdMaxlen;
import com.redislabs.riot.redis.writer.map.Xadd.XaddMaxlen;
import com.redislabs.riot.redis.writer.map.Zadd;

import io.lettuce.core.ScriptOutputType;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
public class ImportOptions {

	public enum Command {
		EVALSHA, EXPIRE, GEOADD, FTADD, FTSUGADD, HMSET, LPUSH, NOOP, RPUSH, SADD, SET, XADD, ZADD
	}

	@Option(names = "--command", description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private Command command = Command.HMSET;
	@Option(names = "--member-space", description = "Prefix for member IDs", paramLabel = "<str>")
	private String memberKeyspace;
	@Option(names = "--members", arity = "1..*", description = "Member field names for collections", paramLabel = "<name>")
	private String[] memberIds = new String[0];
	@Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String separator = KeyBuilder.DEFAULT_KEY_SEPARATOR;
	@Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix", paramLabel = "<str>")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] keys = new String[0];
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String score;
	@Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double defaultScore = 1d;
	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitude;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitude;
	@Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long defaultTimeout = 60;
	@Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<name>")
	private String timeout;
	@Option(names = { "-i", "--index" }, description = "Name of the RediSearch index", paramLabel = "<name>")
	private String index;
	@Option(names = "--nosave", description = "Do not save docs, only index")
	private boolean noSave;
	@Option(names = "--replace", description = "UPSERT-style insertion")
	private boolean replace;
	@Option(names = "--partial", description = "Partial update (only applicable with replace)")
	private boolean partial;
	@Option(names = "--language", description = "Stemmer to use for indexing: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
	private Language language;
	@Option(names = "--if-condition", description = "Boolean expression for conditional update", paramLabel = "<exp>")
	private String ifCondition;
	@Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
	private String payload;
	@Option(names = "--suggest", description = "Field containing the suggestion", paramLabel = "<field>")
	private String suggest;
	@Option(names = "--increment", description = "Use increment to set value")
	private boolean increment;
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
	@Option(names = "--eval-args", arity = "1..*", description = "EVALSHA arg field names", paramLabel = "<names>")
	private String[] args = new String[0];
	@Option(names = "--eval-sha", description = "EVALSHA digest", paramLabel = "<sha>")
	private String sha;
	@Option(names = "--eval-output", description = "EVALSHA output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
	private ScriptOutputType outputType = ScriptOutputType.STATUS;

	private KeyBuilder keyBuilder() {
		if (keyspace == null && keys.length == 0) {
			log.warn("No keyspace nor key fields specified; using empty key (\"\")");
		}
		return KeyBuilder.builder().separator(separator).prefix(keyspace).fields(keys).build();
	}

	private KeyBuilder memberIdBuilder() {
		return KeyBuilder.builder().separator(separator).prefix(memberKeyspace).fields(memberIds).build();
	}

	public CommandWriter<Map<String, Object>> writer() {
		CommandWriter<Map<String, Object>> writer = mapCommandWriter(command);
		if (writer instanceof AbstractMapCommandWriter) {
			((AbstractKeyMapCommandWriter) writer).keyBuilder(keyBuilder());
			if (writer instanceof AbstractCollectionMapCommandWriter) {
				((AbstractCollectionMapCommandWriter) writer).setMemberIdBuilder(memberIdBuilder());
			}
			if (writer instanceof AbstractSearchMapCommandWriter) {
				((AbstractSearchMapCommandWriter) writer).score(score).defaultScore(defaultScore);
			}
		}
		return writer;
	}

	private CommandWriter<Map<String, Object>> mapCommandWriter(Command command) {
		switch (command) {
		case EVALSHA:
			return new Evalsha().args(args).keys(keys).outputType(outputType).sha(sha);
		case EXPIRE:
			return new Expire().defaultTimeout(defaultTimeout).timeoutField(timeout);
		case FTADD:
			return ftAdd().options(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave)
					.replace(replace).replacePartial(partial).build()).index(index);
		case FTSUGADD:
			return ftSugadd().field(suggest).increment(increment);
		case GEOADD:
			return new Geoadd().longitude(longitude).latitude(latitude);
		case LPUSH:
			return new Lpush();
		case NOOP:
			return new Noop<Map<String, Object>>();
		case RPUSH:
			return new Rpush();
		case SADD:
			return new Sadd();
		case SET:
			return set();
		case XADD:
			return xadd();
		case ZADD:
			return new Zadd().defaultScore(defaultScore).score(score);
		default:
			return new Hmset();
		}
	}

	private Set set() {
		switch (format) {
		case RAW:
			return new SetField().field(value);
		case XML:
			return new SetObject().objectWriter(new XmlMapper().writer().withRootName(root));
		default:
			return new SetObject().objectWriter(new ObjectMapper().writer().withRootName(root));
		}
	}

	private Xadd xadd() {
		if (id == null) {
			if (maxlen == null) {
				return new Xadd();
			}
			XaddMaxlen xaddMaxlen = new XaddMaxlen();
			xaddMaxlen.approximateTrimming(trim);
			xaddMaxlen.maxlen(maxlen);
			return xaddMaxlen;
		}
		if (maxlen == null) {
			XaddId xaddId = new XaddId();
			xaddId.id(id);
			return xaddId;
		}
		XaddIdMaxlen xaddIdMaxlen = new XaddIdMaxlen();
		xaddIdMaxlen.approximateTrimming(trim);
		xaddIdMaxlen.maxlen(maxlen);
		xaddIdMaxlen.id(id);
		return xaddIdMaxlen;
	}

	private FtAdd ftAdd() {
		if (payload == null) {
			return new FtAdd();
		}
		return new FtAddPayload().payload(payload);
	}

	private FtSugadd ftSugadd() {
		if (payload == null) {
			return new FtSugadd();
		}
		return new FtSugaddPayload().payload(payload);
	}

}
