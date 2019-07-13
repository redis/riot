package com.redislabs.riot.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisDataStructureItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.AbstractStringWriter;
import com.redislabs.riot.redis.writer.ExpireWriter;
import com.redislabs.riot.redis.writer.GeoWriter;
import com.redislabs.riot.redis.writer.HashWriter;
import com.redislabs.riot.redis.writer.ListLeftPushWriter;
import com.redislabs.riot.redis.writer.ListRightPushWriter;
import com.redislabs.riot.redis.writer.LuaWriter;
import com.redislabs.riot.redis.writer.SetWriter;
import com.redislabs.riot.redis.writer.StreamIdMaxlenWriter;
import com.redislabs.riot.redis.writer.StreamIdWriter;
import com.redislabs.riot.redis.writer.StreamMaxlenWriter;
import com.redislabs.riot.redis.writer.StreamWriter;
import com.redislabs.riot.redis.writer.StringFieldWriter;
import com.redislabs.riot.redis.writer.StringObjectWriter;
import com.redislabs.riot.redis.writer.ZSetWriter;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "redis", description = "Redis data structure")
public class RedisDataStructureWriterCommand extends AbstractRedisWriterCommand<RedisAsyncCommands<String, String>> {

	enum RedisDataStructure {
		Geo, Hash, List, Set, Stream, String, ZSet, Lua, Expire
	}

	@Option(names = "--type", description = "Redis data structure: ${COMPLETION-CANDIDATES}.")
	private RedisDataStructure dataStructure = RedisDataStructure.Hash;
	@Option(names = "--fields", arity = "1..*", description = "Fields used to build member ids for collections (list, set, zset, geo).")
	private String[] fields = new String[0];
	@ArgGroup(exclusive = false, heading = "Lists%n")
	private ListOptions list = new ListOptions();
	@ArgGroup(exclusive = false, heading = "Geospatial%n")
	private GeoOptions geo = new GeoOptions();
	@ArgGroup(exclusive = false, heading = "Streams%n")
	private StreamOptions stream = new StreamOptions();
	@ArgGroup(exclusive = false, heading = "Sorted sets%n")
	private ZSetOptions zset = new ZSetOptions();
	@ArgGroup(exclusive = false, heading = "Strings%n")
	private StringOptions string = new StringOptions();
	@ArgGroup(exclusive = false, heading = "LUA script%n")
	private LuaOptions lua = new LuaOptions();
	@ArgGroup(exclusive = false, heading = "Expire%n")
	private ExpireOptions expire = new ExpireOptions();

	static class ExpireOptions {
		@Option(names = "--default-timeout", description = "Default timeout in seconds.", paramLabel = "<seconds>")
		long defaultTimeout = 60;
		@Option(names = "--timeout", description = "Field to get the timeout value from", paramLabel = "<field>")
		String timeoutField;

		public ExpireWriter writer() {
			ExpireWriter writer = new ExpireWriter();
			writer.setDefaultTimeout(defaultTimeout);
			writer.setTimeoutField(timeoutField);
			return writer;
		}

	}

	static class LuaOptions {
		@Option(names = "--sha", description = "SHA1 digest of the LUA script")
		String sha;
		@Option(names = "--output", description = "Script output type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
		ScriptOutputType outputType = ScriptOutputType.STATUS;
		@Option(names = "--lua-keys", arity = "1..*", description = "Field names of the LUA script keys", paramLabel = "<field1,field2,...>")
		String[] keys = new String[0];
		@Option(names = "--lua-args", arity = "1..*", description = "Field names of the LUA script args", paramLabel = "<field1,field2,...>")
		String[] args = new String[0];

		private LuaWriter writer() {
			LuaWriter writer = new LuaWriter(sha);
			writer.setOutputType(outputType);
			writer.setKeys(keys);
			writer.setArgs(args);
			return writer;
		}
	}

	static class GeoOptions {

		@Option(names = "--lon", description = "Longitude field for geo sets.", paramLabel = "<field>")
		String longitudeField;
		@Option(names = "--lat", description = "Latitude field for geo sets.", paramLabel = "<field>")
		String latitudeField;

		public GeoWriter writer() {
			GeoWriter writer = new GeoWriter();
			writer.setLatitudeField(latitudeField);
			writer.setLongitudeField(longitudeField);
			return writer;
		}
	}

	static class StreamOptions {
		@Option(names = "--trim", description = "Apply efficient trimming for capped streams using the ~ flag.")
		boolean trim;
		@Option(names = "--max", description = "Limit stream to maxlen entries.", paramLabel = "<len>")
		Long maxlen;
		@Option(names = "--id", description = "Field used for stream entry IDs.", paramLabel = "<field>")
		String id;

		public AbstractRedisDataStructureItemWriter writer() {
			if (id == null) {
				if (maxlen == null) {
					return new StreamWriter();
				}
				StreamMaxlenWriter writer = new StreamMaxlenWriter();
				writer.setMaxlen(maxlen);
				writer.setApproximateTrimming(trim);
				return writer;
			}
			if (maxlen == null) {
				StreamIdWriter writer = new StreamIdWriter();
				writer.setIdField(id);
				return writer;
			}
			StreamIdMaxlenWriter writer = new StreamIdMaxlenWriter();
			writer.setApproximateTrimming(trim);
			writer.setIdField(id);
			writer.setMaxlen(maxlen);
			return writer;
		}
	}

	static class ListOptions {

		public enum PushDirection {
			Left, Right
		}

		@Option(names = "--push-direction", hidden = true, description = "Direction for list push: ${COMPLETION-CANDIDATES}.")
		private PushDirection direction = PushDirection.Left;

		public AbstractCollectionRedisItemWriter writer() {
			switch (direction) {
			case Right:
				return new ListRightPushWriter();
			default:
				return new ListLeftPushWriter();
			}
		}
	}

	static class ZSetOptions {
		@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
		private String score;
		@Option(names = "--default-score", description = "Default score to use when score field is not present.", paramLabel = "<float>")
		private double defaultScore = 1d;

		private ZSetWriter writer() {
			ZSetWriter writer = new ZSetWriter();
			writer.setScoreField(score);
			writer.setDefaultScore(defaultScore);
			return writer;
		}
	}

	static class StringOptions {

		public static enum StringFormat {
			Raw, Xml, Json
		}

		@Option(names = "--format", description = "Serialization format: ${COMPLETION-CANDIDATES}.")
		private StringFormat format = StringFormat.Json;
		@Option(names = "--root", description = "XML root element name.", paramLabel = "<name>")
		private String root;
		@Option(names = "--value", description = "Field to use for value when using raw format.", paramLabel = "<field>")
		private String field;

		private AbstractStringWriter writer() {
			switch (format) {
			case Raw:
				return new StringFieldWriter(field);
			case Xml:
				return new StringObjectWriter(objectWriter(new XmlMapper()));
			default:
				return new StringObjectWriter(objectWriter(new ObjectMapper()));
			}
		}

		private ObjectWriter objectWriter(ObjectMapper mapper) {
			return mapper.writer().withRootName(root);
		}

	}

	@Override
	protected AbstractRedisItemWriter<RedisAsyncCommands<String, String>> redisItemWriter() {
		AbstractRedisDataStructureItemWriter itemWriter = dataStructureWriter();
		if (itemWriter instanceof AbstractCollectionRedisItemWriter) {
			((AbstractCollectionRedisItemWriter) itemWriter).setFields(fields);
		}
		return itemWriter;
	}

	private AbstractRedisDataStructureItemWriter dataStructureWriter() {
		switch (dataStructure) {
		case Expire:
			return expire.writer();
		case Geo:
			return geo.writer();
		case List:
			return list.writer();
		case Lua:
			return lua.writer();
		case Set:
			return new SetWriter();
		case Stream:
			return stream.writer();
		case String:
			return string.writer();
		case ZSet:
			return zset.writer();
		default:
			return new HashWriter();
		}
	}

	@Override
	protected String getTargetDescription() {
		return "Redis " + dataStructure.name() + " \"" + keyspaceDescription() + "\"";
	}

}
