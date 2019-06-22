package com.redislabs.riot.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisDataStructureItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.AbstractStringWriter;
import com.redislabs.riot.redis.writer.GeoWriter;
import com.redislabs.riot.redis.writer.HashWriter;
import com.redislabs.riot.redis.writer.ListWriter;
import com.redislabs.riot.redis.writer.SetWriter;
import com.redislabs.riot.redis.writer.StreamIdMaxlenWriter;
import com.redislabs.riot.redis.writer.StreamIdWriter;
import com.redislabs.riot.redis.writer.StreamMaxlenWriter;
import com.redislabs.riot.redis.writer.StreamWriter;
import com.redislabs.riot.redis.writer.StringFieldWriter;
import com.redislabs.riot.redis.writer.StringObjectWriter;
import com.redislabs.riot.redis.writer.ZSetWriter;

import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Getter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "redis", description = "Redis data structure")
public class RedisDataStructureWriterCommand extends AbstractRedisWriterCommand<RedisAsyncCommands<String, String>> {

	enum RedisDataStructure {
		Geo, Hash, List, Set, Stream, String, ZSet
	}

	@Option(names = "--type", description = "Redis data structure: ${COMPLETION-CANDIDATES}.")
	private RedisDataStructure dataStructure = RedisDataStructure.Hash;
	@Option(names = "--fields", description = "Fields used to build member ids for collections (list, set, zset, geo).")
	private String[] fields = new String[0];
	@ArgGroup(exclusive = false, heading = "Geospatial%n")
	private GeoOptions geo = new GeoOptions();
	@ArgGroup(exclusive = false, heading = "Streams%n")
	private StreamOptions stream = new StreamOptions();
	@ArgGroup(exclusive = false, heading = "Sorted sets%n")
	private ZSetOptions zset = new ZSetOptions();
	@ArgGroup(exclusive = false, heading = "Strings%n")
	private StringOptions string = new StringOptions();

	@Getter
	static class GeoOptions {
		@Option(names = "--lon", description = "Longitude field for geo sets.", paramLabel = "<field>")
		private String lon;
		@Option(names = "--lat", description = "Latitude field for geo sets.", paramLabel = "<field>")
		private String lat;
	}

	@Getter
	static class StreamOptions {
		@Option(names = "--trim", description = "Apply efficient trimming for capped streams using the ~ flag.")
		private boolean trim;
		@Option(names = "--max", description = "Limit stream to maxlen entries.", paramLabel = "<len>")
		private Long maxlen;
		@Option(names = "--id", description = "Field used for stream entry IDs.", paramLabel = "<field>")
		private String id;
	}

	@Getter
	static class ListOptions {

		public enum PushDirection {
			Left, Right
		}

		@Option(names = "--push-direction", hidden = true, description = "Direction for list push: ${COMPLETION-CANDIDATES}.")
		private PushDirection direction = PushDirection.Left;
	}

	@Getter
	static class ZSetOptions {
		@Option(names = "--score", description = "Name of the field to use for scores.", paramLabel = "<field>")
		private String score;
		@Option(names = "--default-score", description = "Default score to use when score field is not present.", paramLabel = "<float>")
		private double defaultScore = 1d;
	}

	@Getter
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

	}

	private AbstractStringWriter stringWriter() {
		switch (string.getFormat()) {
		case Raw:
			return new StringFieldWriter(string.getField());
		case Xml:
			return new StringObjectWriter(objectWriter(new XmlMapper()));
		default:
			return new StringObjectWriter(objectWriter(new ObjectMapper()));
		}
	}

	private ObjectWriter objectWriter(ObjectMapper mapper) {
		return mapper.writer().withRootName(string.getRoot());
	}

	private GeoWriter geoWriter() {
		GeoWriter writer = new GeoWriter();
		writer.setLatitudeField(geo.getLat());
		writer.setLongitudeField(geo.getLon());
		return writer;
	}

	private AbstractRedisDataStructureItemWriter streamWriter() {
		if (stream.getId() == null) {
			if (stream.getMaxlen() == null) {
				return new StreamWriter();
			}
			StreamMaxlenWriter writer = new StreamMaxlenWriter();
			writer.setMaxlen(stream.getMaxlen());
			writer.setApproximateTrimming(stream.isTrim());
			return writer;
		}
		if (stream.getMaxlen() == null) {
			StreamIdWriter writer = new StreamIdWriter();
			writer.setIdField(stream.getId());
			return writer;
		}
		StreamIdMaxlenWriter writer = new StreamIdMaxlenWriter();
		writer.setApproximateTrimming(stream.isTrim());
		writer.setIdField(stream.getId());
		writer.setMaxlen(stream.getMaxlen());
		return writer;
	}

	private ZSetWriter zSetWriter() {
		ZSetWriter writer = new ZSetWriter();
		writer.setScoreField(zset.getScore());
		writer.setDefaultScore(zset.getDefaultScore());
		return writer;
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
		case Geo:
			return geoWriter();
		case List:
			return new ListWriter();
		case Set:
			return new SetWriter();
		case Stream:
			return streamWriter();
		case String:
			return stringWriter();
		case ZSet:
			return zSetWriter();
		default:
			return new HashWriter();
		}
	}

	@Override
	protected String getTargetDescription() {
		return "Redis " + dataStructure.name() + " \"" + keyspaceDescription() + "\"";
	}

}
