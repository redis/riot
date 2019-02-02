package com.redislabs.recharge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.search.field.Matcher;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {

	private boolean concurrent;
	private boolean flushall;
	private long flushallWait = 5000;
	private boolean meter;
	private Map<String, FlowConfiguration> flows;
	private Map<String, FileType> fileTypes = new LinkedHashMap<String, FileType>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6218223585260809771L;

		{
			put("dat", FileType.Delimited);
			put("csv", FileType.Delimited);
			put("txt", FileType.FixedLength);
		}
	};

	@Data
	public static class FlowConfiguration {
		private int chunkSize = 50;
		private int maxItemCount = 10000;
		private int partitions = 1;
		private ReaderConfiguration reader = new ReaderConfiguration();
		private ProcessorConfiguration processor = new ProcessorConfiguration();
		private List<WriterConfiguration> writers = new ArrayList<>();
	}

	@Data
	public static class ReaderConfiguration {
		private GeneratorReaderConfiguration generator;
		private FileReaderConfiguration file;
	}

	public static enum ReaderType {
		Generator, File
	}

	@Data
	public static class ProcessorConfiguration {
		private String source;
		private String merge;
		private Map<String, String> fields = new LinkedHashMap<>();
	}

	@Data
	public static class FileReaderConfiguration {
		private String path;
		private Boolean gzip;
		private FileType type;
		private DelimitedFileConfiguration delimited = new DelimitedFileConfiguration();
		private FixedLengthFileConfiguration fixedLength = new FixedLengthFileConfiguration();
		private JsonFileConfiguration json = new JsonFileConfiguration();
	}

	@Data
	public static class JsonFileConfiguration {
		private String key;
	}

	@Data
	public static class FlatFileConfiguration {
		private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
		private boolean header = false;
		private int linesToSkip = 0;
		private String[] fields = new String[0];
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class DelimitedFileConfiguration extends FlatFileConfiguration {
		private String delimiter;
		private Integer[] includedFields;
		private Character quoteCharacter;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class FixedLengthFileConfiguration extends FlatFileConfiguration {
		private String[] ranges = new String[0];
		private Boolean strict;
	}

	@Data
	public static class GeneratorReaderConfiguration {
		private String map;
		private Map<String, String> fields = new LinkedHashMap<>();
		private String locale = "en-US";
	}

	@Data
	public static class WriterConfiguration {
		private WriterType type = WriterType.Redis;
		private RedisWriterConfiguration redis = new RedisWriterConfiguration();
	}

	public static enum WriterType {
		Redis
	}

	@Data
	public static class RedisWriterConfiguration {
		private String keyspace;
		private String[] keys;
		private RedisType type;
		private NilConfiguration nil;
		private StringConfiguration string;
		private HashConfiguration hash;
		private ListConfiguration list;
		private SetConfiguration set;
		private ZSetConfiguration zset;
		private GeoConfiguration geo;
		private SearchConfiguration search;
		private SuggestConfiguration suggest;
		private StreamConfiguration stream;
	}

	public static enum RedisType {
		nil, string, hash, list, set, zset, geo, search, suggest, stream
	}

	@Data
	public static class NilConfiguration {
		private long sleepInMillis = 0;
	}

	@Data
	public static class SuggestConfiguration {
		private String field;
		private String score;
		private double defaultScore = 1d;
		private boolean increment;
	}

	@Data
	public static class StreamConfiguration {
		private boolean approximateTrimming;
		private String id;
		private Long maxlen;
	}

	public static enum Command {
		Nil, Set, SAdd, HMSet, HIncrBy, GeoAdd, FtAdd, FtSugAdd, LPush, Zadd
	}

	@Data
	public static class HashConfiguration {
		private String[] includeFields;
		private HIncrByConfiguration incrby;
	}

	@Data
	public static class HIncrByConfiguration {
		private String sourceField;
		private String targetField;
	}

	@Data
	public static class StringConfiguration {
		private XmlConfiguration xml;
		private JsonConfiguration json;
	}

	@Data
	public static class XmlConfiguration {
		private String rootName;
	}

	@Data
	public static class JsonConfiguration {
		private String dummy;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class ZSetConfiguration extends CollectionRedisWriterConfiguration {
		private String score;
		private double defaultScore = 1d;
	}

	@Data
	public static class SearchConfiguration {
		private boolean drop = false;
		private boolean create = true;
		private List<RediSearchField> schema = new ArrayList<>();
		private String language;
		private String score;
		private double defaultScore = 1d;
		private boolean replace;
		private boolean noSave;
	}

	@Data
	public static class SearchCommandConfiguration {
		private String language;
		private String score;
		private double defaultScore = 1d;
		private boolean noSave;
	}

	@Data
	public static class CollectionRedisWriterConfiguration {
		private String[] fields;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class GeoConfiguration extends CollectionRedisWriterConfiguration {
		private String longitude;
		private String latitude;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class ListConfiguration extends CollectionRedisWriterConfiguration {
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class SetConfiguration extends CollectionRedisWriterConfiguration {

	}

	@Data
	public static class RediSearchField {
		private String name;
		private RediSearchFieldType type = RediSearchFieldType.Text;
		private boolean sortable;
		private boolean noIndex;
		private Double weight;
		private boolean noStem;
		private Matcher matcher;
	}

	public static enum RediSearchFieldType {
		Text, Numeric, Geo
	}

	public static enum IndexType {
		Set, Zset, List, Geo, Search, Suggestion
	}

	public static enum FileType {
		Xml, Json, Delimited, FixedLength
	}

}
