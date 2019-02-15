package com.redislabs.recharge;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.redislabs.lettusearch.search.field.Matcher;
import com.redislabs.recharge.file.FileConfiguration;
import com.redislabs.recharge.generator.GeneratorConfiguration;
import com.redislabs.recharge.processor.SpelProcessorConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {
	private boolean meter;
	private boolean flushall;
	private int flushallWait = 5;
	private List<FlowConfiguration> flows = new ArrayList<>();

	@Data
	public static class FlowConfiguration {
		private String name;
		private int chunkSize = 50;
		private int maxItemCount = 10000;
		private int partitions = 1;
		private GeneratorConfiguration generator;
		private FileConfiguration file;
		private SpelProcessorConfiguration processor;
		private List<RedisWriterConfiguration> redis = new ArrayList<>();
	}

	@Data
	public static class RedisWriterConfiguration {
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

	@Data
	public static class AbstractRedisConfiguration {
		private String keyspace;
		private String[] keys;
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

	public static enum RedisType {
		nil, string, hash, list, set, zset, geo, search, suggest, stream
	}

	@Data
	public static class NilConfiguration {
		private long sleepInMillis = 0;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class SuggestConfiguration extends AbstractRedisConfiguration {
		private String field;
		private String score;
		private double defaultScore = 1d;
		private boolean increment;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class StreamConfiguration extends AbstractRedisConfiguration {
		private boolean approximateTrimming;
		private String id;
		private Long maxlen;
	}

	public static enum Command {
		Nil, Set, SAdd, HMSet, HIncrBy, GeoAdd, FtAdd, FtSugAdd, LPush, Zadd
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class HashConfiguration extends AbstractRedisConfiguration {
		private String[] includeFields;
		private HIncrByConfiguration incrby;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class HIncrByConfiguration extends AbstractRedisConfiguration {
		private String sourceField;
		private String targetField;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class StringConfiguration extends AbstractRedisConfiguration {
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
	@EqualsAndHashCode(callSuper = true)
	public static class SearchConfiguration extends AbstractRedisConfiguration {
		private boolean drop = false;
		private boolean create = true;
		private List<RediSearchField> schema = new ArrayList<>();
		private String language;
		private String score;
		private double defaultScore = 1d;
		private boolean replace;
		private boolean replacePartial;
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
	@EqualsAndHashCode(callSuper = true)
	private abstract static class CollectionRedisWriterConfiguration extends AbstractRedisConfiguration {
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