package com.redislabs.recharge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {

	boolean concurrent = true;
	boolean flushall = false;
	long flushallWait = 5000;
	List<FlowConfiguration> flows = new ArrayList<>();
	Map<String, FileType> fileTypes = new LinkedHashMap<>();

	@Data
	public static class FlowConfiguration {
		int maxThreads = 1;
		int chunkSize = 50;
		int maxItemCount = 10000;
		GeneratorReaderConfiguration generator;
		FileReaderConfiguration file;
		ProcessorConfiguration processor;
		List<WriterConfiguration> writers;
	}

	@Data
	public static class ProcessorConfiguration {
		String map;
		Map<String, String> fields;
	}

	@Data
	public static class FileReaderConfiguration {
		String path;
		Boolean gzip;
		DelimitedFileConfiguration delimited;
		FixedLengthFileConfiguration fixedLength;
		JsonFileConfiguration json;
	}

	@Data
	public static class JsonFileConfiguration {
		String key;
	}

	@Data
	public static class FlatFileConfiguration {
		String encoding = FlatFileItemReader.DEFAULT_CHARSET;
		boolean header = false;
		int linesToSkip = 0;
		String[] fields = new String[0];
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class DelimitedFileConfiguration extends FlatFileConfiguration {
		String delimiter;
		Integer[] includedFields;
		Character quoteCharacter;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class FixedLengthFileConfiguration extends FlatFileConfiguration {
		String[] ranges = new String[0];
		Boolean strict;
	}

	@Data
	public static class GeneratorReaderConfiguration {
		Map<String, String> fields = new LinkedHashMap<>();
		String locale = "en-US";
	}

	@Data
	public static class WriterConfiguration {
		RedisWriterConfiguration redis;
	}

	@Data
	public static class RedisWriterConfiguration {
		String keyspace;
		String[] keys;
		GeoConfiguration geo;
		SearchConfiguration search;
		SuggestConfiguration suggest;
		ZSetConfiguration zset;
		StringConfiguration string;
		PushConfiguration list;
		SetConfiguration set;
		HashConfiguration hash;
		NilConfiguration nil;
	}

	@Data
	public static class NilConfiguration {
		long sleepInMillis = 0;
	}

	@Data
	public static class SuggestConfiguration {
		String field;
		String score;
		double defaultScore = 1d;
		boolean increment;
	}

	public static enum Command {
		Nil, Set, SAdd, HMSet, HIncrBy, GeoAdd, FtAdd, FtSugAdd, LPush, Zadd
	}

	@Data
	public static class HashConfiguration {
		String[] includeFields;
		HIncrByConfiguration incrby;
	}

	@Data
	public static class HIncrByConfiguration {
		String sourceField;
		String targetField;
	}

	@Data
	public static class StringConfiguration {
		XmlConfiguration xml;
		JsonConfiguration json;
	}

	@Data
	public static class XmlConfiguration {
		String rootName;
	}

	@Data
	public static class JsonConfiguration {
		String dummy;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class ZSetConfiguration extends CollectionRedisWriterConfiguration {
		String score;
		double defaultScore = 1d;
	}

	@Data
	public static class SearchConfiguration {
		String index;
		boolean drop;
		boolean create;
		List<RediSearchField> schema = new ArrayList<>();
		String language;
		String score;
		double defaultScore = 1d;
		boolean replace;
		boolean noSave;
		SearchAddCommand command = SearchAddCommand.Add;
	}

	@Data
	public static class SearchCommandConfiguration {
		String language;
		String score;
		double defaultScore = 1d;
		boolean noSave;
	}

	public static enum SearchAddCommand {
		Add, AddHash
	}

	@Data
	public static class CollectionRedisWriterConfiguration {
		String[] fields;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class GeoConfiguration extends CollectionRedisWriterConfiguration {
		String longitude;
		String latitude;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class PushConfiguration extends CollectionRedisWriterConfiguration {
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class SetConfiguration extends CollectionRedisWriterConfiguration {

	}

	@Data
	public static class RediSearchField {
		String name;
		RediSearchFieldType type = RediSearchFieldType.Text;
		boolean sortable;
		boolean noIndex;
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
