package com.redislabs.recharge;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import io.redisearch.Schema.FieldType;
import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {

	int maxThreads = 10;
	int chunkSize = 50;
	int maxItemCount = 1000000;
	Map<String, EntityConfiguration> entities = new LinkedHashMap<>();
	Map<String, FileType> fileTypes = new LinkedHashMap<>();

	@Data
	public static class GeneratorConfiguration {
		Map<String, String> fields = new LinkedHashMap<>();
		String locale = "en-US";
	}

	@Data
	public static class EntityConfiguration {
		String[] keys;
		DataType type = DataType.Hash;
		String[] fields;
		Map<String, IndexConfiguration> indexes = new LinkedHashMap<>();
		ValueFormat format = ValueFormat.Json;
		FileConfiguration file;
		GeneratorConfiguration generator;
	}

	@Data
	public static class IndexConfiguration {
		IndexType type = IndexType.Set;
		String[] fields;
		String score;
		String longitude;
		String latitude;
		boolean cluster;
		String host;
		String password;
		Integer port;
		int timeout = 500;
		int poolSize = 100;
		Map<String, RediSearchField> schema = new LinkedHashMap<>();
	}

	@Data
	public static class RediSearchField {
		FieldType type;
		boolean sortable;
		boolean noIndex;
	}

	public static enum IndexType {
		Set, Zset, List, Geo, Search
	}

	public static enum DataType {
		Nil, String, Hash
	}

	public static enum ValueFormat {
		Xml, Json
	}

	@Data
	public static class FileConfiguration {
		String path;
		Boolean gzip;
		String encoding;
		Integer linesToSkip;
		FileType type;
		String delimiter;
		Integer[] includedFields;
		Character quoteCharacter;
		String[] ranges;
		Boolean strict;
	}

	public static enum FileType {
		Xml, Json, Delimited, FixedLength
	}

}
