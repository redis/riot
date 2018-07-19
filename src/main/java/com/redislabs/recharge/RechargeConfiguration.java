package com.redislabs.recharge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

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
	Map<String, FileType> fileTypes;

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
		List<IndexConfiguration> indexes = new ArrayList<>();
		ValueFormat format = ValueFormat.Json;
		FileConfiguration file;
		GeneratorConfiguration generator;
	}

	@Data
	public static class IndexConfiguration {
		IndexType type = IndexType.Set;
		String name;
		String[] fields;
		String score;
		String longitude;
		String latitude;
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

	@Data
	public static class RediSearchConfiguration {
		String index;
		boolean cluster;
		String host;
		String password;
		Integer port;
		Integer timeout;
		Integer poolSize = 100;
	}

}
