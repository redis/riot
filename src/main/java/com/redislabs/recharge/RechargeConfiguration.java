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
	int maxItemCount = 1000;
	Map<String, EntityConfiguration> entities = new LinkedHashMap<>();
	Map<String, FileType> fileTypes;

	@Data
	public static class GeneratorEntityConfiguration {
		Map<String, String> fields = new LinkedHashMap<>();
		String locale = "en-US";
	}

	@Data
	public static class EntityConfiguration {
		List<String> keys = new ArrayList<>();
		DataType type = DataType.Hash;
		List<String> fields = new ArrayList<>();
		List<IndexConfiguration> indexes = new ArrayList<>();
		ValueFormat format = ValueFormat.Json;
		XmlConfiguration xml = new XmlConfiguration();
		FileEntityConfiguration file;
		GeneratorEntityConfiguration generator;
	}

	@Data
	public static class IndexConfiguration {
		IndexType type = IndexType.Set;
		String field;
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
	public static class XmlConfiguration {
		String rootName = "root";
	}

	@Data
	public static class FixedLengthConfiguration {
		String[] ranges;
		Boolean strict;
	}

	@Data
	public static class DelimitedConfiguration {
		String delimiter;
		Integer[] includedFields;
		Character quoteCharacter;
	}

	@Data
	public static class FileEntityConfiguration {
		String path;
		Boolean gzip;
		String encoding;
		Integer linesToSkip;
		FileType type;
		DelimitedConfiguration delimited = new DelimitedConfiguration();
		FixedLengthConfiguration fixedLength = new FixedLengthConfiguration();
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
