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

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {

	boolean concurrent = true;
	boolean flushall = false;
	List<EntityConfiguration> entities = new ArrayList<>();
	Map<String, FileType> fileTypes = new LinkedHashMap<>();

	@Data
	public static class EntityConfiguration {
		String name;
		int chunkSize = 50;
		String[] keys;
		Command command = new Command();
		String[] fields;
		int maxItemCount = 10000;
		int maxThreads = 1;
		List<IndexConfiguration> indexes = new ArrayList<>();
		ValueFormat format = ValueFormat.Json;
		String file;
		FileConfiguration fileConfig;
		Map<String, String> generator = new LinkedHashMap<>();
		String fakerLocale = "en-US";
	}

	@Data
	public static class IndexConfiguration {
		String name;
		IndexType type = IndexType.Set;
		String[] keys;
		String[] fields;
		String score;
		String longitude;
		String latitude;
		List<RediSearchField> schemaFields = new ArrayList<>();
		boolean drop;
		String suggestion;
	}

	@Data
	public static class RediSearchField {
		String name;
		RediSearchFieldType type;
		boolean sortable;
		boolean noIndex;
	}

	public static enum RediSearchFieldType {
		Text, Numeric, Geo
	}

	public static enum IndexType {
		Set, Zset, List, Geo, Search, Suggestion
	}

	@Data
	public static class Command {
		CommandType type = CommandType.Hmset;
		String sourceField = "delta";
		String targetField = "onhand";
	}

	public static enum CommandType {
		Nil, Set, Hmset, Hincrby
	}

	public static enum ValueFormat {
		Xml, Json
	}

	@Data
	public static class FileConfiguration {
		Boolean gzip;
		String encoding = FlatFileItemReader.DEFAULT_CHARSET;
		boolean header = false;
		int linesToSkip = 0;
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
