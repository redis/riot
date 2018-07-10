package com.redislabs.recharge;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RechargeConfiguration {

	Connector connector;
	int maxThreads = 1;
	int chunkSize = 50;
	int maxItemCount = 1000;
	KeyConfiguration key = new KeyConfiguration();
	FileConfiguration file = new FileConfiguration();
	GeneratorConfiguration generator = new GeneratorConfiguration();
	List<DataType> datatypes = new ArrayList<>();
	GeoConfiguration geo = new GeoConfiguration();
	ListConfiguration list = new ListConfiguration();
	RediSearchConfiguration redisearch = new RediSearchConfiguration();
	SetConfiguration set = new SetConfiguration();
	ZSetConfiguration zset = new ZSetConfiguration();
	StringConfiguration string = new StringConfiguration();
	XmlConfiguration xml = new XmlConfiguration();

	public static enum Connector {
		Delimited, FixedLength, Generator, Dummy
	}

	public static enum DataType {
		String, List, Set, Hash, ZSet, Geo, RediSearchIndex
	}

	@Data
	public static class KeyConfiguration {
		String prefix;
		String separator = ":";
		String[] fields;
	}

	public static enum StringFormat {
		Json, Xml
	}

	@Data
	public static class XmlConfiguration {
		String rootName = "root";
	}

	@Data
	public static class StringConfiguration {
		StringFormat format = StringFormat.Json;
	}

	@Data
	public static class ListConfiguration {
		KeyConfiguration key = new KeyConfiguration();
	}

	@Data
	public static class GeoConfiguration {
		String xField;
		String yField;
		KeyConfiguration key = new KeyConfiguration();
	}

	@Data
	public static class SetConfiguration {
		KeyConfiguration key = new KeyConfiguration();
	}

	@Data
	public static class ZSetConfiguration {
		String score;
		KeyConfiguration key = new KeyConfiguration();
	}

	@Data
	public static abstract class FlatFileConfiguration {
		Integer linesToSkip;
		String[] fields;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class FixedLengthConfiguration extends FlatFileConfiguration {
		String[] ranges;
		Boolean strict;
	}

	@Data
	@EqualsAndHashCode(callSuper = true)
	public static class DelimitedConfiguration extends FlatFileConfiguration {
		String delimiter;
		Integer[] includedFields;
		Character quoteCharacter;
	}

	@Data
	public static class GeneratorConfiguration {
		Map<String, String> fields = new LinkedHashMap<>();
		String locale;

		public boolean isEnabled() {
			return !fields.isEmpty();
		}
	}

	@Data
	public static class FileConfiguration {

		private Pattern filePathPattern = Pattern.compile("(?<basename>.+)\\.(?<extension>\\w+)(?<gz>\\.gz)?");

		String path;
		Boolean gzip;
		String encoding;
		DelimitedConfiguration delimited = new DelimitedConfiguration();
		FixedLengthConfiguration fixedLength = new FixedLengthConfiguration();

		public Boolean getGzip() {
			if (gzip == null) {
				String gz = getRegexGroupFromFilename("gz");
				return gz != null && gz.length() > 0;
			}
			return gzip;
		}

		public boolean isEnabled() {
			return path != null && path.length() > 0;
		}

		private String getRegexGroupFromFilename(String groupName) {
			Matcher matcher = filePathPattern.matcher(new java.io.File(path).getName());
			if (matcher.find()) {
				return matcher.group(groupName);
			}
			return null;
		}

		public Resource getResource() throws IOException {
			Resource resource = getResource(path);
			if (Boolean.TRUE.equals(getGzip())) {
				return getGZipResource(resource);
			}
			return resource;
		}

		private Resource getGZipResource(Resource resource) throws IOException {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}

		private Resource getResource(String path) throws MalformedURLException {
			if (ResourceUtils.isUrl(path)) {
				return new UrlResource(path);
			}
			return new FileSystemResource(path);
		}

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
