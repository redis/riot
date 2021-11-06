package com.redis.riot.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public interface FileUtils {

	public static final String GS_URI_PREFIX = "gs://";
	public static final String S3_URI_PREFIX = "s3://";

	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");
	public static final String EXTENSION_CSV = "csv";
	public static final String EXTENSION_TSV = "tsv";
	public static final String EXTENSION_PSV = "psv";
	public static final String EXTENSION_FW = "fw";
	public static final String EXTENSION_JSON = "json";
	public static final String EXTENSION_XML = "xml";

	public static boolean isGzip(String file) {
		return extensionGroup(file, "gz") != null;
	}

	public static String extension(String file) {
		return extensionGroup(file, "extension");
	}

	public static String extensionGroup(String file, String group) {
		Matcher matcher = EXTENSION_PATTERN.matcher(file);
		if (matcher.find()) {
			return matcher.group(group);
		}
		return null;
	}

	public static boolean isFile(String file) {
		return !(isGcs(file) || isS3(file) || ResourceUtils.isUrl(file) || isConsole(file));
	}

	public static boolean isConsole(String file) {
		return "-".equals(file);
	}

	public static boolean isS3(String file) {
		return file.startsWith(S3_URI_PREFIX);
	}

	public static boolean isGcs(String file) {
		return file.startsWith(GS_URI_PREFIX);
	}

	public static <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> clazz) {
		JsonItemReaderBuilder<T> jsonReaderBuilder = new JsonItemReaderBuilder<>();
		jsonReaderBuilder.name(resource.getFilename() + "-json-file-reader");
		jsonReaderBuilder.resource(resource);
		JacksonJsonObjectReader<T> jsonObjectReader = new JacksonJsonObjectReader<>(clazz);
		jsonObjectReader.setMapper(new ObjectMapper());
		jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
		return jsonReaderBuilder.build();
	}

	public static <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> clazz) {
		XmlItemReaderBuilder<T> xmlReaderBuilder = new XmlItemReaderBuilder<>();
		xmlReaderBuilder.name(resource.getFilename() + "-xml-file-reader");
		xmlReaderBuilder.resource(resource);
		XmlObjectReader<T> xmlObjectReader = new XmlObjectReader<>(clazz);
		xmlObjectReader.setMapper(new XmlMapper());
		xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
		return xmlReaderBuilder.build();
	}

	public static List<String> expand(List<String> files) throws IOException {
		if (files == null) {
			return Collections.emptyList();
		}
		List<String> expandedFiles = new ArrayList<>();
		for (String file : files) {
			if (isFile(file)) {
				Path path = Paths.get(file);
				if (Files.exists(path)) {
					expandedFiles.add(file);
				} else {
					// Path might be glob pattern
					Path parent = path.getParent();
					if (parent != null && Files.exists(parent)) {
						try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent,
								path.getFileName().toString())) {
							stream.forEach(p -> expandedFiles.add(p.toString()));
						}
					}
				}
			} else {
				expandedFiles.add(file);
			}
		}
		return expandedFiles;
	}

}
