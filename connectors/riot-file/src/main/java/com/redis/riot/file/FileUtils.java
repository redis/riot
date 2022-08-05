package com.redis.riot.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.ObjectUtils;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.riot.file.resource.XmlItemReaderBuilder;
import com.redis.riot.file.resource.XmlObjectReader;

public interface FileUtils {

	public static final String GS_URI_PREFIX = "gs://";
	public static final String S3_URI_PREFIX = "s3://";

	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?:\\.(?<gz>gz))?$");

	public static boolean isGzip(String file) {
		return extensionGroup(file, "gz").isPresent();
	}

	public static Optional<FileExtension> extension(Resource resource) {
		return extensionGroup(resource.getFilename(), "extension");
	}

	public static Optional<FileExtension> extensionGroup(String file, String group) {
		Matcher matcher = EXTENSION_PATTERN.matcher(file);
		if (matcher.find()) {
			String extensionString = matcher.group(group);
			if (extensionString == null) {
				return Optional.empty();
			}
			try {
				return Optional.of(FileExtension.valueOf(extensionString.toUpperCase()));
			} catch (Exception e) {
				// do nothing
			}
			return Optional.empty();
		}
		return Optional.empty();
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

	public static List<String> expand(String file) throws IOException {
		if (isFile(file)) {
			Path path = Paths.get(file);
			if (!Files.exists(path)) {
				List<String> expandedFiles = new ArrayList<>();
				// Path might be glob pattern
				Path parent = path.getParent();
				if (parent != null && Files.exists(parent)) {
					try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent,
							path.getFileName().toString())) {
						stream.forEach(p -> expandedFiles.add(p.toString()));
					}
				}
				if (ObjectUtils.isEmpty(expandedFiles)) {
					throw new FileNotFoundException("File not found: " + file);
				}
			}
		}
		return Arrays.asList(file);
	}

}
