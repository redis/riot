package com.redis.riot.core;

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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.resource.XmlItemReader;
import com.redis.riot.core.resource.XmlItemReaderBuilder;
import com.redis.riot.core.resource.XmlObjectReader;

public interface FileUtils {

	public static final String GS_URI_PREFIX = "gs://";
	public static final String S3_URI_PREFIX = "s3://";
	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?:\\.(?<gz>gz))?$");

	static boolean isGzip(String file) {
		return extensionGroup(file, "gz").isPresent();
	}

	static FileExtension extension(Resource resource) {
		return extensionGroup(resource.getFilename(), "extension")
				.orElseThrow(() -> new UnknownFileTypeException("Could not determine file extension"));
	}

	static Optional<FileExtension> extensionGroup(String file, String group) {
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

	static boolean isFile(String file) {
		return !(isGcs(file) || isS3(file) || ResourceUtils.isUrl(file) || isConsole(file));
	}

	static boolean isConsole(String file) {
		return "-".equals(file);
	}

	static boolean isS3(String file) {
		return file.startsWith(S3_URI_PREFIX);
	}

	static boolean isGcs(String file) {
		return file.startsWith(GS_URI_PREFIX);
	}

	static <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> clazz) {
		JsonItemReaderBuilder<T> jsonReaderBuilder = new JsonItemReaderBuilder<>();
		jsonReaderBuilder.name(resource.getFilename() + "-json-file-reader");
		jsonReaderBuilder.resource(resource);
		JacksonJsonObjectReader<T> jsonObjectReader = new JacksonJsonObjectReader<>(clazz);
		jsonObjectReader.setMapper(new ObjectMapper());
		jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
		return jsonReaderBuilder.build();
	}

	static <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> clazz) {
		XmlItemReaderBuilder<T> xmlReaderBuilder = new XmlItemReaderBuilder<>();
		xmlReaderBuilder.name(resource.getFilename() + "-xml-file-reader");
		xmlReaderBuilder.resource(resource);
		XmlObjectReader<T> xmlObjectReader = new XmlObjectReader<>(clazz);
		xmlObjectReader.setMapper(new XmlMapper());
		xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
		return xmlReaderBuilder.build();
	}

	/**
	 * 
	 * @param filename Filename that might include a glob pattern
	 * @return List of file
	 * @throws IOException
	 */
	static Stream<String> expand(String filename) throws IOException {
		if (isFile(filename)) {
			return expand(Paths.get(filename)).stream().map(Path::toString);
		}
		return Stream.of(filename);
	}

	static List<Path> expand(Path path) throws IOException {
		if (Files.exists(path) || path.getParent() == null || !Files.exists(path.getParent())) {
			return Arrays.asList(path);
		}
		// Path might be glob pattern
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(path.getParent(), path.getFileName().toString())) {
			List<Path> paths = new ArrayList<>();
			stream.iterator().forEachRemaining(paths::add);
			return paths;
		}
	}

	static Stream<String> expandAll(Iterable<String> files) {
		return StreamSupport.stream(files.spliterator(), false).flatMap(f -> {
			try {
				return FileUtils.expand(f);
			} catch (IOException e) {
				throw new RuntimeIOException(e);
			}
		});
	}

}
