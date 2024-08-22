package com.redis.riot.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import io.awspring.cloud.s3.Location;

public abstract class FileUtils {

	public static final String GOOGLE_STORAGE_PROTOCOL_PREFIX = "gs://";
	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?:\\.(?<gz>gz))?$");

	public static final String CSV = "csv";
	public static final String TSV = "tsv";
	public static final String PSV = "psv";
	public static final String FW = "fw";
	public static final String JSON = "json";
	public static final String JSONL = "jsonl";
	public static final String XML = "xml";

	private FileUtils() {
	}

	private static String extensionGroup(String file, String group) {
		Matcher matcher = EXTENSION_PATTERN.matcher(file);
		if (matcher.find()) {
			return matcher.group(group);
		}
		return null;
	}

	public static FileType fileType(Resource resource) {
		String extension = fileExtension(resource);
		if (extension == null) {
			return null;
		}
		switch (extension.toLowerCase()) {
		case FW:
			return FileType.FIXED;
		case JSON:
			return FileType.JSON;
		case JSONL:
			return FileType.JSONL;
		case XML:
			return FileType.XML;
		case CSV:
		case PSV:
		case TSV:
			return FileType.CSV;
		default:
			return null;
		}
	}

	public static String fileExtension(Resource resource) {
		return extensionGroup(resource.getFilename(), "extension");
	}

	/**
	 * 
	 * @param file File path that might include a glob pattern
	 * @return List of file
	 * @throws IOException
	 */
	public static List<String> expand(String file) throws IOException {
		if (isFileSystem(file)) {
			return expand(Paths.get(file)).stream().map(Path::toString).collect(Collectors.toList());
		}
		return Arrays.asList(file);
	}

	public static boolean isFileSystem(String file) {
		return !isAwsStorage(file) && !isGoogleStorage(file) && !ResourceUtils.isUrl(file) && !isStdin(file);
	}

	public static List<Path> expand(Path path) throws IOException {
		if (Files.exists(path) || path.getParent() == null || !Files.exists(path.getParent())) {
			return Arrays.asList(path);
		}
		Path dir = path.getParent();
		String glob = path.getFileName().toString();
		// Path might be glob pattern
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
			List<Path> paths = new ArrayList<>();
			stream.iterator().forEachRemaining(paths::add);
			return paths;
		}
	}

	public static boolean isGzip(String file) {
		return extensionGroup(file, "gz") != null;
	}

	public static boolean isStdin(String file) {
		return "-".equals(file);
	}

	public static boolean isGoogleStorage(String location) {
		return StringUtils.hasLength(location) && location.toLowerCase().startsWith(GOOGLE_STORAGE_PROTOCOL_PREFIX);
	}

	public static boolean isAwsStorage(String location) {
		return StringUtils.hasLength(location) && location.toLowerCase().startsWith(Location.S3_PROTOCOL_PREFIX);
	}

}
