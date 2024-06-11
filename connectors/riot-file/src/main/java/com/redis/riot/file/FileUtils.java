package com.redis.riot.file;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;

public abstract class FileUtils {

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

	public static boolean isGzip(String file) {
		return extensionGroup(file, "gz") != null;
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
	 * @param file Filename that might include a glob pattern
	 * @return List of file
	 * @throws IOException
	 */
	public static Stream<String> expand(String file) {
		if (isFile(file)) {
			return expand(Paths.get(file)).stream().map(Path::toString);
		}
		return Stream.of(file);
	}

	public static boolean isStdin(String file) {
		return "-".equals(file);
	}

	public static boolean isFile(String file) {
		return !AmazonS3Args.isSimpleStorageResource(file) && !GoogleStorageArgs.isGoogleStorageResource(file)
				&& !ResourceUtils.isUrl(file) && !isStdin(file);
	}

	public static List<Path> expand(Path path) {
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
		} catch (IOException e) {
			throw new RuntimeIOException(
					MessageFormat.format("Could not list files in directory {0} with glob pattern {1}", dir, glob), e);
		}
	}

}
