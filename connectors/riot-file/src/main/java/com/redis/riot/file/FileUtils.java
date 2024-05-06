package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.cloud.gcp.autoconfigure.storage.GcpStorageAutoConfiguration;
import org.springframework.cloud.gcp.core.GcpScope;
import org.springframework.cloud.gcp.core.UserAgentHeaderProvider;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;

public abstract class FileUtils {

	public static final String GS_URI_PREFIX = "gs://";
	public static final String S3_URI_PREFIX = "s3://";
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

	/**
	 * 
	 * @param filename Filename that might include a glob pattern
	 * @return List of file
	 * @throws IOException
	 */
	public static Stream<String> expand(String filename) throws IOException {
		if (isFile(filename)) {
			return expand(Paths.get(filename)).stream().map(Path::toString);
		}
		return Stream.of(filename);
	}

	public static List<Path> expand(Path path) throws IOException {
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

	public static Stream<String> expandAll(Iterable<String> files) {
		return StreamSupport.stream(files.spliterator(), false).flatMap(f -> {
			try {
				return FileUtils.expand(f);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
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

	public static boolean isFile(String file) {
		return !(isGoogleStorageResource(file) || isAmazonS3Resource(file) || ResourceUtils.isUrl(file)
				|| isConsole(file));
	}

	public static boolean isConsole(String file) {
		return "-".equals(file);
	}

	public static boolean isAmazonS3Resource(String file) {
		return file.startsWith(S3_URI_PREFIX);
	}

	public static boolean isGoogleStorageResource(String file) {
		return file.startsWith(GS_URI_PREFIX);
	}

	public static Resource safeInputResource(String file, FileOptions options) {
		try {
			return inputResource(file, options);
		} catch (IOException e) {
			throw new RuntimeException("Could not open file " + file, e);
		}
	}

	public static Resource inputResource(String file, FileOptions options) throws IOException {
		if (FileUtils.isConsole(file)) {
			return new FilenameInputStreamResource(System.in, "stdin", "Standard Input");
		}
		Resource resource = resource(file, true, options);
		resource.getInputStream();
		if (options.isGzipped() || FileUtils.isGzip(file)) {
			GZIPInputStream gzipInputStream = new GZIPInputStream(resource.getInputStream());
			return new FilenameInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	public static WritableResource outputResource(String file, FileOptions options) throws IOException {
		if (FileUtils.isConsole(file)) {
			return new OutputStreamResource(System.out, "stdout", "Standard Output");
		}
		Resource resource = resource(file, false, options);
		Assert.notNull(resource, "Could not resolve file " + file);
		Assert.isInstanceOf(WritableResource.class, resource);
		WritableResource writableResource = (WritableResource) resource;
		if (options.isGzipped() || isGzip(file)) {
			OutputStream outputStream = writableResource.getOutputStream();
			return new OutputStreamResource(new GZIPOutputStream(outputStream), resource.getFilename(),
					resource.getDescription());
		}
		return writableResource;
	}

	private static GoogleStorageResource googleStorageResource(String location, boolean readOnly,
			GoogleStorageOptions options) throws IOException {
		StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId())
				.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (options.getKeyFile() != null) {
			builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(options.getKeyFile().toPath()))
					.createScoped(gcpScope(readOnly).getUrl()));
		}
		if (options.getEncodedKey() != null) {
			builder.setCredentials(GoogleCredentials
					.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(options.getEncodedKey()))));
		}
		if (options.getProjectId() != null) {
			builder.setProjectId(options.getProjectId());
		}
		return new GoogleStorageResource(builder.build().getService(), location);
	}

	private static GcpScope gcpScope(boolean readOnly) {
		if (readOnly) {
			return GcpScope.STORAGE_READ_ONLY;
		}
		return GcpScope.STORAGE_READ_WRITE;
	}

	private static Resource resource(String location, boolean readOnly, FileOptions options) throws IOException {
		if (FileUtils.isAmazonS3Resource(location)) {
			return amazonS3Resource(location, options.getAmazonS3Options());
		}
		if (FileUtils.isGoogleStorageResource(location)) {
			return googleStorageResource(location, readOnly, options.getGoogleStorageOptions());
		}
		if (ResourceUtils.isUrl(location)) {
			return new UncustomizedUrlResource(location);
		}
		return new FileSystemResource(location);
	}

	private static Resource amazonS3Resource(String location, AmazonS3Options options) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
		if (options.getRegion() != null) {
			clientBuilder.withRegion(options.getRegion());
		}
		if (options.getAccessKey() != null) {
			if (options.getSecretKey() == null) {
				throw new IllegalArgumentException("Amazon S3 secret key not specified");
			}
			BasicAWSCredentials credentials = new BasicAWSCredentials(options.getAccessKey(), options.getSecretKey());
			clientBuilder.withCredentials(new AWSStaticCredentialsProvider(credentials));
		}
		AmazonS3ProtocolResolver resolver = new AmazonS3ProtocolResolver(clientBuilder);
		resolver.afterPropertiesSet();
		return resolver.resolve(location, new DefaultResourceLoader());
	}

	public static List<Resource> inputResources(List<String> files, FileOptions fileOptions) {
		return expandAll(files).map(f -> safeInputResource(f, fileOptions)).collect(Collectors.toList());
	}

	public static FileType fileType(Resource resource) {
		String extension = fileExtension(resource);
		if (extension == null) {
			return null;
		}
		switch (extension.toLowerCase()) {
		case FW:
			return FileType.FIXED_LENGTH;
		case JSON:
			return FileType.JSON;
		case JSONL:
			return FileType.JSONL;
		case XML:
			return FileType.XML;
		case CSV:
		case PSV:
		case TSV:
			return FileType.DELIMITED;
		default:
			return null;
		}
	}

	public static String fileExtension(Resource resource) {
		return extensionGroup(resource.getFilename(), "extension");
	}

}
