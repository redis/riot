package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spring.autoconfigure.storage.GcpStorageAutoConfiguration;
import com.google.cloud.spring.core.UserAgentHeaderProvider;
import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.StorageOptions;

import io.awspring.cloud.s3.InMemoryBufferingS3OutputStreamProvider;
import io.awspring.cloud.s3.Location;
import io.awspring.cloud.s3.PropertiesS3ObjectContentTypeResolver;
import io.awspring.cloud.s3.S3Resource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public abstract class Files {

	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?:\\.(?<gz>gz))?$");
	public static final String DELIMITER_PIPE = "|";
	public static final String GOOGLE_STORAGE_PROTOCOL_PREFIX = "gs://";
	public static final String STDIN = "-";
	public static final FileReaderRegistry readerRegistry = defaultReaderRegistry();
	public static final FileWriterRegistry writerRegistry = defaultWriterRegistry();

	private Files() {
	}

	public static Resource resource(String location, ResourceOptions options) throws IOException {
		Resource resource = isStdin(location) ? new SystemInResource() : readableResource(location, options);
		// Systematically obtain the input stream to validate the resource
		InputStream inputStream = resource.getInputStream();
		if (options.isGzipped() || Files.isGzip(resource.getFilename())) {
			GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
			return new FilenameInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	private static Resource readableResource(String location, ResourceOptions options) throws IOException {
		if (ResourceUtils.isUrl(location)) {
			return new UncustomizedUrlResource(location);
		}
		return createResource(location, options);
	}

	public static WritableResource writableResource(String location, ResourceOptions options) throws IOException {
		WritableResource resource = location == null ? new SystemOutResource() : createResource(location, options);
		OutputStream outputStream = resource.getOutputStream();
		if (options.isGzipped() || Files.isGzip(resource.getFilename())) {
			GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
			return new OutputStreamResource(gzipOutputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	private static WritableResource createResource(String location, ResourceOptions options) throws IOException {
		if (isAwsStorage(location)) {
			return s3Resource(location, options.getAwsOptions());
		}
		if (isGoogleStorage(location)) {
			return googleStorageResource(location, options.getGcpOptions());
		}
		return new FileSystemResource(location);
	}

	public static boolean isStdin(String file) {
		return STDIN.equals(file);
	}

	public static boolean isFileSystem(String file) {
		return !isAwsStorage(file) && !isGoogleStorage(file) && !ResourceUtils.isUrl(file) && !isStdin(file);
	}

	public static boolean isGoogleStorage(String location) {
		return StringUtils.hasLength(location) && location.toLowerCase().startsWith(GOOGLE_STORAGE_PROTOCOL_PREFIX);
	}

	public static boolean isAwsStorage(String location) {
		return StringUtils.hasLength(location) && location.toLowerCase().startsWith(Location.S3_PROTOCOL_PREFIX);
	}

	public static GoogleStorageResource googleStorageResource(String location, GcpOptions options) throws IOException {
		StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId())
				.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (options.getKeyFile() != null) {
			InputStream inputStream = java.nio.file.Files.newInputStream(options.getKeyFile().toPath());
			builder.setCredentials(GoogleCredentials.fromStream(inputStream).createScoped(options.getScope().getUrl()));
		}
		if (options.getEncodedKey() != null) {
			ByteArrayInputStream stream = new ByteArrayInputStream(Base64.getDecoder().decode(options.getEncodedKey()));
			builder.setCredentials(GoogleCredentials.fromStream(stream));
		}
		options.getProjectId().ifPresent(builder::setProjectId);
		return new GoogleStorageResource(builder.build().getService(), location);
	}

	public static S3Resource s3Resource(String location, AwsOptions options) {
		S3ClientBuilder clientBuilder = S3Client.builder();
		options.getRegion().ifPresent(clientBuilder::region);
		options.getEndpoint().ifPresent(clientBuilder::endpointOverride);
		clientBuilder.credentialsProvider(credentialsProvider(options.getCredentials()));
		S3Client client = clientBuilder.build();
		InMemoryBufferingS3OutputStreamProvider outputStreamProvider = new InMemoryBufferingS3OutputStreamProvider(
				client, new PropertiesS3ObjectContentTypeResolver());
		return S3Resource.create(location, client, outputStreamProvider);
	}

	private static AwsCredentialsProvider credentialsProvider(AwsCredentials credentials) {
		if (credentials != null && StringUtils.hasText(credentials.getAccessKey())
				&& StringUtils.hasText(credentials.getSecretKey())) {
			return StaticCredentialsProvider
					.create(AwsBasicCredentials.create(credentials.getAccessKey(), credentials.getSecretKey()));
		}
		return AnonymousCredentialsProvider.create();
	}

	/**
	 * 
	 * @param file File path that might include a glob pattern
	 * @return List of file
	 * @throws IOException
	 */
	public static Stream<String> expand(String file) throws IOException {
		if (isFileSystem(file)) {
			return expand(Paths.get(file)).stream().map(Path::toString);
		}
		return Stream.of(file);
	}

	public static List<Path> expand(Path path) throws IOException {
		if (java.nio.file.Files.exists(path) || path.getParent() == null
				|| !java.nio.file.Files.exists(path.getParent())) {
			return Arrays.asList(path);
		}
		Path dir = path.getParent();
		String glob = path.getFileName().toString();
		// Path might be glob pattern
		try (DirectoryStream<Path> stream = java.nio.file.Files.newDirectoryStream(dir, glob)) {
			List<Path> paths = new ArrayList<>();
			stream.iterator().forEachRemaining(paths::add);
			return paths;
		}
	}

	public static boolean isGzip(String filename) {
		return extensionGroup(filename, "gz") != null;
	}

	private static String extensionGroup(String file, String group) {
		Matcher matcher = EXTENSION_PATTERN.matcher(file);
		if (matcher.find()) {
			return matcher.group(group);
		}
		return null;
	}

	public static String extension(String filename) {
		return extensionGroup(filename, "extension");
	}

	public static FileReaderRegistry defaultReaderRegistry() {
		FileReaderRegistry registry = new FileReaderRegistry();
		registry.register(FileType.DELIMITED, FileReaderFactories::delimited);
		registry.register(FileType.FIXED_WIDTH, FileReaderFactories::fixedWidth);
		registry.register(FileType.JSON, FileReaderFactories::json);
		registry.register(FileType.JSONL, FileReaderFactories::jsonLines);
		registry.register(FileType.XML, FileReaderFactories::xml);
		return registry;
	}

	private static FileWriterRegistry defaultWriterRegistry() {
		FileWriterRegistry registry = new FileWriterRegistry();
		registry.register(FileType.DELIMITED, FileWriterFactories::delimited);
		registry.register(FileType.FIXED_WIDTH, FileWriterFactories::formatted);
		registry.register(FileType.JSON, FileWriterFactories::json);
		registry.register(FileType.JSONL, FileWriterFactories::jsonLines);
		registry.register(FileType.XML, FileWriterFactories::xml);
		registry.register(null, FileWriterFactories::jsonLines);
		return registry;
	}

	public static String delimiter(Resource resource) {
		String extension = extension(resource.getFilename());
		if (FileType.EXTENSION_CSV.equalsIgnoreCase(extension)) {
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
		if (FileType.EXTENSION_PSV.equalsIgnoreCase(extension)) {
			return DELIMITER_PIPE;
		}
		if (FileType.EXTENSION_TSV.equalsIgnoreCase(extension)) {
			return DelimitedLineTokenizer.DELIMITER_TAB;
		}
		return null;
	}

}
