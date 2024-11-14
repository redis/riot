package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.MimeType;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spring.autoconfigure.storage.GcpStorageAutoConfiguration;
import com.google.cloud.spring.core.GcpScope;
import com.google.cloud.spring.core.UserAgentHeaderProvider;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public abstract class AbstractFactoryRegistry<T, O extends FileOptions> {

	public static final String DELIMITER_PIPE = "|";
	private final Map<MimeType, String> delimiterMap = defaultDelimiterMap();
	private ResourceTypeMap resourceTypeMap = ResourceTypeMap.defaultResourceTypeMap();
	private final Map<MimeType, Factory<T, O>> factories = new HashMap<>();

	public void registerDelimiter(MimeType type, String delimiter) {
		delimiterMap.put(type, delimiter);
	}

	private static Map<MimeType, String> defaultDelimiterMap() {
		Map<MimeType, String> map = new HashMap<>();
		map.put(FileUtils.CSV, DelimitedLineTokenizer.DELIMITER_COMMA);
		map.put(FileUtils.PSV, DELIMITER_PIPE);
		map.put(FileUtils.TSV, DelimitedLineTokenizer.DELIMITER_TAB);
		return map;
	}

	public ResourceTypeMap getResourceTypeMap() {
		return resourceTypeMap;
	}

	public void setResourceTypeMap(ResourceTypeMap map) {
		this.resourceTypeMap = map;
	}

	public void register(MimeType type, Factory<T, O> factory) {
		factories.put(type, factory);
	}

	protected String delimiter(Resource resource, O options) {
		if (options.getDelimiter() == null) {
			return delimiterMap.get(getType(resource.getFilename(), options));
		}
		return options.getDelimiter();
	}

	private Resource normalize(Resource resource, O options) throws IOException {
		if (options.isGzipped() || FileUtils.isGzip(resource.getFilename())) {
			return gzip(resource);
		}
		return resource;
	}

	protected abstract Resource gzip(Resource resource) throws IOException;

	public T get(String location, O options) throws IOException {
		Resource resource = resource(location, options);
		return get(resource, options);
	}

	public Resource resource(String location, O options) {
		return resourceLoader(options).getResource(location);
	}

	protected ResourceLoader resourceLoader(O options) {
		DefaultResourceLoader loader = new DefaultResourceLoader();
		protocolResolvers(options).forEach(loader::addProtocolResolver);
		return loader;
	}

	protected Collection<ProtocolResolver> protocolResolvers(O options) {
		S3ProtocolResolver s3ProtocolResolver = new S3ProtocolResolver();
		s3ProtocolResolver.setClientSupplier(() -> s3Client(options.getS3Options()));
		GoogleStorageProtocolResolver googleStorageProtocolResolver = new GoogleStorageProtocolResolver();
		googleStorageProtocolResolver.setStorageSupplier(() -> googleStorage(options.getGoogleStorageOptions()));
		return Arrays.asList(s3ProtocolResolver, googleStorageProtocolResolver);
	}

	private S3Client s3Client(S3Options options) {
		S3ClientBuilder clientBuilder = S3Client.builder();
		if (options.getRegion() != null) {
			clientBuilder.region(options.getRegion());
		}
		if (options.getEndpoint() != null) {
			clientBuilder.endpointOverride(options.getEndpoint());
		}
		clientBuilder.credentialsProvider(credentialsProvider(options));
		return clientBuilder.build();
	}

	private AwsCredentialsProvider credentialsProvider(S3Options options) {
		if (options.getAccessKey() == null && options.getSecretKey() == null) {
			return AnonymousCredentialsProvider.create();
		}
		return StaticCredentialsProvider
				.create(AwsBasicCredentials.create(options.getAccessKey(), options.getSecretKey()));
	}

	private Storage googleStorage(GoogleStorageOptions options) {
		StorageOptions.Builder builder = StorageOptions.newBuilder();
		builder.setProjectId(ServiceOptions.getDefaultProjectId());
		builder.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (options.getKeyFile() != null) {
			InputStream inputStream;
			try {
				inputStream = Files.newInputStream(options.getKeyFile());
			} catch (IOException e) {
				throw new RuntimeIOException("Could not read key file", e);
			}
			builder.setCredentials(credentials(inputStream, options));
		}
		if (options.getEncodedKey() != null) {
			byte[] bytes = Base64.getDecoder().decode(options.getEncodedKey());
			builder.setCredentials(credentials(new ByteArrayInputStream(bytes), options));
		}
		if (options.getProjectId() != null) {
			builder.setProjectId(options.getProjectId());
		}
		return builder.build().getService();
	}

	private GoogleCredentials credentials(InputStream inputStream, GoogleStorageOptions options) {
		GoogleCredentials credentials;
		try {
			credentials = GoogleCredentials.fromStream(inputStream);
		} catch (IOException e) {
			throw new RuntimeIOException("Could not create Google credentials", e);
		}
		credentials.createScoped(googleStorageScope().getUrl());
		return credentials;
	}

	protected abstract GcpScope googleStorageScope();

	public T get(Resource resource, O options) throws IOException {
		MimeType type = getType(resource.getFilename(), options);
		Factory<T, O> factory = factories.get(type);
		if (factory == null) {
			return null;
		}
		return factory.create(normalize(resource, options), options);
	}

	public MimeType getType(String filename) {
		return resourceTypeMap.getContentType(FileUtils.normalize(filename));
	}

	public MimeType getType(String filename, O options) {
		return getType(filename, options.getType());
	}

	public MimeType getType(String filename, MimeType type) {
		if (type == null) {
			return getType(filename);
		}
		return type;
	}

}
