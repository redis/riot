package com.redislabs.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.item.resource.StandardInputResource;
import org.springframework.batch.item.resource.StandardOutputResource;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import lombok.Getter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileOptions {

    public final static String EXT_CSV = "csv";
    public final static String EXT_TSV = "tsv";
    public final static String EXT_FW = "fw";
    public final static String EXT_JSON = "json";
    public final static String EXT_XML = "xml";
    public final static String GS_URI_PREFIX = "gs://";
    public final static String S3_URI_PREFIX = "s3://";
    private final static Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
    private boolean gzip;
    @Getter
    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private String encoding = Charset.defaultCharset().name();
    @ArgGroup(exclusive = false, heading = "S3 options%n")
    private S3Options s3 = new S3Options();
    @ArgGroup(exclusive = false, heading = "GCS options%n")
    private GcsOptions gcs = new GcsOptions();

    public String fileName(Resource resource) throws IOException {
	if (resource instanceof StandardInputResource) {
	    return "stdin";
	}
	if (resource instanceof StandardOutputResource) {
	    return "stdout";
	}
	if (resource.isFile()) {
	    return resource.getFilename();
	}
	String path = resource.getURI().getPath();
	int cut = path.lastIndexOf('/');
	if (cut == -1) {
	    return path;
	}
	return path.substring(cut + 1);
    }

    public FileType fileType(String file) {
	if (type == null) {
	    String extension = extension(file);
	    for (FileType type : FileType.values()) {
		for (String typeExtension : type.getExtensions()) {
		    if (typeExtension.equals(extension)) {
			return type;
		    }
		}
	    }
	    return FileType.DELIMITED;
	}
	return type;
    }

    public Resource inputResource(String file) throws IOException {
	if (isConsole(file)) {
	    return new StandardInputResource();
	}
	Resource resource = resource(file, true);
	if (isGzip(file)) {
	    return new GZIPInputStreamResource(resource.getInputStream(), resource.getDescription());
	}
	return resource;
    }

    public WritableResource outputResource(String file) throws IOException {
	if (isConsole(file)) {
	    return new StandardOutputResource();
	}
	Resource resource = resource(file, false);
	Assert.isInstanceOf(WritableResource.class, resource);
	WritableResource writable = (WritableResource) resource;
	if (isGzip(file)) {
	    return new GZIPOutputStreamResource(writable.getOutputStream(), writable.getDescription());
	}
	return writable;
    }

    public boolean isFile(String file) {
	return !(isGcs(file) || isS3(file) || ResourceUtils.isUrl(file) || isConsole(file));
    }

    private Resource resource(String location, boolean readOnly) throws IOException {
	if (isGcs(location)) {
	    return new GoogleStorageResource(gcsStorage(readOnly), location);
	}
	if (isS3(location)) {
	    AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
	    if (s3.getRegion() != null) {
		clientBuilder.withRegion(s3.getRegion());
	    }
	    if (s3.getCredentials() != null) {
		clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(s3.getCredentials().getAccessKey(),
			s3.getCredentials().getSecretKey()));
	    }
	    SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver() {
		@Override
		public AmazonS3 getAmazonS3() {
		    return clientBuilder.build();
		}
	    };
	    resolver.afterPropertiesSet();
	    return resolver.resolve(location, new DefaultResourceLoader());
	}
	if (ResourceUtils.isUrl(location)) {
	    return new UncustomizedUrlResource(location);
	}
	return new FileSystemResource(location);
    }

    private boolean isS3(String file) {
	return file.startsWith(S3_URI_PREFIX);
    }

    private boolean isGcs(String file) {
	return file.startsWith(GS_URI_PREFIX);
    }

    private Storage gcsStorage(boolean readOnly) throws IOException {
	StorageOptions.Builder builder = StorageOptions.newBuilder();
	if (gcs.getCredentials() != null) {
	    builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(gcs.getCredentials().toPath()))
		    .createScoped(gcsScope(readOnly).getUrl()));
	}
	if (gcs.getEncodedKey() != null) {
	    builder.setCredentials(GoogleCredentials
		    .fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(gcs.getEncodedKey()))));
	}
	builder.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
	builder.setProjectId(gcsProjectId());
	return builder.build().getService();
    }

    private String gcsProjectId() {
	if (gcs.getProjectId() == null) {
	    return ServiceOptions.getDefaultProjectId();
	}
	return gcs.getProjectId();
    }

    private GcpScope gcsScope(boolean readOnly) {
	if (readOnly) {
	    return GcpScope.STORAGE_READ_ONLY;
	}
	return GcpScope.STORAGE_READ_WRITE;
    }

    private static class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

	private final String accessKey;
	private final String secretKey;

	private SimpleAWSCredentialsProvider(String accessKey, String secretKey) {
	    this.accessKey = accessKey;
	    this.secretKey = secretKey;
	}

	@Override
	public AWSCredentials getCredentials() {
	    return new BasicAWSCredentials(accessKey, secretKey);
	}

	@Override
	public void refresh() {
	    // do nothing
	}

    }

    public boolean isGzip(String file) {
	if (gzip) {
	    return true;
	}
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

    public boolean isConsole(String file) {
	return "-".equals(file);
    }

}
