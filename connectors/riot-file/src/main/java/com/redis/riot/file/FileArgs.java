package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;

import org.springframework.cloud.gcp.autoconfigure.storage.GcpStorageAutoConfiguration;
import org.springframework.cloud.gcp.core.UserAgentHeaderProvider;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileArgs {

	public static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
	public static final char DEFAULT_QUOTE_CHARACTER = '"';

	@ArgGroup(exclusive = false)
	private AmazonS3Args amazonS3Args = new AmazonS3Args();

	@Option(names = "--delimiter", description = "Delimiter character.", paramLabel = "<string>")
	private String delimiter;

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
	private String encoding = DEFAULT_ENCODING;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@ArgGroup(exclusive = false)
	private GoogleStorageArgs googleStorageArgs = new GoogleStorageArgs();

	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
	private boolean gzipped;

	@Option(names = "--header", description = "Use first line as field names for CSV/fixed-length files")
	private boolean header;

	@Option(names = "--quote", description = "Escape character for CSV files (default: ${DEFAULT-VALUE}).", paramLabel = "<char>")
	private char quoteCharacter = DEFAULT_QUOTE_CHARACTER;

	public FileType fileType(Resource resource) {
		if (fileType == null) {
			FileType type = FileUtils.fileType(resource);
			if (type == null) {
				return FileType.JSONL;
			}
			return type;
		}
		return fileType;
	}

	private GoogleStorageResource googleStorageResource(String location) throws IOException {
		StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId())
				.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (googleStorageArgs.getKeyFile() != null) {
			InputStream inputStream = Files.newInputStream(googleStorageArgs.getKeyFile().toPath());
			builder.setCredentials(
					GoogleCredentials.fromStream(inputStream).createScoped(googleStorageArgs.getScope().getUrl()));
		}
		if (googleStorageArgs.getEncodedKey() != null) {
			ByteArrayInputStream stream = new ByteArrayInputStream(
					Base64.getDecoder().decode(googleStorageArgs.getEncodedKey()));
			builder.setCredentials(GoogleCredentials.fromStream(stream));
		}
		if (googleStorageArgs.getProjectId() != null) {
			builder.setProjectId(googleStorageArgs.getProjectId());
		}
		return new GoogleStorageResource(builder.build().getService(), location);
	}

	public Resource resource(String location) throws IOException {
		if (FileUtils.isAmazonS3(location)) {
			return amazonS3Resource(location);
		}
		if (FileUtils.isGoogleStorage(location)) {
			return googleStorageResource(location);
		}
		if (ResourceUtils.isUrl(location)) {
			return new UncustomizedUrlResource(location);
		}
		return new FileSystemResource(location);
	}

	private Resource amazonS3Resource(String location) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
		if (amazonS3Args.getRegion() != null) {
			clientBuilder.withRegion(amazonS3Args.getRegion());
		}
		if (amazonS3Args.getAccessKey() != null) {
			if (amazonS3Args.getSecretKey() == null) {
				throw new IllegalArgumentException("Amazon S3 secret key not specified");
			}
			BasicAWSCredentials credentials = new BasicAWSCredentials(amazonS3Args.getAccessKey(),
					amazonS3Args.getSecretKey());
			clientBuilder.withCredentials(new AWSStaticCredentialsProvider(credentials));
		}
		AmazonS3ProtocolResolver resolver = new AmazonS3ProtocolResolver(clientBuilder);
		resolver.afterPropertiesSet();
		return resolver.resolve(location, new DefaultResourceLoader());
	}

	public FileType fileType(String file) throws IOException {
		return fileType(resource(file));
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public boolean isGzipped() {
		return gzipped;
	}

	public void setGzipped(boolean gzipped) {
		this.gzipped = gzipped;
	}

	public GoogleStorageArgs getGoogleStorageArgs() {
		return googleStorageArgs;
	}

	public void setGoogleStorageArgs(GoogleStorageArgs args) {
		this.googleStorageArgs = args;
	}

	public AmazonS3Args getAmazonS3Args() {
		return amazonS3Args;
	}

	public void setAmazonS3Args(AmazonS3Args args) {
		this.amazonS3Args = args;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType type) {
		this.fileType = type;
	}

	public char getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(char character) {
		this.quoteCharacter = character;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public boolean isHeader() {
		return header;
	}

	public void setHeader(boolean header) {
		this.header = header;
	}

}
