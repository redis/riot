package com.redislabs.riot.file;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
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
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Getter
public class FileOptions {

    public final static String EXT_CSV = "csv";
    public final static String EXT_TSV = "tsv";
    public final static String EXT_FW = "fw";
    public final static String EXT_JSON = "json";
    public final static String EXT_XML = "xml";
    public final static String GS_URI_PREFIX = "gs://";
    public final static String S3_URI_PREFIX = "s3://";
    private final static Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");
    private static final FileType DEFAULT_FILETYPE = FileType.DELIMITED;

    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private boolean gzip;
    @Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @CommandLine.Option(names = "--structured", description = "Use JSON/XML record structure")
    private boolean structured;
    @Option(names = "--fields", arity = "1..*", description = "Field names", paramLabel = "<names>")
    private String[] names = new String[0];
    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
    @Option(names = {"-h", "--header"}, description = "First line contains field names")
    private boolean header;
    @Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
    private String delimiter;
    @CommandLine.ArgGroup(exclusive = false, heading = "S3 options%n")
    private S3Options s3 = new S3Options();
    @CommandLine.ArgGroup(exclusive = false, heading = "GCS options%n")
    private GcsOptions gcs = new GcsOptions();

    public FileType fileType(String file) {
        String extension = extension(file);
        for (FileType type : FileType.values()) {
            for (String typeExtension : type.getExtensions()) {
                if (typeExtension.equals(extension)) {
                    return type;
                }
            }
        }
        return DEFAULT_FILETYPE;
    }

    public String delimiter(String file) {
        if (delimiter == null) {
            String extension = extension(file);
            if (extension != null) {
                switch (extension) {
                    case EXT_TSV:
                        return DelimitedLineTokenizer.DELIMITER_TAB;
                    case EXT_CSV:
                        return DelimitedLineTokenizer.DELIMITER_COMMA;
                }
            }
            return DelimitedLineTokenizer.DELIMITER_COMMA;
        }
        return delimiter;
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

    public String fileName(Resource resource) {
        if (resource.isFile()) {
            return resource.getFilename();
        }
        try {
            String path = resource.getURI().getPath();
            int cut = path.lastIndexOf('/');
            if (cut == -1) {
                return path;
            }
            return path.substring(cut + 1);
        } catch (IOException e) {
            log.debug("Could not get resource URI", e);
            return "unknown";
        }
    }

    public boolean isFile(String file) {
        return !(isGcs(file) || isS3(file) || ResourceUtils.isUrl(file));
    }

    private Resource resource(String file, boolean readOnly) throws IOException {
        if (isGcs(file)) {
            return new GoogleStorageResource(gcsStorage(readOnly), file);
        }
        if (isS3(file)) {
            AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
            if (s3.getRegion() != null) {
                clientBuilder.withRegion(s3.getRegion());
            }
            if (s3.getCredentials() != null) {
                clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(s3.getCredentials().getAccessKey(), s3.getCredentials().getSecretKey()));
            }
            SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver(clientBuilder.build());
            resolver.afterPropertiesSet();
            return resolver.resolve(file, new DefaultResourceLoader());
        }
        if (ResourceUtils.isUrl(file)) {
            return new UncustomizedUrlResource(file);
        }
        return new FileSystemResource(file);
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
            builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(gcs.getCredentials().toPath())).createScoped(gcsScope(readOnly).getUrl()));
        }
        if (gcs.getEncodedKey() != null) {
            builder.setCredentials(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(gcs.getEncodedKey()))));
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

    public String extension(String file) {
        return extensionGroup(file, "extension");
    }

    public String extensionGroup(String file, String group) {
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
