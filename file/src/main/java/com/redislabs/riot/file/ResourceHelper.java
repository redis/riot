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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResourceHelper {

    public final static String EXT_CSV = "csv";
    public final static String EXT_TSV = "tsv";
    public final static String EXT_FW = "fw";
    public final static String EXT_JSON = "json";
    public final static String EXT_XML = "xml";

    public final static String GS_URI_PREFIX = "gs://";
    public final static String S3_URI_PREFIX = "s3://";

    private final static Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");
    private static final FileType DEFAULT_FILETYPE = FileType.DELIMITED;

    private final FileOptions fileOptions;

    public ResourceHelper(FileOptions fileOptions) {
        this.fileOptions = fileOptions;
    }

    public FileType getFileType() {
        String extension = getExtension();
        for (FileType type : FileType.values()) {
            for (String typeExtension : type.getExtensions()) {
                if (typeExtension.equals(extension)) {
                    return type;
                }
            }
        }
        return DEFAULT_FILETYPE;
    }

    public String getDelimiter() {
        if (fileOptions.getDelimiter() == null) {
            String extension = getExtension();
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
        return fileOptions.getDelimiter();
    }

    public Resource getInputResource() throws IOException {
        if (isConsole()) {
            return new StandardInputResource();
        }
        Resource resource = getResource(true);
        if (isGzip()) {
            return new GZIPInputStreamResource(resource.getInputStream(), resource.getDescription());
        }
        return resource;
    }

    public WritableResource getOutputResource() throws IOException {
        if (isConsole()) {
            return new StandardOutputResource();
        }
        Resource resource = getResource(false);
        Assert.isInstanceOf(WritableResource.class, resource);
        WritableResource writable = (WritableResource) resource;
        if (isGzip()) {
            return new GZIPOutputStreamResource(writable.getOutputStream(), writable.getDescription());
        }
        return writable;
    }

    private Resource getResource(boolean readOnly) throws IOException {
        if (fileOptions.getFile().startsWith(GS_URI_PREFIX)) {
            return new GoogleStorageResource(gcsStorage(readOnly), fileOptions.getFile());
        }
        if (fileOptions.getFile().startsWith(S3_URI_PREFIX)) {
            AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
            if (fileOptions.getS3().getRegion() != null) {
                clientBuilder.withRegion(fileOptions.getS3().getRegion());
            }
            if (fileOptions.getS3().getCredentials() != null) {
                clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(fileOptions.getS3().getCredentials().getAccessKey(), fileOptions.getS3().getCredentials().getSecretKey()));
            }
            SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver(clientBuilder.build());
            resolver.afterPropertiesSet();
            return resolver.resolve(fileOptions.getFile(), new DefaultResourceLoader());
        }
        if (ResourceUtils.isUrl(fileOptions.getFile())) {
            return new UncustomizedUrlResource(fileOptions.getFile());
        }
        return new FileSystemResource(fileOptions.getFile());
    }

    private Storage gcsStorage(boolean readOnly) throws IOException {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (fileOptions.getGcs().getCredentials() != null) {
            builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(fileOptions.getGcs().getCredentials().toPath())).createScoped(gcsScope(readOnly).getUrl()));
        }
        if (fileOptions.getGcs().getEncodedKey() != null) {
            builder.setCredentials(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(fileOptions.getGcs().getEncodedKey()))));
        }
        builder.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
        builder.setProjectId(gcsProjectId());
        return builder.build().getService();
    }

    private String gcsProjectId() {
        if (fileOptions.getGcs().getProjectId() == null) {
            return ServiceOptions.getDefaultProjectId();
        }
        return fileOptions.getGcs().getProjectId();
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

    public boolean isGzip() {
        if (fileOptions.getGzip() == null) {
            return getExtensionGroup("gz") != null;
        }
        return fileOptions.getGzip();
    }

    public String getExtension() {
        return getExtensionGroup("extension");
    }

    public String getExtensionGroup(String group) {
        Matcher matcher = EXTENSION_PATTERN.matcher(fileOptions.getFile());
        if (matcher.find()) {
            return matcher.group(group);
        }
        return null;
    }

    public boolean isConsole() {
        return "-".equals(fileOptions.getFile());
    }
}
