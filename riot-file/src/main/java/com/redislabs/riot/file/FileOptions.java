package com.redislabs.riot.file;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;
import lombok.Data;
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
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Base64;

@Data
public class FileOptions {

    public static final Charset DEFAULT_ENCODING = Charset.defaultCharset();

    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private Charset encoding = DEFAULT_ENCODING;
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private boolean gzip;
    @ArgGroup(exclusive = false, heading = "Amazon Simple Storage Service options%n")
    private S3Options s3;
    @ArgGroup(exclusive = false, heading = "Google Cloud Storage options%n")
    private GcsOptions gcs;

    @SuppressWarnings({"unchecked", "FieldCanBeLocal"})
    public static class FileOptionsBuilder<B extends FileOptionsBuilder<B>> {

        protected Charset encoding = DEFAULT_ENCODING;
        protected boolean gzip;
        protected S3Options s3;
        protected GcsOptions gcs;

        public B encoding(Charset encoding) {
            Assert.notNull(encoding, "Encoding must not be null");
            this.encoding = encoding;
            return (B) this;
        }

        public B gzip(boolean gzip) {
            this.gzip = gzip;
            return (B) this;
        }

        public B s3(S3Options s3) {
            this.s3 = s3;
            return (B) this;
        }

        public B gcs(GcsOptions gcs) {
            this.gcs = gcs;
            return (B) this;
        }

        protected <T extends FileOptions> T build(T options) {
            options.setEncoding(encoding);
            options.setGcs(gcs);
            options.setGzip(gzip);
            options.setS3(s3);
            return options;
        }

    }

    public Resource inputResource(String file) throws IOException {
        if (FileUtils.isConsole(file)) {
            return new StandardInputResource();
        }
        Resource resource = resource(file, true);
        if (gzip || FileUtils.isGzip(file)) {
            return new GZIPInputStreamResource(resource.getInputStream(), resource.getDescription());
        }
        return resource;
    }

    public WritableResource outputResource(String file) throws IOException {
        if (FileUtils.isConsole(file)) {
            return new StandardOutputResource();
        }
        Resource resource = resource(file, false);
        Assert.isInstanceOf(WritableResource.class, resource);
        WritableResource writable = (WritableResource) resource;
        if (gzip || FileUtils.isGzip(file)) {
            return new GZIPOutputStreamResource(writable.getOutputStream(), writable.getDescription());
        }
        return writable;
    }

    public Resource s3Resource(String location) {
        AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
        if (s3 != null) {
            if (s3.getRegion() != null) {
                clientBuilder.withRegion(s3.getRegion());
            }
            if (s3.getAccessKey() != null) {
                clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(s3.getAccessKey(), s3.getSecretKey()));
            }
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

    private Resource resource(String location, boolean readOnly) throws IOException {
        if (FileUtils.isS3(location)) {
            return s3Resource(location);
        }
        if (FileUtils.isGcs(location)) {
            return gcsResource(location, readOnly);
        }
        if (ResourceUtils.isUrl(location)) {
            return new UncustomizedUrlResource(location);
        }
        return new FileSystemResource(location);
    }

    private GoogleStorageResource gcsResource(String locationUri, boolean readOnly) throws IOException {
        StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId()).setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
        if (gcs != null) {
            if (gcs.getCredentials() != null) {
                builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(gcs.getCredentials().toPath())).createScoped((readOnly ? GcpScope.STORAGE_READ_ONLY : GcpScope.STORAGE_READ_WRITE).getUrl()));
            }
            if (gcs.getEncodedKey() != null) {
                builder.setCredentials(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(gcs.getEncodedKey()))));
            }
            if (gcs.getProjectId() != null) {
                builder.setProjectId(gcs.getProjectId());
            }
        }
        return new GoogleStorageResource(builder.build().getService(), locationUri);
    }

}

