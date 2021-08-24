package com.redis.riot.file;

import lombok.Data;
import org.springframework.batch.item.resource.StandardInputResource;
import org.springframework.batch.item.resource.StandardOutputResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.charset.Charset;

@Data
public class FileOptions {

    public static final Charset DEFAULT_ENCODING = Charset.defaultCharset();

    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private Charset encoding = DEFAULT_ENCODING;
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private boolean gzip;
    @ArgGroup(exclusive = false, heading = "Amazon Simple Storage Service options%n")
    private S3Options s3 = new S3Options();
    @ArgGroup(exclusive = false, heading = "Google Cloud Storage options%n")
    private GcsOptions gcs = new GcsOptions();

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

    private Resource resource(String location, boolean readOnly) throws IOException {
        if (FileUtils.isS3(location)) {
            return s3.resource(location);
        }
        if (FileUtils.isGcs(location)) {
            return gcs.resource(location, readOnly);
        }
        if (ResourceUtils.isUrl(location)) {
            return new UncustomizedUrlResource(location);
        }
        return new FileSystemResource(location);
    }

}

