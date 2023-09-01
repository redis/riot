package com.redis.riot.core.file;

import com.redis.riot.core.file.resource.AbstractResourceItemWriter;

public class FileOptions {

    public static final String DEFAULT_ENCODING = AbstractResourceItemWriter.DEFAULT_CHARSET;

    private String encoding = DEFAULT_ENCODING;

    private boolean gzipped;

    private GoogleStorageOptions googleStorageOptions = new GoogleStorageOptions();

    private AmazonS3Options amazonS3Options = new AmazonS3Options();

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

    public GoogleStorageOptions getGoogleStorageOptions() {
        return googleStorageOptions;
    }

    public void setGoogleStorageOptions(GoogleStorageOptions googleStorageOptions) {
        this.googleStorageOptions = googleStorageOptions;
    }

    public AmazonS3Options getAmazonS3Options() {
        return amazonS3Options;
    }

    public void setAmazonS3Options(AmazonS3Options amazonS3Options) {
        this.amazonS3Options = amazonS3Options;
    }

}
