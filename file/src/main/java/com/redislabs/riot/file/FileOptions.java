package com.redislabs.riot.file;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import lombok.Getter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ResourceUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileOptions {

    public final static String EXT_CSV = "csv";
    public final static String EXT_TSV = "tsv";
    public final static String EXT_FW = "fw";
    public final static String EXT_JSON = "json";
    public final static String EXT_XML = "xml";

    public enum FileType {

        DELIMITED(EXT_CSV, EXT_TSV), FIXED(EXT_FW), JSON(EXT_JSON), XML(EXT_XML);

        private final String[] extensions;

        FileType(String... extensions) {
            this.extensions = extensions;
        }

        public String[] getExtensions() {
            return extensions;
        }
    }


    private final static Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");
    private static final FileType DEFAULT_FILETYPE = FileType.DELIMITED;


    @CommandLine.Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    private String file;
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private Boolean gzip;
    @Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @Option(names = "--s3-access", description = "AWS S3 access key ID", paramLabel = "<string>")
    private String accessKey;
    @Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
    private String secretKey;
    @Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
    private String region;
    @Getter
    @Option(names = "--fields", arity = "1..*", description = "Field names", paramLabel = "<names>")
    private String[] names = new String[0];
    @Getter
    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
    @Getter
    @Option(names = {"-h", "--header"}, description = "First line contains field names")
    private boolean header;
    @Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
    private String delimiter;

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
        if (delimiter == null) {
            String extension = getExtension();
            if (extension != null) {
                switch (extension) {
                    case FileOptions.EXT_TSV:
                        return DelimitedLineTokenizer.DELIMITER_TAB;
                    case FileOptions.EXT_CSV:
                        return DelimitedLineTokenizer.DELIMITER_COMMA;
                }
            }
            return DelimitedLineTokenizer.DELIMITER_COMMA;
        }
        return delimiter;
    }

    public Resource getResource() throws IOException {
        if (ResourceUtils.isUrl(file)) {
            URI uri = URI.create(file);
            if ("s3".equals(uri.getScheme())) {
                SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver(AmazonS3Client.builder().withCredentials(new SimpleAWSCredentialsProvider()).withRegion(region).build());
                resolver.afterPropertiesSet();
                return resolver.resolve(uri.toString(), new DefaultResourceLoader());
            }
            return new UncustomizedUrlResource(uri);
        }
        return new FileSystemResource(file);
    }

    private class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

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
        if (gzip == null) {
            return getExtensionGroup("gz") != null;
        }
        return gzip;
    }

    public String getExtension() {
        return getExtensionGroup("extension");
    }

    public String getExtensionGroup(String group) {
        Matcher matcher = EXTENSION_PATTERN.matcher(file);
        if (matcher.find()) {
            return matcher.group(group);
        }
        return null;
    }

    public boolean isConsole() {
        return "-".equals(file);
    }
}
