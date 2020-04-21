package com.redislabs.riot.cli.file;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingFormatArgumentException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.redislabs.riot.file.StandardInputResource;
import com.redislabs.riot.file.StandardOutputResource;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import javax.lang.model.type.UnknownTypeException;

@Slf4j
public class FileOptions {

    private final static Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");

    static class PathOptions {
        @Option(names = {"-f", "--file"}, description = "Path to local file")
        private File file;
        @Option(names = "--url", description = "URL of a file")
        private URI uri;
    }

    @ArgGroup(exclusive = true, multiplicity = "1")
    private PathOptions path = new PathOptions();
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private boolean gzip;
    @Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @Option(names = "--s3-access", description = "AWS S3 access key ID", paramLabel = "<string>")
    private String accessKey;
    @Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
    private String secretKey;
    @Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
    private String region;
    @Option(names = "--fields", arity = "1..*", description = "Field names", paramLabel = "<names>")
    private String[] names = new String[0];
    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
    @Option(names = {"-h", "--header"}, description = "First line contains field names")
    private boolean header;
    @Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
    private String delimiter;

    protected String getEncoding() {
        return encoding;
    }

    protected boolean isHeader() {
        return header;
    }

    protected String getDelimiter() {
        if (delimiter == null) {
            switch (getType()) {
                case TSV:
                    return DelimitedLineTokenizer.DELIMITER_TAB;
                default:
                    return DelimitedLineTokenizer.DELIMITER_COMMA;
            }
        }
        return delimiter;
    }

    protected String[] getNames() {
        return names;
    }

    protected Resource getInputResource() throws IOException {
        if (isConsoleResource()) {
            return new StandardInputResource();
        }
        return getResource();
    }

    private boolean isConsoleResource() {
        return path.file != null && path.file.equals("-");
    }

    protected Resource getOutputResource() throws IOException {
        if (isConsoleResource()) {
            return new StandardOutputResource();
        }
        return getResource();
    }

    private Resource getResource() throws IOException {
        if (path.file != null) {
            return new FileSystemResource(path.file);
        }
        if (path.uri != null) {
            if ("s3".equals(path.uri.getScheme())) {
                return S3ResourceBuilder.resource(accessKey, secretKey, region, path.uri);
            }
            return new UncustomizedUrlResource(path.uri);
        }
        throw new IllegalArgumentException("No file or URL specified");
    }

    protected boolean isGzip() {
        if (gzip) {
            return true;
        }
        Matcher matcher = EXTENSION_PATTERN.matcher(filename());
        if (matcher.find()) {
            return matcher.group("gz") != null;
        }
        return false;
    }

    private String filename() {
        if (path.file != null) {
            return path.file.getName();
        }
        if (path.uri != null) {
            return path.uri.toString();
        }
        throw new IllegalArgumentException("No file or URL specified");
    }

    public boolean isSet() {
        return path.file != null || path.uri != null;
    }

    protected FileType getType() {
        if (type == null) {
            String filename = filename();
            Matcher matcher = EXTENSION_PATTERN.matcher(filename);
            if (matcher.find()) {
                String extension = matcher.group("extension");
                for (FileType type : FileType.values()) {
                    if (type.getExtension().equals(extension)) {
                        return type;
                    }
                }
            }
            log.info("Unknown file type, defaulting to CSV");
            return FileType.CSV;
        }
        return type;
    }

}
