package com.redislabs.riot.file;

import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.nio.charset.Charset;

@Data
public class FileOptions {

    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private String encoding = Charset.defaultCharset().name();
    @Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private boolean gzip;
    @ArgGroup(exclusive = false, heading = "Amazon Simple Storage Service options%n")
    private S3Options s3 = new S3Options();
    @ArgGroup(exclusive = false, heading = "Google Cloud Storage options%n")
    private GcsOptions gcs = new GcsOptions();

}

