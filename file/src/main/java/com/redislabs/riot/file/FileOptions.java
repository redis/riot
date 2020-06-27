package com.redislabs.riot.file;

import lombok.Getter;
import org.springframework.batch.item.file.FlatFileItemReader;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Getter
public class FileOptions {

    @CommandLine.Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
    private String file;
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private Boolean gzip;
    @Option(names = {"-t", "--filetype"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
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

}
