package com.redislabs.riot.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.nio.charset.Charset;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileOptions {

    @Builder.Default
    @Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
    private String encoding = Charset.defaultCharset().name();
    @Option(names = {"-t", "--type"}, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
    private FileType type;
    @Option(names = {"-z", "--gzip"}, description = "File is gzip compressed")
    private boolean gzip;
    @Builder.Default
    @ArgGroup(exclusive = false, heading = "Amazon Simple Storage Service options%n")
    private S3Options s3 = S3Options.builder().build();
    @Builder.Default
    @ArgGroup(exclusive = false, heading = "Google Cloud Storage options%n")
    private GcsOptions gcs = GcsOptions.builder().build();

    public FileType type(String file) {
        if (type == null) {
            String fileExtension = FileUtils.extension(file);
            for (FileType type : FileType.values()) {
                if (type.getExtension().equals(fileExtension)) {
                    return type;
                }
            }
            return FileType.CSV;
        }
        return type;
    }

}

