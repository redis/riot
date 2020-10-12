package com.redislabs.riot.file;

import lombok.Getter;
import picocli.CommandLine;

import java.io.File;

@Getter
public class GcsOptions {

    @CommandLine.Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json)", paramLabel = "<file>")
    private File credentials;
    @CommandLine.Option(names = "--gcs-project-id", description ="GCP project id", paramLabel = "<string>")
    private String projectId;
    @CommandLine.Option(names="--gcs-encoded-key", description="GCS Base64 encoded key", paramLabel="<string>")
    private String encodedKey;

}
