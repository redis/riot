package com.redislabs.riot.cli;

import lombok.Getter;
import picocli.CommandLine;

public class SetOptions {

    public enum StringFormat {
        RAW, XML, JSON
    }

    @Getter
    @CommandLine.Option(names = "--format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
    private StringFormat format = StringFormat.JSON;
    @Getter
    @CommandLine.Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
    private String root;
    @Getter
    @CommandLine.Option(names = "--value", description = "String raw value field", paramLabel = "<field>")
    private String value;
}
