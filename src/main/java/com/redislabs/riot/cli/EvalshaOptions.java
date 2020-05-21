package com.redislabs.riot.cli;

import io.lettuce.core.ScriptOutputType;
import lombok.*;
import picocli.CommandLine.Option;

public class EvalshaOptions {

    @Getter
    @Option(names = "--args", arity = "1..*", description = "Arg field names", paramLabel = "<fields>")
    private String[] args = new String[0];
    @Getter
    @Option(names = "--sha", description = "Digest", paramLabel = "<sha>")
    private String sha;
    @Getter
    @Option(names = "--output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
    private ScriptOutputType outputType = ScriptOutputType.STATUS;

}
