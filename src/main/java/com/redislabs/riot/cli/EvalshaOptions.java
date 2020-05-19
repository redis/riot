package com.redislabs.riot.cli;

import io.lettuce.core.ScriptOutputType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine.Option;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EvalshaOptions {

    @Builder.Default
    @Option(names = "--args", arity = "1..*", description = "EVALSHA arg field names", paramLabel = "<fields>")
    private String[] args = new String[0];
    @Option(names = "--sha", description = "EVALSHA digest", paramLabel = "<sha>")
    private String sha;
    @Builder.Default
    @Option(names = "--output", description = "EVALSHA output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
    private ScriptOutputType outputType = ScriptOutputType.STATUS;

}
