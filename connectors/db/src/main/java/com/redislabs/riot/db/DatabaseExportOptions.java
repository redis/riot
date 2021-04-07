package com.redislabs.riot.db;

import lombok.*;
import picocli.CommandLine;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseExportOptions {

    @Builder.Default
    @CommandLine.Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String keyRegex = "\\w+:(?<id>.+)";
    @Builder.Default
    @CommandLine.Option(names = "--no-assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true)
    private boolean assertUpdates = true;
}
