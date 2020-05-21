package com.redislabs.riot.cli;

import com.redislabs.lettusearch.search.Language;
import lombok.Getter;
import picocli.CommandLine;

public class FtAddOptions {

    @Getter
    @CommandLine.Option(names = "--if-condition", description = "Boolean expression for conditional update", paramLabel = "<exp>")
    private String ifCondition;
    @Getter
    @CommandLine.Option(names = "--language", description = "Stemmer to use for indexing: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
    private Language language;
    @Getter
    @CommandLine.Option(names = "--nosave", description = "Do not save docs, only index")
    private boolean nosave;
    @Getter
    @CommandLine.Option(names = "--replace", description = "UPSERT-style insertion")
    private boolean replace;
    @Getter
    @CommandLine.Option(names = "--partial", description = "Partial update (only applicable with replace)")
    private boolean replacePartial;


}
