package com.redislabs.riot;

import com.redislabs.lettusearch.search.Language;
import lombok.Getter;
import picocli.CommandLine;

public class RediSearchImportOptions extends RediSearchOptions {

    @Getter
    @CommandLine.Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
    private String payloadField;
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
    @CommandLine.Option(names = "--partial", description = "Partial update (only applicable with replace)")
    private boolean replacePartial;
    @Getter
    @CommandLine.Option(names = "--replace", description = "UPSERT-style insertion")
    private boolean replace;
    @Getter
    @CommandLine.Option(names = "--sug-field", description = "Field containing suggestion", paramLabel = "<field>")
    private String field;
    @Getter
    @CommandLine.Option(names = "--sug-incr", description = "Increment the score")
    private boolean increment;


}
