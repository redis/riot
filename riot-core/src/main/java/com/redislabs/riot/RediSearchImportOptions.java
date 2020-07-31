package com.redislabs.riot;

import com.redislabs.lettusearch.search.Language;
import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RediSearchImportOptions extends RediSearchOptions {

    @CommandLine.Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
    private String payloadField;
    @CommandLine.Option(names = "--if-condition", description = "Boolean expression for conditional update", paramLabel = "<exp>")
    private String ifCondition;
    @CommandLine.Option(names = "--language", description = "Stemmer to use for indexing: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
    private Language language;
    @CommandLine.Option(names = "--nosave", description = "Do not save docs, only index")
    private boolean nosave;
    @CommandLine.Option(names = "--partial", description = "Partial update (only applicable with replace)")
    private boolean replacePartial;
    @CommandLine.Option(names = "--replace", description = "UPSERT-style insertion")
    private boolean replace;
    @CommandLine.Option(names = "--sug-field", description = "Field containing suggestion", paramLabel = "<field>")
    private String field;
    @CommandLine.Option(names = "--sug-incr", description = "Increment the score")
    private boolean increment;

}
