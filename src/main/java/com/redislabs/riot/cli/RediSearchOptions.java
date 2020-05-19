package com.redislabs.riot.cli;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine.Option;

import java.util.List;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RediSearchOptions {

    @Option(names = {"-i", "--index"}, description = "Name of the RediSearch index", paramLabel = "<name>")
    private String index;
    @Option(names = "--nosave", description = "Do not save docs, only index")
    private boolean noSave;
    @Option(names = "--replace", description = "UPSERT-style insertion")
    private boolean replace;
    @Option(names = "--partial", description = "Partial update (only applicable with replace)")
    private boolean partial;
    @Option(names = "--language", description = "Stemmer to use for indexing: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
    private Language language;
    @Option(names = "--if-condition", description = "Boolean expression for conditional update", paramLabel = "<exp>")
    private String ifCondition;
    @Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
    private String payload;
    @Option(names = "--suggest", description = "Field containing the suggestion", paramLabel = "<field>")
    private String suggest;
    @Option(names = "--increment", description = "Use increment to set value")
    private boolean increment;
    @Option(names = "--query", description = "RediSearch query", paramLabel = "<string>")
    private String query;
    @Option(names = "--options", arity = "1..*", description = "Search/aggregate options", paramLabel = "<string>")
    private List<String> options;

    public boolean hasPayload() {
        return payload != null;
    }

    public AddOptions getAddOptions() {
        return AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave).replace(replace)
                .replacePartial(partial).build();
    }


}
