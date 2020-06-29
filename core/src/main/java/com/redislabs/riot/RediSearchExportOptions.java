package com.redislabs.riot;

import com.redislabs.lettusearch.search.Limit;
import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RediSearchExportOptions extends RediSearchOptions {

    public enum RediSearchReader {
        SEARCH, AGGREGATE, CURSOR, SUGGEST
    }

    @CommandLine.Option(names = "--redisearch-reader", description = "RediSearch reader: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private RediSearchReader reader = RediSearchReader.SEARCH;
    @CommandLine.Option(names = "--query", description = "Search/suggest query (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
    private String query = "*";
    @CommandLine.Option(names = "--ft-args", arity = "1..*", description = "Args for SEARCH and AGGREGATE commands", paramLabel = "<str>")
    private Object[] args;
    @CommandLine.Option(names = "--fuzzy", description = "Fuzzy suggestions")
    private boolean fuzzy;
    @CommandLine.Option(names = "--with-scores", description = "Include scores")
    private boolean withScores;
    @CommandLine.Option(names = "--with-payloads", description = "Include payloads")
    private boolean withPayloads;
    @CommandLine.Option(names = "--max-results", description = "Max results (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private long max = Limit.DEFAULT_NUM;
}
