package com.redislabs.riot.cli;

import io.lettuce.core.ScriptOutputType;
import lombok.Getter;
import picocli.CommandLine;

public class RedisCommandOptions {

    public enum StringFormat {
        RAW, XML, JSON
    }

    @Getter
    @CommandLine.Option(names = "--eval-sha", description = "Digest", paramLabel = "<sha>")
    private String evalSha;
    @Getter
    @CommandLine.Option(names = "--eval-args", arity = "1..*", description = "EVAL arg field names", paramLabel = "<names>")
    private String[] evalArgs = new String[0];
    @Getter
    @CommandLine.Option(names = "--eval-output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
    private ScriptOutputType evalOutputType = ScriptOutputType.STATUS;
    @Getter
    @CommandLine.Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @Getter
    @CommandLine.Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;
    @Getter
    @CommandLine.Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String field;
    @Getter
    @CommandLine.Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double defaultValue = 1d;
    @Getter
    @CommandLine.Option(names = "--string-format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
    private StringFormat format = StringFormat.JSON;
    @Getter
    @CommandLine.Option(names = "--string-raw", description = "String raw value field", paramLabel = "<field>")
    private String valueField;
    @Getter
    @CommandLine.Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long defaultTimeout = 60;
    @Getter
    @CommandLine.Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeout;
    @Getter
    @CommandLine.Option(names = "--xadd-id", description = "Stream entry ID field", paramLabel = "<field>")
    private String idField;
    @Getter
    @CommandLine.Option(names = "--xadd-maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @Getter
    @CommandLine.Option(names = "--xadd-trim", description = "Stream efficient trimming (~ flag)")
    private boolean approximateTrimming;
    @Getter
    @CommandLine.Option(names = "--xml-root", description = "XML root element name", paramLabel = "<name>")
    private String root;




}
