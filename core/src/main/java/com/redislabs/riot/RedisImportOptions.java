package com.redislabs.riot;

import com.redislabs.riot.convert.KeyMaker;
import io.lettuce.core.ScriptOutputType;
import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RedisImportOptions {

    public enum StringFormat {
        RAW, XML, JSON
    }

    @CommandLine.Option(names = "--key-separator", description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String keySeparator = KeyMaker.DEFAULT_SEPARATOR;
    @CommandLine.Option(names = {"-p", "--keyspace"}, description = "Keyspace prefix", paramLabel = "<str>")
    private String keyspace;
    @CommandLine.Option(names = {"-k", "--keys"}, arity = "1..*", description = "Key fields", paramLabel = "<fields>")
    private String[] keyFields = new String[0];
    @CommandLine.Option(names = "--member-space", description = "Prefix for member IDs", paramLabel = "<str>")
    private String memberSpace;
    @CommandLine.Option(names = "--members", arity = "1..*", description = "Member field names for collections", paramLabel = "<fields>")
    private String[] memberFields = new String[0];
    @CommandLine.Option(names = "--eval-sha", description = "Digest", paramLabel = "<sha>")
    private String evalSha;
    @CommandLine.Option(names = "--eval-args", arity = "1..*", description = "EVAL arg field names", paramLabel = "<names>")
    private String[] evalArgs = new String[0];
    @CommandLine.Option(names = "--eval-output", description = "Output: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<type>")
    private ScriptOutputType evalOutputType = ScriptOutputType.STATUS;
    @CommandLine.Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @CommandLine.Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;
    @CommandLine.Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
    private String field;
    @CommandLine.Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
    private double defaultValue = 1d;
    @CommandLine.Option(names = "--string-format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
    private StringFormat format = StringFormat.JSON;
    @CommandLine.Option(names = "--string-raw", description = "String raw value field", paramLabel = "<field>")
    private String valueField;
    @CommandLine.Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
    private long defaultTimeout = 60;
    @CommandLine.Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
    private String timeout;
    @CommandLine.Option(names = "--xadd-id", description = "Stream entry ID field", paramLabel = "<field>")
    private String idField;
    @CommandLine.Option(names = "--xadd-maxlen", description = "Stream maxlen", paramLabel = "<int>")
    private Long maxlen;
    @CommandLine.Option(names = "--xadd-trim", description = "Stream efficient trimming (~ flag)")
    private boolean approximateTrimming;
    @CommandLine.Option(names = "--xml-root", description = "XML root element name", paramLabel = "<name>")
    private String root;

}
