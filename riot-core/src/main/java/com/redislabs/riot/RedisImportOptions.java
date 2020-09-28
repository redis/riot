package com.redislabs.riot;

import io.lettuce.core.ScriptOutputType;
import lombok.Getter;
import picocli.CommandLine;

@Getter
public class RedisImportOptions {

	public enum StringFormat {
		RAW, XML, JSON
	}

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
	private String scoreField;
	@CommandLine.Option(names = "--score-default", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double scoreDefault = 1d;
	@CommandLine.Option(names = "--string-format", description = "Serialization: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<fmt>")
	private StringFormat stringFormat = StringFormat.JSON;
	@CommandLine.Option(names = "--string-field", description = "String value field", paramLabel = "<field>")
	private String stringField;
	@CommandLine.Option(names = "--ttl", description = "EXPIRE timeout field", paramLabel = "<field>")
	private String timeoutField;
	@CommandLine.Option(names = "--ttl-default", description = "EXPIRE default timeout (default: ${DEFAULT-VALUE})", paramLabel = "<sec>")
	private long timeoutDefault = 60;
	@CommandLine.Option(names = "--xadd-id", description = "Stream entry ID field", paramLabel = "<field>")
	private String xaddIdField;
	@CommandLine.Option(names = "--xadd-maxlen", description = "Stream maxlen", paramLabel = "<int>")
	private Long xaddMaxlen;
	@CommandLine.Option(names = "--xadd-trim", description = "Stream efficient trimming ('~' flag)")
	private boolean xaddTrim;
	@CommandLine.Option(names = "--root", description = "XML root element name", paramLabel = "<name>")
	private String xmlRoot;

}
