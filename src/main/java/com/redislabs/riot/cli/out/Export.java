package com.redislabs.riot.cli.out;

import java.util.Map;

import com.redislabs.riot.cli.JobCommand;
import com.redislabs.riot.cli.out.file.DelimitedFileExport;
import com.redislabs.riot.cli.out.file.FixedLengthFileExport;
import com.redislabs.riot.cli.out.file.JsonFileExport;

import lombok.Getter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "export", description = "Export from Redis", subcommands = { DelimitedFileExport.class,
		FixedLengthFileExport.class, JsonFileExport.class, DatabaseExport.class })
public class Export extends JobCommand<Map<String, Object>, Map<String, Object>> {

	@Getter
	@Option(names = "--max", description = "Maximum number of entries to read.", paramLabel = "<count>")
	private Integer max;
	@Getter
	@Option(names = "--count", description = "Number of elements to return for each scan call.")
	private Integer scanCount;
	@Getter
	@Option(names = "--keyspace", description = "Redis keyspace prefix.")
	private String keyspace;
	@Getter
	@Option(names = "--keys", arity = "1..*", description = "Key fields.")
	private String[] keys = new String[0];
	@Getter
	@Option(names = "--key-separator", description = "Redis key separator. (default: ${DEFAULT-VALUE}).")
	private String separator = ":";

}
