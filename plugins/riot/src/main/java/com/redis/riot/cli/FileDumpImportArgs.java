package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FileDumpImportArgs extends FileArgs {

    @Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    List<String> files;

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileDumpType type;

}
