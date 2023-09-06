package com.redis.riot.cli;

import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.Option;

public class FileDumpArgs extends FileArgs {

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    FileDumpType type;

}
