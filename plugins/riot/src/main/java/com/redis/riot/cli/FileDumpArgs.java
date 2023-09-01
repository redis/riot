package com.redis.riot.cli;

import com.redis.riot.core.file.FileDumpType;

import picocli.CommandLine.Option;

public class FileDumpArgs extends FileArgs {

    @Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
    private FileDumpType type;

    public FileDumpType getType() {
        return type;
    }

    public void setType(FileDumpType type) {
        this.type = type;
    }

}
