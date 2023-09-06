package com.redis.riot.cli;

import java.util.ArrayList;
import java.util.List;

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.file.FileImport;
import com.redis.spring.batch.util.BatchUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "file-import", description = "Import from CSV/JSON/XML files.")
public class FileImportCommand extends AbstractImportCommand {

    @Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    List<String> files = new ArrayList<>();

    @ArgGroup(exclusive = false, heading = "File options%n")
    FileImportArgs args = new FileImportArgs();

    @Override
    protected AbstractMapImport getMapImportExecutable() {
        FileImport executable = new FileImport(redisClient(), files);
        executable.setColumnRanges(args.columnRanges);
        executable.setContinuationString(args.continuationString);
        executable.setDelimiter(args.delimiter);
        executable.setFields(args.fields);
        executable.setFileOptions(args.fileOptions());
        executable.setFileType(args.fileType);
        executable.setHeader(args.header);
        executable.setHeaderLine(args.headerLine);
        executable.setIncludedFields(args.includedFields);
        executable.setLinesToSkip(args.linesToSkip);
        executable.setMaxItemCount(args.maxItemCount);
        executable.setQuoteCharacter(args.quoteCharacter);
        return executable;
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.SIZE_UNKNOWN;
    }

}
