package com.redis.riot.cli;

import com.redis.riot.core.AbstractMapImport;
import com.redis.riot.file.FileImport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import from CSV/JSON/XML files.")
public class FileImportCommand extends AbstractImportCommand {

    @ArgGroup(exclusive = false)
    FileImportArgs args = new FileImportArgs();

    @Override
    protected AbstractMapImport getMapImportExecutable() {
        FileImport executable = new FileImport();
        executable.setFiles(args.files);
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
        executable.setRegexes(args.regexes);
        return executable;
    }

}
