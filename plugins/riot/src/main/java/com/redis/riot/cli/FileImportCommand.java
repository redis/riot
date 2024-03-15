package com.redis.riot.cli;

import com.redis.riot.core.AbstractImport;
import com.redis.riot.file.FileImport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import from CSV/JSON/XML files.")
public class FileImportCommand extends AbstractImportCommand {

    @ArgGroup(exclusive = false)
    FileImportArgs args = new FileImportArgs();

    @Override
    protected AbstractImport importRunnable() {
        FileImport runnable = new FileImport();
        runnable.setFiles(args.files);
        runnable.setColumnRanges(args.columnRanges);
        runnable.setContinuationString(args.continuationString);
        runnable.setDelimiter(args.delimiter);
        runnable.setFields(args.fields);
        runnable.setFileOptions(args.fileOptions());
        runnable.setFileType(args.fileType);
        runnable.setHeader(args.header);
        runnable.setHeaderLine(args.headerLine);
        runnable.setIncludedFields(args.includedFields);
        runnable.setLinesToSkip(args.linesToSkip);
        runnable.setMaxItemCount(args.maxItemCount);
        runnable.setQuoteCharacter(args.quoteCharacter);
        runnable.setRegexes(args.regexes);
        return runnable;
    }

}
