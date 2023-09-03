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
    private List<String> files = new ArrayList<>();

    @ArgGroup(exclusive = false, heading = "File options%n")
    private FileImportArgs fileImportArgs = new FileImportArgs();

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    @Override
    protected AbstractMapImport getMapImportExecutable() {
        FileImport executable = new FileImport(redisClient(), files);
        executable.setColumnRanges(fileImportArgs.getColumnRanges());
        executable.setContinuationString(fileImportArgs.getContinuationString());
        executable.setDelimiter(fileImportArgs.getDelimiter());
        executable.setFields(fileImportArgs.getFields());
        executable.setFileOptions(fileImportArgs.fileOptions());
        executable.setFileType(fileImportArgs.getFileType());
        executable.setHeader(fileImportArgs.isHeader());
        executable.setHeaderLine(fileImportArgs.getHeaderLine());
        executable.setIncludedFields(fileImportArgs.getIncludedFields());
        executable.setLinesToSkip(fileImportArgs.getLinesToSkip());
        executable.setMaxItemCount(fileImportArgs.getMaxItemCount());
        executable.setQuoteCharacter(fileImportArgs.getQuoteCharacter());
        return executable;
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        return BatchUtils.SIZE_UNKNOWN;
    }

}
