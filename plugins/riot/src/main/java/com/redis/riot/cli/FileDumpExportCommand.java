package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileDumpExportCommand extends AbstractExportCommand {

	@ArgGroup(exclusive = false)
	FileDumpExportArgs args = new FileDumpExportArgs();

	@Override
	protected FileDumpExport exportRunnable() {
		FileDumpExport runnable = new FileDumpExport();
		runnable.setFile(args.file);
		runnable.setAppend(args.append);
		runnable.setElementName(args.elementName);
		runnable.setLineSeparator(args.lineSeparator);
		runnable.setRootName(args.rootName);
		runnable.setFileOptions(args.fileOptions());
		runnable.setType(args.type);
		return runnable;
	}

}
