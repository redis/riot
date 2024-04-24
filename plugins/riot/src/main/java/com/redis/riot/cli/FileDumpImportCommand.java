package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.file.FileDumpImport;
import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "dump-import", description = "Import Redis data files into Redis.")
public class FileDumpImportCommand extends AbstractStructImportCommand {

	@ArgGroup(exclusive = false)
	private FileArgs fileArgs = new FileArgs();

	@Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
	private List<String> files;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileDumpType type;

	@Override
	protected FileDumpImport importRunnable() {
		FileDumpImport runnable = new FileDumpImport();
		runnable.setFiles(files);
		runnable.setFileOptions(fileArgs.fileOptions());
		runnable.setType(type);
		return runnable;
	}

	@Override
	protected String taskName(String stepName) {
		return "Importing";
	}

	public List<String> getFiles() {
		return files;
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public FileDumpType getType() {
		return type;
	}

	public void setType(FileDumpType type) {
		this.type = type;
	}

}
