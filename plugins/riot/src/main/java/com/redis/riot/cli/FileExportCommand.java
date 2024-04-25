package com.redis.riot.cli;

import com.redis.riot.file.FileDumpExport;
import com.redis.riot.file.FileDumpType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileExportCommand extends AbstractExportCommand {

	@ArgGroup(exclusive = false)
	private FileArgs fileArgs = new FileArgs();

	@Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
	private String file;

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileDumpType type;

	@Option(names = "--append", description = "Append to file if it exists.")
	private boolean append;

	@Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String rootName = FileDumpExport.DEFAULT_ROOT_NAME;

	@Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String elementName = FileDumpExport.DEFAULT_ELEMENT_NAME;

	@Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
	private String lineSeparator = FileDumpExport.DEFAULT_LINE_SEPARATOR;

	@Override
	protected FileDumpExport exportCallable() {
		FileDumpExport callable = new FileDumpExport();
		callable.setFile(file);
		callable.setAppend(append);
		callable.setElementName(elementName);
		callable.setLineSeparator(lineSeparator);
		callable.setRootName(rootName);
		callable.setFileOptions(fileArgs.fileOptions());
		callable.setType(type);
		return callable;
	}

	public FileArgs getFileArgs() {
		return fileArgs;
	}

	public void setFileArgs(FileArgs fileArgs) {
		this.fileArgs = fileArgs;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public FileDumpType getType() {
		return type;
	}

	public void setType(FileDumpType type) {
		this.type = type;
	}

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public String getRootName() {
		return rootName;
	}

	public void setRootName(String rootName) {
		this.rootName = rootName;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String elementName) {
		this.elementName = elementName;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}

}
