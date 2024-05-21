package com.redis.riot.cli;

import com.redis.riot.file.ContentType;
import com.redis.riot.file.FileExport;
import com.redis.riot.file.FileType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "file-export", description = "Export Redis data to JSON or XML files.")
public class FileExportCommand extends AbstractExportCommand {

	@Parameters(arity = "1", description = "File path or URL", paramLabel = "FILE")
	private String file;

	@ArgGroup(exclusive = false)
	private FileArgs fileArgs = new FileArgs();

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileType fileType;

	@Option(names = "--dump", description = "Sets the exported content type to Redis (key, ttl, type, value).")
	private boolean dump;

	@Option(names = "--append", description = "Append to file if it exists.")
	private boolean append;

	@Option(names = "--root", description = "XML root element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String rootName = FileExport.DEFAULT_ROOT_NAME;

	@Option(names = "--element", description = "XML element tag name (default: ${DEFAULT-VALUE}).", paramLabel = "<string>")
	private String elementName = FileExport.DEFAULT_ELEMENT_NAME;

	@Option(names = "--line-sep", description = "String to separate lines (default: system default).", paramLabel = "<string>")
	private String lineSeparator = FileExport.DEFAULT_LINE_SEPARATOR;

	@ArgGroup(exclusive = false)
	private KeyValueMapProcessorArgs mapProcessorArgs = new KeyValueMapProcessorArgs();

	@Override
	protected FileExport exportCallable() {
		FileExport callable = new FileExport();
		callable.setAppend(append);
		callable.setContentType(contentType());
		callable.setElementName(elementName);
		callable.setFile(file);
		callable.setFileType(fileType);
		callable.setLineSeparator(lineSeparator);
		callable.setRootName(rootName);
		callable.setFileOptions(fileArgs.fileOptions());
		return callable;
	}

	private ContentType contentType() {
		if (dump) {
			return ContentType.REDIS;
		}
		return ContentType.MAP;
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

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType type) {
		this.fileType = type;
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

	public void setRootName(String name) {
		this.rootName = name;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String name) {
		this.elementName = name;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String separator) {
		this.lineSeparator = separator;
	}

}
