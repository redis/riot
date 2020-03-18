package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import com.redislabs.riot.file.StandardInputResource;
import com.redislabs.riot.file.StandardOutputResource;

import picocli.CommandLine.Option;

public class FileOptions {

	private final static String EXTENSION_GROUP = "extension";
	private final static String EXTENSION_CSV = "csv";
	private final static String EXTENSION_TSV = "tsv";
	private final static String EXTENSION_DAT = "dat";
	private final static String EXTENSION_JSON = "json";
	private final static String EXTENSION_XML = "xml";
	private final static Pattern EXTENSION = Pattern.compile("(?i).*\\.(?<" + EXTENSION_GROUP + ">" + EXTENSION_CSV
			+ "|" + EXTENSION_DAT + "|" + EXTENSION_JSON + "|" + EXTENSION_TSV + "|" + EXTENSION_XML + ")(\\.gz)?$");

	private final static Map<String, FileType> FILETYPES = new HashMap<>();
	static {
		FILETYPES.put(EXTENSION_CSV, FileType.CSV);
		FILETYPES.put(EXTENSION_DAT, FileType.CSV);
		FILETYPES.put(EXTENSION_TSV, FileType.TSV);
		FILETYPES.put(EXTENSION_JSON, FileType.JSON);
		FILETYPES.put(EXTENSION_XML, FileType.XML);
	}

	@Option(names = { "-f", "--file" }, description = "File path or URL")
	private String file;
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@Option(names = "--s3-access", description = "AWS S3 access key ID", paramLabel = "<string>")
	private String accessKey;
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
	private String secretKey;
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
	private String region;
	@Option(names = "--fields", arity = "1..*", description = "Field names", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = { "-e",
			"--encoding" }, description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Option(names = { "-h", "--header" }, description = "First line contains field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
	private String delimiter;

	protected String getEncoding() {
		return encoding;
	}

	protected boolean isHeader() {
		return header;
	}

	protected String getDelimiter() {
		if (delimiter == null) {
			switch (getType()) {
			case TSV:
				return DelimitedLineTokenizer.DELIMITER_TAB;
			default:
				return DelimitedLineTokenizer.DELIMITER_COMMA;
			}
		}
		return delimiter;
	}

	protected String[] getNames() {
		return names;
	}

	protected Resource getInputResource() throws IOException {
		if (isConsoleResource()) {
			return new StandardInputResource();
		}
		return getResource();
	}

	private boolean isConsoleResource() {
		return file.equals("-");
	}

	protected Resource getOutputResource() throws IOException {
		if (isConsoleResource()) {
			return new StandardOutputResource();
		}
		return getResource();
	}

	private Resource getResource() throws IOException {
		if (ResourceUtils.isUrl(file)) {
			UrlResource resource = new UrlResource(file.toString());
			if (resource.getURI().getScheme().equals("s3")) {
				return S3ResourceBuilder.resource(accessKey, secretKey, region, resource.getURI());
			}
			return new UncustomizedUrlResource(resource.getURI());
		}
		return new FileSystemResource(file);
	}

	protected boolean isGzip() {
		return gzip || file.toLowerCase().endsWith(".gz");
	}

	public boolean isSet() {
		return file != null;
	}

	protected FileType getType() {
		if (type == null) {
			Matcher matcher = EXTENSION.matcher(file);
			if (matcher.matches()) {
				return FILETYPES.get(matcher.group(EXTENSION_GROUP));
			}
			return FileType.CSV;
		}
		return type;
	}

}
