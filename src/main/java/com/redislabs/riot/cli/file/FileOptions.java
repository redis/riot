package com.redislabs.riot.cli.file;

import java.io.IOException;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import com.redislabs.riot.cli.StandardInputResource;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FileOptions {

	@Parameters(paramLabel = "FILE", description = "File")
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
	protected String[] names = new String[0];
	@Option(names = { "-e",
			"--encoding" }, description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	protected String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Option(names = { "-h", "--header" }, description = "First line contains field names")
	protected boolean header;
	@Option(names = "--delimiter", description = "Delimiter character (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	protected String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;

	protected Resource resource() throws IOException {
		if (file.equals("-")) {
			return new StandardInputResource();
		}
		if (ResourceUtils.isUrl(file)) {
			UrlResource resource = new UrlResource(file.toString());
			if (resource.getURI().getScheme().equals("s3")) {
				return S3ResourceBuilder.resource(accessKey, secretKey, region, resource.getURI());
			}
			return new UncustomizedUrlResource(resource.getURI());
		}
		return new FileSystemResource(file);
	}

	protected boolean isGzip(Resource resource) {
		return gzip || (resource.getFilename()!=null && resource.getFilename().toLowerCase().endsWith(".gz"));
	}

	public boolean isSet() {
		return file != null;
	}

	protected FileType type() {
		if (type != null) {
			return type;
		}
		String name = file.toLowerCase();
		if (name.endsWith(".json") || name.endsWith(".json.gz")) {
			return FileType.Json;
		}
		if (name.endsWith(".xml") || name.endsWith(".xml.gz")) {
			return FileType.Xml;
		}
		return FileType.Csv;
	}

}
