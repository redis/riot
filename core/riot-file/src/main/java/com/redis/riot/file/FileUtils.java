package com.redis.riot.file;

import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

public abstract class FileUtils {

	public static final String GZ_SUFFIX = ".gz";

	public static final MimeType CSV = new MimeType("text", "csv");
	public static final MimeType PSV = new MimeType("text", "psv");
	public static final MimeType TSV = new MimeType("text", "tsv");
	public static final MimeType TEXT = new MimeType("text", "plain");
	public static final MimeType JSON = MimeTypeUtils.APPLICATION_JSON;
	public static final MimeType JSON_LINES = new MimeType("application", "jsonlines");
	public static final MimeType XML = MimeTypeUtils.APPLICATION_XML;

	private FileUtils() {
	}

	public static boolean isGzip(String filename) {
		return filename.endsWith(GZ_SUFFIX);
	}

	public static String stripGzipSuffix(String filename) {
		if (isGzip(filename)) {
			return filename.substring(0, filename.length() - GZ_SUFFIX.length());
		}
		return filename;
	}

}
