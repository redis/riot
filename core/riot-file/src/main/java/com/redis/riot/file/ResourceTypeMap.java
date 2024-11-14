package com.redis.riot.file;

import java.io.IOException;
import java.net.FileNameMap;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.util.Assert;
import org.springframework.util.MimeType;

public class ResourceTypeMap {

	private final Set<FileNameMap> fileNameMaps = new LinkedHashSet<>();

	public void addFileNameMap(FileNameMap map) {
		fileNameMaps.add(map);
	}

	public MimeType getContentType(String filename) {
		String normalizedFilename = FileUtils.normalize(filename);
		try {
			String type = Files.probeContentType(Path.of(normalizedFilename));
			if (type == null) {
				type = URLConnection.guessContentTypeFromName(normalizedFilename);
				Iterator<FileNameMap> maps = fileNameMaps.iterator();
				while (type == null && maps.hasNext()) {
					type = maps.next().getContentTypeFor(normalizedFilename);
				}
			}
			Assert.notNull(type, () -> "Could not determine type of " + normalizedFilename);
			return MimeType.valueOf(type);
		} catch (IOException e) {
			throw new RuntimeIOException("Could not determine type of " + normalizedFilename, e);
		}
	}

	public static ResourceTypeMap defaultResourceTypeMap() {
		ResourceTypeMap map = new ResourceTypeMap();
		map.addFileNameMap(new JsonLinesFileNameMap());
		return map;
	}

	private static class JsonLinesFileNameMap implements FileNameMap {

		public static final String JSONL_SUFFIX = ".jsonl";

		@Override
		public String getContentTypeFor(String fileName) {
			if (fileName == null) {
				return null;
			}
			if (fileName.endsWith(JSONL_SUFFIX)) {
				return FileUtils.JSON_LINES.toString();
			}
			return null;
		}

	}

}
