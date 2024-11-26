package com.redis.riot.file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;
import org.springframework.util.StreamUtils;

import software.amazon.awssdk.regions.Region;

public class ReaderTests {

	public static final String BUCKET_URL = "https://storage.googleapis.com/jrx/";
	public static final String JSON_FILE = "beers.json";
	public static final String JSON_URL = BUCKET_URL + JSON_FILE;
	public static final String JSONL_FILE = "beers.jsonl";
	public static final String JSONL_URL = BUCKET_URL + JSONL_FILE;
	public static final String CSV_FILE = "beers.csv";
	public static final String CSV_URL = BUCKET_URL + CSV_FILE;
	public static final String S3_BUCKET_URL = "s3://riot-bucket-jrx";
	public static final String JSON_S3_URL = S3_BUCKET_URL + "/beers.json";
	public static final String JSON_GOOGLE_STORAGE_URL = "gs://riot-bucket-jrx/beers.json";
	public static final String JSON_GZ_URL = "http://storage.googleapis.com/jrx/beers.json.gz";

	private final ResourceFactory resourceFactory = new ResourceFactory();
	private final ResourceMap resourceMap = RiotResourceMap.defaultResourceMap();
	private final FileReaderRegistry registry = FileReaderRegistry.defaultReaderRegistry();

	@Test
	void readJsonUrl() throws Exception {
		assertRead(JSON_URL, JsonItemReader.class, 216);
	}

	@Test
	void readJsonGzUrl() throws Exception {
		assertRead(JSON_GZ_URL, JsonItemReader.class, 216);
	}

	@Test
	void readJsonS3Url() throws Exception {
		ReadOptions options = new ReadOptions();
		options.getS3Options().setRegion(Region.US_WEST_1);
		assertRead(JSON_S3_URL, options, JsonItemReader.class, 4432);
	}

	@Test
	void readJsonGoogleStorageUrl() throws Exception {
		assertRead(JSON_GOOGLE_STORAGE_URL, JsonItemReader.class, 4432);
	}

	@Test
	void readJsonFile() throws Exception {
		Path file = Files.createTempFile("readJsonFile", JSON_FILE);
		try (FileOutputStream outputStream = new FileOutputStream(file.toFile())) {
			StreamUtils.copy(urlInputStream(JSON_URL), outputStream);
		}
		assertRead(file.toFile().getAbsolutePath(), JsonItemReader.class, 216);
	}

	private InputStream urlInputStream(String url) throws MalformedURLException, IOException, URISyntaxException {
		return new URI(url).toURL().openConnection().getInputStream();
	}

	@Test
	void readJsonLinesUrl() throws Exception {
		assertRead(JSONL_URL, FlatFileItemReader.class, 6);
	}

	@Test
	void readCsvUrl() throws Exception {
		ReadOptions options = new ReadOptions();
		options.setHeader(true);
		assertRead(CSV_URL, options, FlatFileItemReader.class, 2410);
	}

	@Test
	void readStdIn() throws Exception {
		RiotResourceLoader resourceLoader = new RiotResourceLoader();
		resourceLoader.addProtocolResolver(new StdInProtocolResolver());
		Resource resource = resourceLoader.getResource(SystemInResource.FILENAME);
		Assertions.assertInstanceOf(SystemInResource.class, resource);
	}

	private void assertRead(String location, Class<?> expectedType, int expectedCount) throws Exception {
		assertRead(location, new ReadOptions(), expectedType, expectedCount);
	}

	private void assertRead(String location, ReadOptions options, Class<?> expectedType, int expectedCount)
			throws Exception {
		Resource resource = resourceFactory.resource(location, options);
		MimeType type = resourceMap.getContentTypeFor(resource);
		ItemReader<?> reader = registry.getReaderFactory(type).create(resource, options);
		Assertions.assertNotNull(reader);
		List<?> items = readAll(reader);
		Assertions.assertEquals(expectedCount, items.size());
	}

	private <T> List<T> readAll(ItemReader<T> reader) throws Exception {
		if (reader instanceof ItemStream) {
			((ItemStream) reader).open(new ExecutionContext());
		}
		List<T> items = new ArrayList<>();
		T item;
		while ((item = reader.read()) != null) {
			items.add(item);
		}
		if (reader instanceof ItemStream) {
			((ItemStream) reader).close();
		}
		return items;
	}

}
