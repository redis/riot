package com.redis.riot.file;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.amazonaws.util.IOUtils;
import com.redis.riot.core.AbstractRedisCallable;
import com.redis.riot.core.operation.HsetBuilder;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.spring.batch.test.KeyspaceComparison;

import io.lettuce.core.RedisURI;

abstract class FileTests extends AbstractTargetTestBase {

	public static final String BUCKET_URL = "https://storage.googleapis.com/jrx/";
	public static final String BEERS_JSON_URL = BUCKET_URL + "beers.json";
	public static final String BEERS_JSONL_URL = BUCKET_URL + "beers.jsonl";

	private static final String ID = "id";
	private static final String KEYSPACE = "beer";

	@SuppressWarnings("unchecked")
	@Test
	void fileImportJSON(TestInfo info) throws Exception {
		try (FileImport executable = configure(info, new FileImport())) {
			executable.setFiles(BEERS_JSON_URL);
			HsetBuilder hsetBuilder = new HsetBuilder();
			hsetBuilder.setKeyspace(KEYSPACE);
			hsetBuilder.setKeyFields(ID);
			executable.setOperations(hsetBuilder.build());
			executable.setName(name(info));
			executable.afterPropertiesSet();
			executable.call();
		}
		List<String> keys = redisCommands.keys("*");
		assertEquals(216, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
		Map<String, String> beer1 = redisCommands.hgetall(KEYSPACE + ":1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@SuppressWarnings("unchecked")
	@Test
	void fileApiImportCSV(TestInfo info) throws Exception {
		try (FileImport executable = configure(info, new FileImport())) {
			executable.setFiles("https://storage.googleapis.com/jrx/beers.csv");
			executable.setHeader(true);
			executable.setName(name(info));
			HsetBuilder hsetBuilder = new HsetBuilder();
			hsetBuilder.setKeyspace(KEYSPACE);
			hsetBuilder.setKeyFields(ID);
			executable.setOperations(hsetBuilder.build());
			executable.afterPropertiesSet();
			executable.call();
		}
		List<String> keys = redisCommands.keys("*");
		assertEquals(2410, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
	}

	private <T extends AbstractRedisCallable> T configure(TestInfo info, T callable) {
		callable.setRedisURI(RedisURI.create(getRedisServer().getRedisURI()));
		callable.getRedisClientOptions().setCluster(getRedisServer().isRedisCluster());
		callable.setName(name(info));
		return callable;
	}

	@SuppressWarnings("unchecked")
	@Test
	void fileApiFileExpansion(TestInfo info) throws Exception {
		Path temp = Files.createTempDirectory("fileExpansion");
		File file1 = temp.resolve("beers1.csv").toFile();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("beers1.csv"), new FileOutputStream(file1));
		File file2 = temp.resolve("beers2.csv").toFile();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("beers2.csv"), new FileOutputStream(file2));
		try (FileImport executable = configure(info, new FileImport())) {
			executable.setFiles(temp.resolve("*.csv").toFile().getPath());
			executable.setHeader(true);
			executable.setName(name(info));
			HsetBuilder hsetBuilder = new HsetBuilder();
			hsetBuilder.setKeyspace(KEYSPACE);
			hsetBuilder.setKeyFields(ID);
			executable.setOperations(hsetBuilder.build());
			executable.afterPropertiesSet();
			executable.call();
		}
		List<String> keys = redisCommands.keys("*");
		assertEquals(2410, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void fileImportCSVMultiThreaded(TestInfo info) throws Exception {
		try (FileImport executable = configure(info, new FileImport())) {
			executable.setFiles("https://storage.googleapis.com/jrx/beers.csv");
			executable.setHeader(true);
			executable.setThreads(3);
			executable.setName(name(info));
			HsetBuilder hset = new HsetBuilder();
			hset.setKeyspace(KEYSPACE);
			hset.setKeyFields(ID);
			executable.setOperations(hset.build());
			executable.afterPropertiesSet();
			executable.call();
		}
		List<String> keys = redisCommands.keys("*");
		assertEquals(2410, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void fileImportJSONL(TestInfo info) throws Exception {
		try (FileImport executable = configure(info, new FileImport())) {
			executable.setFiles(BEERS_JSONL_URL);
			HsetBuilder hsetBuilder = new HsetBuilder();
			hsetBuilder.setKeyspace(KEYSPACE);
			hsetBuilder.setKeyFields(ID);
			executable.setOperations(hsetBuilder.build());
			executable.setName(name(info));
			executable.afterPropertiesSet();
			executable.call();
		}
		List<String> keys = redisCommands.keys("*");
		assertEquals(6, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
		Map<String, String> beer1 = redisCommands.hgetall(KEYSPACE + ":1");
		Assertions.assertEquals("Hocus Pocus", beer1.get("name"));
	}

	@Test
	void fileExportImportJson(TestInfo info) throws Exception {
		fileExportImport(info, "export.json");
	}

	@Test
	void fileExportImportJsonl(TestInfo info) throws Exception {
		fileExportImport(info, "export.jsonl");
	}

//	@Test
	void fileExportImportXml(TestInfo info) throws Exception {
		fileExportImport(info, "export.xml");
	}

	private void fileExportImport(TestInfo info, String filename) throws Exception {
		generate(info, generator(100));
		String dirName = name(info);
		Path dir = Files.createTempDirectory(dirName);
		String file = dir.resolve(filename).toFile().getPath();
		try (FileExport fileExport = configure(info, new FileExport())) {
			fileExport.setContentType(ContentType.REDIS);
			fileExport.setFile(file);
			fileExport.afterPropertiesSet();
			fileExport.call();
		}
		try (FileImport fileImport = configure(info, new FileImport())) {
			fileImport.setFiles(file);
			fileImport.setRedisURI(RedisURI.create(getTargetRedisServer().getRedisURI()));
			fileImport.getRedisClientOptions().setCluster(getTargetRedisServer().isRedisCluster());
			fileImport.afterPropertiesSet();
			fileImport.call();
		}
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

}
