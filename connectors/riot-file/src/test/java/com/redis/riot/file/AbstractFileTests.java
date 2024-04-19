package com.redis.riot.file;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.amazonaws.util.IOUtils;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.operation.HsetBuilder;
import com.redis.spring.batch.test.AbstractTestBase;

abstract class AbstractFileTests extends AbstractTestBase {

	public static final String BUCKET_URL = "https://storage.googleapis.com/jrx/";
	public static final String BEERS_JSON_URL = BUCKET_URL + "beers.json";
	public static final String BEERS_JSONL_URL = BUCKET_URL + "beers.jsonl";

	private static final String ID = "id";
	private static final String KEYSPACE = "beer";

	@SuppressWarnings("unchecked")
	@Test
	void fileImportJSON(TestInfo info) throws Exception {
		FileImport executable = new FileImport();
		executable.setRedisClientOptions(redisClientOptions());
		executable.setFiles(BEERS_JSON_URL);
		HsetBuilder hsetBuilder = new HsetBuilder();
		hsetBuilder.setKeyspace(KEYSPACE);
		hsetBuilder.setKeyFields(ID);
		executable.setOperations(hsetBuilder.build());
		executable.setName(name(info));
		executable.execute();
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

	private RedisClientOptions redisClientOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setCluster(getRedisServer().isRedisCluster());
		options.setUri(getRedisServer().getRedisURI());
		return options;
	}

	@SuppressWarnings("unchecked")
	@Test
	void fileApiImportCSV(TestInfo info) throws Exception {
		FileImport executable = new FileImport();
		executable.setRedisClientOptions(redisClientOptions());
		executable.setFiles("https://storage.googleapis.com/jrx/beers.csv");
		executable.setHeader(true);
		executable.setName(name(info));
		HsetBuilder hsetBuilder = new HsetBuilder();
		hsetBuilder.setKeyspace(KEYSPACE);
		hsetBuilder.setKeyFields(ID);
		executable.setOperations(hsetBuilder.build());
		executable.execute();
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
	void fileApiFileExpansion(TestInfo info) throws Exception {
		Path temp = Files.createTempDirectory("fileExpansion");
		File file1 = temp.resolve("beers1.csv").toFile();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("beers1.csv"), new FileOutputStream(file1));
		File file2 = temp.resolve("beers2.csv").toFile();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("beers2.csv"), new FileOutputStream(file2));
		FileImport executable = new FileImport();
		executable.setRedisClientOptions(redisClientOptions());
		executable.setFiles(temp.resolve("*.csv").toFile().getPath());
		executable.setHeader(true);
		executable.setName(name(info));
		HsetBuilder hsetBuilder = new HsetBuilder();
		hsetBuilder.setKeyspace(KEYSPACE);
		hsetBuilder.setKeyFields(ID);
		executable.setOperations(hsetBuilder.build());
		executable.execute();
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
		FileImport executable = new FileImport();
		executable.setRedisClientOptions(redisClientOptions());
		executable.setFiles("https://storage.googleapis.com/jrx/beers.csv");
		executable.setHeader(true);
		executable.setThreads(3);
		executable.setName(name(info));
		HsetBuilder hset = new HsetBuilder();
		hset.setKeyspace(KEYSPACE);
		hset.setKeyFields(ID);
		executable.setOperations(hset.build());
		executable.execute();
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
		FileImport executable = new FileImport();
		executable.setRedisClientOptions(redisClientOptions());
		executable.setFiles(BEERS_JSONL_URL);
		HsetBuilder hsetBuilder = new HsetBuilder();
		hsetBuilder.setKeyspace(KEYSPACE);
		hsetBuilder.setKeyFields(ID);
		executable.setOperations(hsetBuilder.build());
		executable.setName(name(info));
		executable.execute();
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

}
