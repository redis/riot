package com.redis.riot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.util.StreamUtils;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.operation.HsetCommand;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.spring.batch.test.KeyspaceComparison;

import io.lettuce.core.RedisURI;

abstract class FileTests extends AbstractTargetTestBase {

	public static final String BUCKET_URL = "https://storage.googleapis.com/jrx/";
	public static final String BEERS_JSON_URL = BUCKET_URL + "beers.json";
	public static final String BEERS_JSONL_URL = BUCKET_URL + "beers.jsonl";

	private static final String ID = "id";
	private static final String KEYSPACE = "beer";
	
	private static Path tempDir;

	@BeforeAll
	public void setupFiles() throws IOException {
		tempDir = Files.createTempDirectory(getClass().getName());
	}

	protected Path tempFile(String filename) throws IOException {
		Path path = tempDir.resolve(filename);
		if (Files.exists(path)) {
			Files.delete(path);
		}
		return path;
	}


	@Test
	void fileImportJSON(TestInfo info) throws Exception {
		FileImport executable = new FileImport();
		configure(info, executable);
		executable.setFiles(BEERS_JSON_URL);
		HsetCommand hset = new HsetCommand();
		hset.setKeyspace(KEYSPACE);
		hset.setKeyFields(ID);
		executable.setImportOperationCommands(hset);
		executable.setJobName(name(info));
		executable.call();

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

	@Test
	void fileApiImportCSV(TestInfo info) throws Exception {
		FileImport executable = new FileImport();
		configure(info, executable);
		executable.setFiles("https://storage.googleapis.com/jrx/beers.csv");
		executable.getFileReaderArgs().getFileArgs().setHeader(true);
		executable.setJobName(name(info));
		HsetCommand hset = new HsetCommand();
		hset.setKeyspace(KEYSPACE);
		hset.setKeyFields(ID);
		executable.setImportOperationCommands(hset);
		executable.call();
		List<String> keys = redisCommands.keys("*");
		assertEquals(2410, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
	}

	private void configure(TestInfo info, AbstractRedisImportCommand callable) {
		configure(callable.getRedisArgs());
		configureJobCommand(info, callable);
	}

	private void configure(TestInfo info, AbstractRedisExportCommand callable) {
		configure(callable.getRedisArgs());
		configureJobCommand(info, callable);
	}

	private void configureJobCommand(TestInfo info, AbstractJobCommand callable) {
		callable.setJobName(name(info));
		callable.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
	}

	private void configure(RedisArgs redisArgs) {
		redisArgs.setUri(RedisURI.create(getRedisServer().getRedisURI()));
		redisArgs.setCluster(getRedisServer().isRedisCluster());
	}

	@Test
	void fileApiFileExpansion(TestInfo info) throws Exception {
		Path temp = Files.createTempDirectory("fileExpansion");
		File file1 = temp.resolve("beers1.csv").toFile();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers1.csv"), new FileOutputStream(file1));
		File file2 = temp.resolve("beers2.csv").toFile();
		IOUtils.copy(getClass().getClassLoader().getResourceAsStream("files/beers2.csv"), new FileOutputStream(file2));
		FileImport executable = new FileImport();
		configure(info, executable);
		executable.setFiles(temp.resolve("*.csv").toFile().getPath());
		executable.getFileReaderArgs().getFileArgs().setHeader(true);
		executable.setJobName(name(info));
		HsetCommand operationBuilder = new HsetCommand();
		operationBuilder.setKeyspace(KEYSPACE);
		operationBuilder.setKeyFields(ID);
		executable.setImportOperationCommands(operationBuilder);
		executable.call();
		List<String> keys = redisCommands.keys("*");
		assertEquals(2410, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
	}

	@Test
	void fileImportCSVMultiThreaded(TestInfo info) throws Exception {
		AbstractFileImport executable = new FileImport();
		configure(info, executable);
		executable.setFiles("https://storage.googleapis.com/jrx/beers.csv");
		executable.getFileReaderArgs().getFileArgs().setHeader(true);
		executable.getJobArgs().setThreads(3);
		executable.setJobName(name(info));
		HsetCommand operationBuilder = new HsetCommand();
		operationBuilder.setKeyspace(KEYSPACE);
		operationBuilder.setKeyFields(ID);
		executable.setImportOperationCommands(operationBuilder);
		executable.call();
		List<String> keys = redisCommands.keys("*");
		assertEquals(2410, keys.size());
		for (String key : keys) {
			Map<String, String> map = redisCommands.hgetall(key);
			String id = map.get(ID);
			assertEquals(key, KEYSPACE + ":" + id);
		}
	}

	@Test
	void fileImportJSONL(TestInfo info) throws Exception {
		FileImport executable = new FileImport();
		configure(info, executable);
		executable.setFiles(BEERS_JSONL_URL);
		HsetCommand hset = new HsetCommand();
		hset.setKeyspace(KEYSPACE);
		hset.setKeyFields(ID);
		executable.setImportOperationCommands(hset);
		executable.setJobName(name(info));
		executable.call();
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
		FileExport fileExport = new FileExport();
		configure(info, fileExport);
		fileExport.setContentType(ContentType.STRUCT);
		fileExport.setFile(file);
		fileExport.call();
		FileImport fileImport = new FileImport();
		configure(info, fileImport);
		fileImport.setFiles(file);
		fileImport.getRedisArgs().setUri(RedisURI.create(getTargetRedisServer().getRedisURI()));
		fileImport.getRedisArgs().setCluster(getTargetRedisServer().isRedisCluster());
		fileImport.call();
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void fileImportRedisJson(TestInfo info) throws Exception {
		File file = tempFile("fileImportRedisJson.json").toFile();
		StreamUtils.copy(getClass().getClassLoader().getResourceAsStream("files/redis-export.json"),
				new FileOutputStream(file));
		FileImport fileImport = new FileImport();
		fileImport.setFiles(file.toString());
		fileImport.getRedisArgs().setUri(redisURI);
		fileImport.call();
		Assertions.assertEquals(100, redisCommands.dbsize());
	}

}
