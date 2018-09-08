package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.index.Document;
import com.redislabs.lettusearch.index.Document.DocumentBuilder;
import com.redislabs.recharge.RechargeConfiguration.FTAddConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FTAddWriter extends AbstractSearchWriter {

	private FTAddConfiguration add;

	public FTAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer,
			RediSearchClient rediSearchClient) {
		super(template, writer, rediSearchClient);
		this.add = writer.getSearch().getAdd();
	}

	@Override
	protected void write(RediSearchCommands<String, String> commands, String index, String key,
			Map<String, Object> record) {
		double score = getScore(add, record);
		DocumentBuilder builder = Document.builder().id(key).fields(record).score(score);
		if (add.getLanguage() != null) {
			builder.language(add.getLanguage());
		}
		builder.noSave(add.isNoSave());
		try {
			commands.add(index, builder.build());
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

}
