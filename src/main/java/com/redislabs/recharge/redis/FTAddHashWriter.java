package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.FTAddHashConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FTAddHashWriter extends AbstractSearchWriter {

	private FTAddHashConfiguration addHash;

	public FTAddHashWriter(StringRedisTemplate template, RedisWriterConfiguration writer,
			RediSearchClient rediSearchClient) {
		super(template, writer, rediSearchClient);
		this.addHash = writer.getSearch().getAddHash();
	}

	@Override
	protected void write(RediSearchCommands<String, String> commands, String index, String key,
			Map<String, Object> record) {
		double score = getScore(addHash, record);
		try {
			if (addHash.getLanguage() == null) {
				commands.addHash(index, key, score);
			} else {
				commands.addHash(index, key, score, addHash.getLanguage());
			}
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

}
