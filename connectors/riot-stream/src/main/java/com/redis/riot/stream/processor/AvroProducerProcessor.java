package com.redis.riot.stream.processor;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.StreamMessage;

public class AvroProducerProcessor extends AbstractProducerProcessor {

	private Schema schema;

	public AvroProducerProcessor(Converter<StreamMessage<String, String>, String> topicConverter) {
		super(topicConverter);
	}

	@Override
	protected GenericRecord value(StreamMessage<String, String> message) {
		if (schema == null) {
			RecordBuilder<Schema> builder = SchemaBuilder.record(message.getStream());
			FieldAssembler<Schema> fields = builder.fields();
			for (String fieldName : message.getBody().keySet()) {
				fields.name(fieldName).type().optional().stringType();
			}
			this.schema = fields.endRecord();
		}
		GenericRecord avroRecord = new GenericData.Record(schema);
		message.getBody().forEach(avroRecord::put);
		return avroRecord;
	}

}
