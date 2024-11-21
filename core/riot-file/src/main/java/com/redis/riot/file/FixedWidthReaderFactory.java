package com.redis.riot.file;

import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.file.transform.RangeArrayPropertyEditor;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

public class FixedWidthReaderFactory extends AbstractReaderFactory {

	@Override
	public ItemReader<?> create(Resource resource, ReadOptions options) {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		RangeArrayPropertyEditor editor = new RangeArrayPropertyEditor();
		List<String> columnRanges = options.getColumnRanges();
		Assert.notEmpty(columnRanges, "Column ranges are required");
		editor.setAsText(String.join(",", columnRanges));
		Range[] ranges = (Range[]) editor.getValue();
		Assert.notEmpty(ranges, "Invalid ranges specified: " + columnRanges);
		tokenizer.setColumns(ranges);
		return flatFileReader(resource, options, tokenizer);
	}

}
