package com.alibaba.flink.connectors.hologres.table;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.hologres.datastream.sink.DatahubRecordResolver;
import com.alibaba.flink.connectors.hologres.datastream.util.DatahubClientProvider;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Resolver to convert Flink Row into Datahub RecordEntry.
 */
public class DatahubRowRecordResolver implements DatahubRecordResolver<Row> {
	private final RowTypeInfo flinkRowTypeInfo;
	private final String project;
	private final String topic;
	private final String accessId;
	private final String accessKey;
	private final String endpoint;
	private final Map<String, Object> defaultValueMap;
	private final HashMap<String, Integer> rowTypeColumnIdxMap;

	private transient RecordSchema recordSchema;

	public DatahubRowRecordResolver(
			RowTypeInfo flinkRowTypeInfo,
			String project,
			String topic,
			String accessId,
			String accessKey,
			String endpoint,
			Map<String, Object> defaultValueMap) {
		this.flinkRowTypeInfo = flinkRowTypeInfo;
		this.project = project;
		this.topic = topic;
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.defaultValueMap = defaultValueMap;

		this.rowTypeColumnIdxMap = new HashMap<>();
		String[] names = flinkRowTypeInfo.getFieldNames();
		for (int i = 0; i < names.length; ++i) {
			rowTypeColumnIdxMap.put(names[i], i);
		}
	}

	@Override
	public void open() {
		DatahubClient client = new DatahubClientProvider(endpoint, accessId, accessKey).getClient();
		recordSchema = client.getTopic(project, topic).getRecordSchema();

		checkArgument(recordSchema.getFields().size() >= flinkRowTypeInfo.getArity());

		HashSet<String> fieldSet = new HashSet<>();
		for (Field f : recordSchema.getFields()) {
			fieldSet.add(f.getName());
		}
		for (String n : flinkRowTypeInfo.getFieldNames()) {
			if (fieldSet.contains(n)) {
				continue;
			}
			throw new IllegalArgumentException(String.format("Field [%s] not existed in holo table schema.", n));
		}
	}

	@Override
	public RecordEntry getRecordEntry(Row row) {
		RecordEntry record = new RecordEntry();
		TupleRecordData recordData = new TupleRecordData(recordSchema);

		for (int i = 0; i < recordSchema.getFields().size(); i++) {
			Field column = recordSchema.getField(i);
			String name = column.getName();
			Object columnData = null;

			if (rowTypeColumnIdxMap.containsKey(name)) {
				int idx = rowTypeColumnIdxMap.get(name);
				columnData = row.getField(idx);
			} else {
				columnData = defaultValueMap.get(name);
				if (null == columnData) {
					continue;
				}
			}

			switch (column.getType()) {
				case BIGINT:
				case DECIMAL:
				case BOOLEAN:
				case DOUBLE:
				case TIMESTAMP:
				case STRING:
					recordData.setField(i, columnData);
					break;
				default:
					throw new RuntimeException(
							String.format("DatahubRowRecordResolver doesn't support type '%s' yet", columnData.getClass().getName()));
			}
		}
		record.setRecordData(recordData);
		return record;
	}
}
