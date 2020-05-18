/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.hologres.table;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.hologres.datastream.sink.DatahubOutputFormat;
import com.alibaba.flink.connectors.hologres.datastream.util.HoloUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.alibaba.flink.connectors.hologres.table.DatahubDescriptorValidator.CONNECTOR_BATCH_SIZE;
import static com.alibaba.flink.connectors.hologres.table.DatahubDescriptorValidator.CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS;
import static com.alibaba.flink.connectors.hologres.table.DatahubDescriptorValidator.CONNECTOR_BUFFER_SIZE;
import static com.alibaba.flink.connectors.hologres.table.DatahubDescriptorValidator.CONNECTOR_MAX_RETRY_TIMES;
import static com.alibaba.flink.connectors.hologres.table.DatahubDescriptorValidator.CONNECTOR_RETRY_TIMEOUT_IN_MILLS;

/**
 * Table Sink for Datahub.
 */
public class HologresTableSink extends OutputFormatTableSink<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(HoloUtil.class);

	private final String database;
	private final String table;
	private final String accessId;
	private final String accessKey;
	private final String endpoint;
	private final TableSchema schema;
	private final DescriptorProperties  prop;

	public HologresTableSink(
			String database,
			String table,
			String accessId,
			String accessKey,
			String endpoint,
			TableSchema schema,
			DescriptorProperties prop) {
		this.database = database;
		this.table = table;
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.schema = schema;
		this.prop = prop;
	}

	@Override
	public DataType getConsumedDataType() {
		return schema.toRowDataType();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
		return new HologresTableSink(database, table, accessId, accessKey, endpoint, schema, prop);
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		try {
			LOG.info("getOutputFormat started. fe: {}, accessId: {}, database: {}, table: {}",
					endpoint, accessId, database, table);
			String holohubEndpoint = HoloUtil.getHolohubEndpoint(endpoint, accessId, accessKey, database);
			holohubEndpoint = "http://" + holohubEndpoint;

			LOG.info("Got holohub: {}", holohubEndpoint);

			Map<String, Object> defaultValueMap = HoloUtil.getDefaultValueMap(
					endpoint, accessId, accessKey, database, table);

			String[] names = new String[schema.getFieldCount()];
			for (int i = 0; i < schema.getFieldCount(); ++i) {
				names[i] = schema.getFieldName(i).get().toLowerCase();
			}
			RowTypeInfo flinkRowTypeInfo = new RowTypeInfo(schema.getFieldTypes(), names);
			DatahubOutputFormat outputFormat = new DatahubOutputFormat<Row>(
					holohubEndpoint,
					database,
					table,
					accessId,
					accessKey,
					flinkRowTypeInfo);

			if (prop.containsKey(CONNECTOR_BUFFER_SIZE)) {
				outputFormat.setBufferSize(prop.getInt(CONNECTOR_BUFFER_SIZE));
			}

			if (prop.containsKey(CONNECTOR_BATCH_SIZE)) {
				outputFormat.setBatchSize(prop.getInt(CONNECTOR_BATCH_SIZE));
			}

			if (prop.containsKey(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS)) {
				outputFormat.setBatchWriteTimeout(prop.getLong(CONNECTOR_BATCH_WRITE_TIMEOUT_IN_MILLS));
			}
			if (prop.containsKey(CONNECTOR_RETRY_TIMEOUT_IN_MILLS)) {
				outputFormat.setRetryTimeoutInMills(prop.getInt(CONNECTOR_RETRY_TIMEOUT_IN_MILLS));
			}

			if (prop.containsKey(CONNECTOR_MAX_RETRY_TIMES)) {
				outputFormat.setMaxRetryTimes(prop.getInt(CONNECTOR_MAX_RETRY_TIMES));
			}

			outputFormat.setRecordResolver(
					new DatahubRowRecordResolver(
							flinkRowTypeInfo, database, table, accessId, accessKey, holohubEndpoint, defaultValueMap));

			return outputFormat;
		} catch (Exception e) {
			LOG.error("Failed to get outputformat", e);
			return null;
		}
	}
}
