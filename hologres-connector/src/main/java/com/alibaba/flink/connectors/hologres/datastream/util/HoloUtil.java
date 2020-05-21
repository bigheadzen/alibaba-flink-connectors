package com.alibaba.flink.connectors.hologres.datastream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * HoloUtil.
 */
public final class HoloUtil {
	private static final String PG_DRIVER_NAME = "org.postgresql.Driver";
	private static final Logger LOG = LoggerFactory.getLogger(HoloUtil.class);

	static final DateTimeFormatter DATE_TIME_FORMATTER =
			new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_DATE)
					.appendLiteral(' ')
					.appendPattern("HH:mm:ss")
					.appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
					.optionalStart()
					.appendOffset("+HH", "00")
					.optionalEnd()
					.toFormatter();

	static {
		try {
			Class.forName(PG_DRIVER_NAME);
		} catch (ClassNotFoundException e) {
			LOG.error("Failed to load PG JDBC driver: {}", PG_DRIVER_NAME, e);
			System.exit(-1);
		}
	}

	private HoloUtil() {
	}

	public static Map<String, Object> getDefaultValueMap(
			String fe, String accessId, String accessKey, String db, String table) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = getConnection(
					getJdbcUrl(fe, db),
					accessId,
					accessKey);

			Map<String, Integer> columnTypeMap = getTableColumns(conn, table);

			String[] names = getSchemaAndTable(table);
			String sql = String.format(
					"SELECT column_name, column_default " +
							"FROM information_schema.columns " +
							"WHERE (table_schema, table_name) = ('%s', '%s') " +
							"ORDER BY ordinal_position;", names[0], names[1]);

			LOG.info("Will get default value. sql: {}, fe: {}, db: {}", sql, fe, db);

			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			Map<String, Object> defaultMap = new HashMap<>();
			while (rs.next()) {
				String name = rs.getString(1);
				String val = rs.getString(2);

				LOG.info("Column: {}, default: {}", name, val != null ? val : "");

				if (val == null || val.isEmpty()) {
					continue;
				}

				Object defaultVal = parseDefaultValue(val, columnTypeMap.get(name.toLowerCase()));
				LOG.info("Default value. column: {}, val: {}", name.toLowerCase(), defaultVal);
				defaultMap.put(name.toLowerCase(), defaultVal);
			}
			return defaultMap;
		} finally {
			HoloUtil.closeDBResources(rs, stmt, conn);
		}
	}

	// 'xxx'::text
	private static String parseValueFromDefaultExpr(String val) {
		int pos = val.indexOf("::");
		if (pos == -1) {
			return val;
		}
		String subStrWithSingleQuote = val.substring(0, pos);
		return subStrWithSingleQuote.substring(1, subStrWithSingleQuote.length() - 1);
	}

	//    column_name   |                         column_default
	//-----------------+-----------------------------------------------------------------
	//    id              |
	//    type_text       | 'hello'::text
	//    type_int4       | 111
	//    type_int8       | '222'::bigint
	//    type_bool       | true
	//    type_date       | '2020-01-01'::date
	//    type_float4     | '1.1'::real
	//    type_float8     | '2.2'::double precision
	//    type_dec        | 3.3
	//    type_ts_tz      | '2020-01-01 02:03:04.123+08'::timestamp with time zone
	//    type_int4_arr   | '{1,2,3}'::integer[]
	//    type_int8_arr   | '{4,5,6}'::bigint[]
	//    type_bool_arr   | '{t,f,t}'::boolean[]
	//    type_text_arr   | '{xxx,yyy,zzz}'::text[]
	//    type_float4_arr | '{1.1,2.2,3.3}'::real[]
	//    type_float8_arr | '{4.4,5.5,6.6}'::double precision[]
	//    geom            | '010100000000000000000000000000000000000000'::geometry
	//    geog            | '0101000020E6100000000000000000F03F000000000000F03F'::geography
	//
	private static Object parseDefaultValue(String val, int type) {
		String str = parseValueFromDefaultExpr(val);
		switch (type) {
			case Types.CHAR:
			case Types.NCHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.NVARCHAR:
			case Types.LONGNVARCHAR:
				// Parse 'xxx'::text
				return str;
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.TINYINT:
				return Long.parseLong(str);
			case Types.NUMERIC:
			case Types.DECIMAL:
				return new BigDecimal(str);
			case Types.FLOAT:
			case Types.REAL:
			case Types.DOUBLE:
				return Double.parseDouble(str);
			case Types.BOOLEAN:
			case Types.BIT: {
				if (str.equalsIgnoreCase("true")) {
					return true;
				} else if (str.equalsIgnoreCase("false")) {
					return false;
				} else {
					throw new RuntimeException("Invalid boolean str: " + str);
				}
			}
			case Types.DATE: {
				LocalDate ld = convertColumnToLocalDate(str);
				ZonedDateTime startOfDay = ld.atStartOfDay().atZone(ZoneId.of("UTC"));
				long us = startOfDay.toEpochSecond() * 1000 * 1000;
				return us;
			}
			case Types.TIMESTAMP:
			case Types.TIMESTAMP_WITH_TIMEZONE: {
				ZonedDateTime zdt = convertColumnToZonedDateTime(str);
				long us = zdt.toEpochSecond() * 1000 * 1000 + zdt.getNano() / 1000;
				return us;
			}
			case Types.TIME:
				// Time not supported in holo
				throw new RuntimeException("Default value not supported for Time type.");
			case Types.ARRAY:
				throw new RuntimeException("Default value not supported for Array type.");
			default:
				throw new RuntimeException("Default value not supported for type: " + type);
		}
	}

	private static Map<String, Integer> getTableColumns(Connection conn, String table) throws Exception {
		Statement statement = null;
		ResultSet rs = null;
		try {
			statement = conn.createStatement();
			String sql = String.format("select * from %s where 1=2", table);

			LOG.info("Query table columns. table: {}, sql: {}", table, sql);

			rs = statement.executeQuery(sql);
			ResultSetMetaData rsMetaData = rs.getMetaData();

			Map<String, Integer> columnTypeMap = new HashMap<>();

			for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
				String name = rsMetaData.getColumnName(i + 1);
				Integer type = rsMetaData.getColumnType(i + 1);
				String typeName = rsMetaData.getColumnTypeName(i + 1);
				LOG.info("Query table columns. table: {}, column: {} type: {} typeName: {}", table, name, type, typeName);
				if (columnTypeMap.containsKey(name.toLowerCase())) {
					throw new RuntimeException("Column name (ignore case) duplicated: " + name);
				}
				columnTypeMap.put(name.toLowerCase(), type);
			}
			return columnTypeMap;
		} finally {
			closeDBResources(rs, statement, null);
		}
	}

	private static String[] getSchemaAndTable(String originTableName) {
		if (!originTableName.contains(".")) {
			String[] res = new String[2];
			res[0] = "public";
			res[1] = originTableName;
			return res;
		}

		String[] names = originTableName.split("\\.");
		if (names.length != 2) {
			throw new IllegalArgumentException("Invalid table name: " + originTableName);
		}
		if (names[0].isEmpty() || names[1].isEmpty()) {
			throw new IllegalArgumentException("Invalid table name: " + originTableName);
		}
		return names;
	}

	public static String getHolohubEndpoint(String fe, String accessId, String accessKey, String db) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = getConnection(
					getJdbcUrl(fe, db),
					accessId,
					accessKey);

			stmt = conn.createStatement();
			rs = stmt.executeQuery("SHOW hg_datahub_endpoints;");

			String endpointStr = null;
			while (rs.next()) {
				endpointStr = rs.getString(1);
				break;
			}

			if (endpointStr == null || endpointStr.isEmpty()) {
				throw new RuntimeException("Realtime ingestion service (holohub) not enabled or its endpoint invalid.");
			}

			LOG.info("Got holohub endpoint from FE: {}", endpointStr);

			Map<String, String> endpointMap = new HashMap<>();
			String[] endpoints = endpointStr.split(";");
			for (String endpoint : endpoints) {
				String[] endpointElements = endpoint.split(",");
				if (endpointElements.length != 2) {
					LOG.error("Endpoint format invalid: {}. FE: {}", endpoint, fe);
					throw new RuntimeException("Realtime ingestion endpoint invalid. endpoint: " + endpointStr);
				}

				endpointMap.put(endpointElements[0], endpointElements[1]);
			}

			String feNetwork = getFeEndpointNetwork(fe);
			if (!endpointMap.containsKey(feNetwork)) {
				LOG.error("No network-matching holohub endpoint '{}' found for fe: {}",
						endpointStr, fe);
				throw new RuntimeException(String.format("No network-matching realtime ingestion endpoint '%s' found for fe: %s",
						endpointStr, fe));
			}

			String result = endpointMap.get(feNetwork);
			LOG.info("Choose holohub endpoint: {}, FE: {}, network: {}", result, fe, feNetwork);
			return result;
		} finally {
			closeDBResources(rs, stmt, conn);
		}
	}

	private static String getFeEndpointNetwork(String fe) {
		String firstDomain = fe.split("\\.")[0];
		if (firstDomain.endsWith("-vpc")) {
			return "vpc";
		}
		if (firstDomain.endsWith("-internal")) {
			return "intranet";
		}
		return "internet";
	}

	public static Connection getConnection(String jdbcUrl, String user, String password) throws Exception {

		return RetryUtil.executeWithRetry(new Callable<Connection>() {
			@Override
			public Connection call() throws Exception {
				Properties prop = new Properties();
				prop.put("user", user);
				prop.put("password", password);

				DriverManager.setLoginTimeout(60);
				return DriverManager.getConnection(jdbcUrl, prop);
			}
		}, 9, 1000L, true);
	}

	public static String getJdbcUrl(String endpoint, String db) {
		return getJdbcUrl(endpoint, db, 60, 60);
	}

	public static String getJdbcUrl(String endpoint, String db, int readTimeoutSec, int connectTimeoutSec) {
		return String.format("jdbc:postgresql://%s/%s?socketTimeout=%d&connectTimeout=%d&sslmode=disable",
				endpoint, db, readTimeoutSec, connectTimeoutSec);
	}

	public static void closeDBResources(ResultSet rs, Statement stmt,
										Connection conn) {
		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException unused) {
			}
		}

		if (null != stmt) {
			try {
				stmt.close();
			} catch (SQLException unused) {
			}
		}

		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException unused) {
			}
		}
	}

	// System Timezone
	public static LocalDate convertColumnToLocalDate(String str) {

		// Try parse as date format first
		try {
			LocalDate ld = LocalDate.parse(str, DateTimeFormatter.ISO_DATE);
			return ld;
		} catch (DateTimeParseException e) {
		}

		LOG.error("Failed to parse column as DATE. str: {}", str);
		throw new RuntimeException(
				String.format("String [%s] cannot be converted to date.", str));
	}

	// For Timestamp
	public static LocalDateTime convertColumnToLocalDateTime(String str) {
		try {
			LocalDateTime dt = LocalDateTime.parse(str, DATE_TIME_FORMATTER);
			return dt;
		} catch (DateTimeParseException e) {
		}

		LOG.error("Failed to parse column as TIMESTAMP. str: {}", str);
		throw new RuntimeException(
				String.format("Data [%s] cannot be converted to timestamptz.", str));
	}

	// For timestamp and timestamptz
	public static ZonedDateTime convertColumnToZonedDateTime(String str) {
		try {
			ZonedDateTime dt = ZonedDateTime.parse(str, DATE_TIME_FORMATTER);
			return dt;
		} catch (DateTimeParseException e) {
		}

		try {
			LocalDateTime dt = LocalDateTime.parse(str, DATE_TIME_FORMATTER);
			return dt.atZone(ZoneId.of("UTC"));
		} catch (DateTimeParseException e) {
		}

		LOG.error("Failed to parse column as TIMESTAMP. str: {}", str);
		throw new RuntimeException(
				String.format("Data [%s] cannot be converted to timestamptz.", str));
	}
}
