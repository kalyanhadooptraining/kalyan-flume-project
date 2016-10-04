package com.orienit.kalyan.flume.sink;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import twitter4j.internal.org.json.JSONObject;

/**
 * An {@link HbaseEventSerializer} which parses columns based on a supplied
 * regular expression and column name list.
 * <p>
 * Note that if the regular expression does not return the correct number of
 * groups for a particular event, or it does not correctly match an event, the
 * event is silently dropped.
 * <p>
 * Row keys for each event consist of a timestamp concatenated with an
 * identifier which enforces uniqueness of keys across flume agents.
 * <p>
 * See static constant variables for configuration options.
 */
public class JsonHbaseEventSerializer implements HbaseEventSerializer {
	// Config vars
	/** Comma separated list of column names to place matching groups in. */
	public static final String COL_NAME_CONFIG = "colNames";
	public static final String COLUMN_NAME_DEFAULT = "payload";

	/** Index of the row key in matched regex groups */
	public static final String ROW_KEY_INDEX_CONFIG = "rowKeyIndex";

	/** Placeholder in colNames for row key */
	public static final String ROW_KEY_NAME = "ROW_KEY";

	/** Whether to deposit event headers into corresponding column qualifiers */
	public static final String DEPOSIT_HEADERS_CONFIG = "depositHeaders";
	public static final boolean DEPOSIT_HEADERS_DEFAULT = false;

	/** What charset to use when serializing into HBase's byte arrays */
	public static final String CHARSET_CONFIG = "charset";
	public static final String CHARSET_DEFAULT = "UTF-8";

	/*
	 * This is a nonce used in HBase row-keys, such that the same row-key never
	 * gets written more than once from within this JVM.
	 */
	protected static final AtomicInteger nonce = new AtomicInteger(0);
	protected static String randomKey = RandomStringUtils.randomAlphanumeric(10);

	protected byte[] cf;
	private byte[] payload;
	private List<byte[]> colNames = Lists.newArrayList();
	private Map<String, String> headers;
	private boolean depositHeaders;
	private Charset charset;
	private int rowKeyIndex;

	@Override
	public void configure(Context context) {
		depositHeaders = context.getBoolean(DEPOSIT_HEADERS_CONFIG, DEPOSIT_HEADERS_DEFAULT);
		charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));

		String colNameStr = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
		String[] columnNames = colNameStr.split(",");
		for (String s : columnNames) {
			colNames.add(s.getBytes(charset));
		}

		// Rowkey is optional, default is -1
		rowKeyIndex = context.getInteger(ROW_KEY_INDEX_CONFIG, -1);
		// if row key is being used, make sure it is specified correct
		if (rowKeyIndex >= 0) {
			if (rowKeyIndex >= columnNames.length) {
				throw new IllegalArgumentException(ROW_KEY_INDEX_CONFIG + " must be " + "less than num columns " + columnNames.length);
			}
			if (!ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])) {
				throw new IllegalArgumentException("Column at " + rowKeyIndex + " must be " + ROW_KEY_NAME + " and is " + columnNames[rowKeyIndex]);
			}
		}
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
		this.headers = event.getHeaders();
		this.payload = event.getBody();
		this.cf = columnFamily;
	}

	/**
	 * Returns a row-key with the following format: [time in millis]-[random
	 * key]-[nonce]
	 */
	protected byte[] getRowKey(Calendar cal) {
		/*
		 * NOTE: This key generation strategy has the following properties:
		 * 
		 * 1) Within a single JVM, the same row key will never be duplicated. 2)
		 * Amongst any two JVM's operating at different time periods (according
		 * to their respective clocks), the same row key will never be
		 * duplicated. 3) Amongst any two JVM's operating concurrently
		 * (according to their respective clocks), the odds of duplicating a
		 * row-key are non-zero but infinitesimal. This would require
		 * simultaneous collision in (a) the timestamp (b) the respective nonce
		 * and (c) the random string. The string is necessary since (a) and (b)
		 * could collide if a fleet of Flume agents are restarted in tandem.
		 * 
		 * Row-key uniqueness is important because conflicting row-keys will
		 * cause data loss.
		 */
		String rowKey = String.format("%s-%s-%s", cal.getTimeInMillis(), randomKey, nonce.getAndIncrement());
		return rowKey.getBytes(charset);
	}

	protected byte[] getRowKey() {
		return getRowKey(Calendar.getInstance());
	}

	@Override
	public List<Row> getActions() throws FlumeException {
		List<Row> actions = Lists.newArrayList();
		byte[] rowKey;

		JSONObject inputJson = new JSONObject();

		String payloadData = new String(payload, charset);

		try {
			inputJson = new JSONObject(payloadData);
		} catch (Exception e) {
			// logger.debug("payload is not proper json");
			return Lists.newArrayList();
		}

		if (inputJson.length() == 0 || inputJson.length() != colNames.size()) {
			return Lists.newArrayList();
		}

		try {
			if (rowKeyIndex < 0) {
				rowKey = getRowKey();
			} else {
				byte[] colName = colNames.get(rowKeyIndex + 1);
				rowKey = inputJson.get(Bytes.toString(colName)).toString().getBytes(Charsets.UTF_8);
			}
			Put put = new Put(rowKey);

			for (int i = 0; i < colNames.size(); i++) {
				if (i != rowKeyIndex) {
					byte[] colName = colNames.get(i);
					put.add(cf, colName, inputJson.get(Bytes.toString(colName)).toString().getBytes(Charsets.UTF_8));
				}
			}
			if (depositHeaders) {
				for (Map.Entry<String, String> entry : headers.entrySet()) {
					put.add(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
				}
			}
			actions.add(put);
		} catch (Exception e) {
			throw new FlumeException("Could not get row key!", e);
		}
		return actions;
	}

	@Override
	public List<Increment> getIncrements() {
		return Lists.newArrayList();
	}

	@Override
	public void close() {
	}
}