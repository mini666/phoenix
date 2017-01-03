/**
 * 
 */
package org.apache.phoenix.schema.rowkey;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mini666
 *
 */
public class RowKeyUtil {

	private static final Logger LOG = LoggerFactory.getLogger(RowKeyUtil.class);
	
	static final String GLOBAL_ROWKEY_DELIMTER = "global.rowkey.delimiter";
	static final String DEFAULT_ROWKEY_DELIMITER = "^";
	static final String GLOBAL_ROWKEY_VALUE_ACCESSOR = "global.rowkey.value.accessor";
	static final String EXTRAORDINARY_ROWKEY_VALUE_ACCESSOR_TABLES = "extraordinary.rowkey.value.accessor.tables";
	
	
	private static Configuration conf;
	private static List<String> extraordinaryTableList;
	
	static {
		conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
		initialize();
	}
	
	private RowKeyUtil() {}
	
	private static void initialize() {
		String[] tableNames = conf.getStrings(EXTRAORDINARY_ROWKEY_VALUE_ACCESSOR_TABLES);
		
		if (tableNames != null) {
			extraordinaryTableList = Arrays.asList(tableNames);
		} else {
			extraordinaryTableList = Collections.<String>emptyList();
		}
	}
	
	public static RowKeyValueAccessor createRowKeyValueAccessor(PTable table, int pkSlotPosition) {
		RowKeyValueAccessor rowKeyValueAccessor = null;
		
		String tableName = table.getName().getString();
		if (!isExtraOrdinaryTable(table)) {
			rowKeyValueAccessor = new RowKeyValueAccessor(table.getPKColumns(), pkSlotPosition);
		} else {
			try {
				String rowKeyValueAccessorOfTable = conf.get(tableName + ".rowkey.value.accessor");
				
				if (rowKeyValueAccessorOfTable != null) {
					rowKeyValueAccessor = Class.forName(rowKeyValueAccessorOfTable).asSubclass(RowKeyValueAccessor.class)
							.getConstructor(PTable.class, Boolean.class, Boolean.class, Integer.class, Byte.class)
							.newInstance(table, isSalt(table), hasDelimiter(table), pkSlotPosition, getSeparator(table));
				} else {
					Class<? extends RowKeyValueAccessor> rowKeyValueAccessorClass = conf.getClass(GLOBAL_ROWKEY_VALUE_ACCESSOR, DelimiterRowKeyValueAccessor.class, DelimiterRowKeyValueAccessor.class);
					rowKeyValueAccessor = rowKeyValueAccessorClass
							.getConstructor(PTable.class, Boolean.class, Boolean.class, Integer.class, Byte.class)
							.newInstance(table, isSalt(table), hasDelimiter(table), pkSlotPosition, getSeparator(table));
				}
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				rowKeyValueAccessor = new RowKeyValueAccessor(table.getPKColumns(), pkSlotPosition);
			}
		}
		
		return rowKeyValueAccessor;
	}
	
	public static boolean isSalt(PTable table) {
		Integer bucketNum = table.getBucketNum();
		return bucketNum == null ? false : true;
	}
	
	public static boolean hasDelimiter(PTable table) {
		return table.getPKColumns().size() > 1 || isSalt(table);
	}
	
	public static boolean isExtraOrdinaryTable(PTable table) {
		return extraordinaryTableList.contains(table.getName().getString());
	}
	
	public static byte getSeparator(PTable table) {
		return Bytes.toBytes(conf.get(GLOBAL_ROWKEY_DELIMTER, conf.get(table.getName().getString() + ".rowkey.delimiter", DEFAULT_ROWKEY_DELIMITER)))[0];
	}
	
	// For Testing
	static void setConf(Configuration conf) {
		RowKeyUtil.conf = conf;
		initialize();
	}
}
