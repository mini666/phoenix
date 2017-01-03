/**
 * 
 */
package org.apache.phoenix.schema.rowkey;

import java.sql.Types;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.DelegateColumn;
import org.apache.phoenix.schema.DelegateTable;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.types.PDataType;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author mini666
 *
 */
public class RowKeyUtilTest {

	private Configuration conf;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		conf = HBaseConfiguration.create();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateRowKeyValueAccessor() {
		// SYSTEM 테이블 테스트
		PTable systemTable = createSimpleRowKeyTable("SYSTEM.CATALOG", 0);
		RowKeyValueAccessor accessor = RowKeyUtil.createRowKeyValueAccessor(systemTable, 0);
		Assert.assertThat(accessor, CoreMatchers.isA(RowKeyValueAccessor.class));
		
		// 일반 테이블.
		String tableName = "test";
		PTable testTable = createSimpleRowKeyTable(tableName, 0);
		
		conf.set(RowKeyUtil.EXTRAORDINARY_ROWKEY_VALUE_ACCESSOR_TABLES, tableName);
		RowKeyUtil.setConf(conf);
		
		accessor = RowKeyUtil.createRowKeyValueAccessor(testTable, 0);
		Assert.assertThat(accessor instanceof DelimiterRowKeyValueAccessor, CoreMatchers.is(true));
	}

	private TestTable createSimpleRowKeyTable(String tableName, int bucketNum) {
		TestTable testTable = new TestTable(tableName, Lists.newArrayList((PColumn)new TestColumn("pk1", Types.VARCHAR, null)), bucketNum, null);
		
		return testTable;
	}
	
	class TestTable extends DelegateTable {
		
		private String name;
		private List<PColumn> pkColumnList;
		private int bucketNum;
		
		public TestTable(String name, List<PColumn> pkColumnList, int bucketNum, PTable delegate) {
			super(delegate);
			this.name = name;
			this.pkColumnList = pkColumnList;
			this.bucketNum = bucketNum;
		}

		@Override
		public PName getName() {
			return new PName() {

				@Override
				public String getString() {
					return name;
				}

				@Override
				public byte[] getBytes() {
					return null;
				}

				@Override
				public ImmutableBytesPtr getBytesPtr() {
					return null;
				}

				@Override
				public int getEstimatedSize() {
					return 0;
				}
			};
		}

		@Override
		public List<PColumn> getPKColumns() {
			return pkColumnList;
		}

		@Override
		public Integer getBucketNum() {
			return bucketNum == 0 ? null : bucketNum;
		}
	}
	
	class TestColumn extends DelegateColumn {

		private String name;
		private int type;
		
		public TestColumn(String name, int type, PColumn delegate) {
			super(delegate);
			this.name = name;
			this.type = type;
		}

		@Override
		public PDataType getDataType() {
			return PDataType.fromTypeId(type);
		}
	}
}
