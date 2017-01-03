/**
 * 
 */
package org.apache.phoenix.schema.rowkey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Salting 되었거나 복합키의 경우 무조건 구분자를 사용하였다고 가정.
 * 
 * @author mini666
 *
 */
public class DelimiterRowKeyValueAccessor extends RowKeyValueAccessor {
	protected Logger log = LoggerFactory.getLogger(getClass());
	
	protected boolean hasDelimiter;
	protected boolean isSalt;
	protected byte rowKeyDelimiterByte;
	
	public DelimiterRowKeyValueAccessor() {
		super();
	}

	public DelimiterRowKeyValueAccessor(PTable table, Boolean isSalt, Boolean hasDelimiter, Integer index, Byte rowKeyDelimiterByte) {
		super(table.getPKColumns(), index);
		
		this.isSalt = isSalt;
		this.hasDelimiter = hasDelimiter;
		this.rowKeyDelimiterByte = rowKeyDelimiterByte;
	}

	@Override
	public int getOffset(byte[] keyBuffer, int keyOffset) {
		int offset = super.getOffset(keyBuffer, keyOffset);
		
		if (hasDelimiter || isSalt) {
			offset += index;
		}
		
		return offset;
	}

	@Override
	public int getLength(byte[] keyBuffer, int keyOffset, int maxOffset) {
		if (!hasSeparator) {
			return maxOffset - keyOffset - (keyBuffer[maxOffset - 1] == QueryConstants.DESC_SEPARATOR_BYTE ? 1 : 0);
		}
		int offset = keyOffset;
		// FIXME: offset < maxOffset required because HBase passes bogus keys to filter to position scan (HBASE-6562)
		while (offset < maxOffset && !isSeparatorByte(keyBuffer[offset])) {
			offset++;
		}
		return offset - keyOffset;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		super.readFields(input);
		
		index = input.readInt();							// 2017-01-02 added by mini666 - phoenix에서는 client에서만 index가 필요하지만 custom 구분자 적용을 위해서는 서버에서도 필요하다.
		hasDelimiter = input.readBoolean();
		isSalt = input.readBoolean();
		rowKeyDelimiterByte = input.readByte();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		super.write(output);
		
		output.writeInt(index);							// 2017-01-02 added by mini666 - phoenix에서는 client에서만 index가 필요하지만 custom 구분자 적용을 위해서는 서버에서도 필요하다.
		output.writeBoolean(hasDelimiter);
		output.writeBoolean(isSalt);
		output.write(rowKeyDelimiterByte);
	}

	private boolean isSeparatorByte(byte b) {
		return b == rowKeyDelimiterByte || b == QueryConstants.DESC_SEPARATOR_BYTE;
	}
}
