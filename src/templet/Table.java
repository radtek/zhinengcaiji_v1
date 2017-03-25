package templet;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import framework.ConstDef;

/**
 * 分发模板中 table标签 类
 * 
 * @author YangJian
 * @since 3.1
 * @version 3.1.1 liangww 2012-06-06 把Colum修改为静态类，listColumnNamesWithType方法增加ConstDef .COLLECT_FIELD_SPECIAL_FORMAT处理
 */
public class Table {

	private int id;

	private String name;

	private String splitSign = ";"; // 默认使用分号

	private String indexOfTimeColumn;// 时间字段的列索引

	/** <column的index,Column对象> */
	private Map<Integer, Column> columns = new TreeMap<Integer, Column>(new IDComparator());

	/** <column的src,Column对象> */
	private Map<String, Column> columnMap = new TreeMap<String, Column>(new HashCodeComparator());

	private static final String ORACLE_KEYWORD = "Mode,"; // oracle关键字

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSplitSign() {
		return splitSign;
	}

	public void setSplitSign(String splitSign) {
		this.splitSign = splitSign;
	}

	public Map<Integer, Column> getColumns() {
		return columns;
	}

	public String getIndexOfTimeColumn() {
		return indexOfTimeColumn;
	}

	public void setIndexOfTimeColumn(String indexOfTimeColumn) {
		this.indexOfTimeColumn = indexOfTimeColumn;
	}

	public void setColumns(Map<Integer, Column> columns) {
		this.columns = columns;
	}

	public void addColumns(Map<Integer, Column> columns) {
		this.columns.putAll(columns);
	}

	public Map<String, Column> getColumnMap() {
		return columnMap;
	}

	public void setColumnMap(Map<String, Column> columnMap) {
		this.columnMap = columnMap;
	}

	/**
	 * 列出所有的字段名
	 * 
	 * @param splitSign
	 *            字段之间以分隔符隔开
	 * @return 返回值中最后一个字符为指定的分隔符
	 */
	public String listColumnNames(String splitSign) {
		StringBuilder sb = new StringBuilder();
		Collection<Column> cols = columns.values();
		for (Column col : cols) {

			sb.append(wrapName(col.getName())).append(splitSign);
		}

		return sb.toString();
	}

	private String wrapName(String name) {
		if (ORACLE_KEYWORD.indexOf(name) != -1)
			name = "\"" + name + "\"";

		return name;
	}

	/**
	 * 列出所有的字段名 --并带上类型
	 * 
	 * @param splitSign
	 * @return
	 */
	public String listColumnNamesWithType(String splitSign) {
		StringBuilder sb = new StringBuilder();
		Collection<Column> cols = columns.values();
		int index = 0;
		for (Column col : cols) {
			String colName = wrapName(col.getName());
			int type = col.getType();
			String format = col.getFormat();
			if (type == 2) // 表示字符类型（当sqlldr中字符串大于255的时候必须申明大小），必须加上格式,格式为长度
			{
				sb.append(colName).append(" CHAR(").append(format).append(") ").append(splitSign);
			} else if (type == 3 || type == 7) // 表示时间类型，必须加上格式
			{
				// String format = col.getFormat();
				sb.append(colName).append(" Date '").append(format).append("'").append(splitSign);
			} else if (type == ConstDef.COLLECT_FIELD_DATATYPE_LOB) // lob类型
			{
				String filler = "filler_" + (index++);
				sb.append(filler).append(" filler char(9999999)").append(splitSign).append(colName)
						.append(" LOBFILE(" + filler + ") TERMINATED BY EOF ").append(splitSign);

			} else if (type == ConstDef.COLLECT_FIELD_TO_NUMBER) {
				sb.append(colName).append(" \"to_number(:").append(colName).append(")\"").append(splitSign);
			}
			// liangww modify 2012-06-06 修改为其它的方式 时 追加format
			else if (type == ConstDef.COLLECT_FIELD_SPECIAL_FORMAT) {
				sb.append(colName);
				if (format != null) {
					sb.append(" ").append(format);
				}
				sb.append(splitSign);
			} else {
				sb.append(colName).append(splitSign);
			}
		}

		return sb.toString();
	}

	/** 对应分发模板中column标签 */
	public static class Column {

		/*
		 * 以下属性都是对应采集表的字段信息
		 */
		private String name;// 字段名

		private int index;// 此字段在分发模板对应的索引

		private int type;// 此字段的自定义类型(如1:其它,2:字符串,3:日期,4:Clob)

		private String format;// 日期格式

		// add by liuwx
		private int srcIndex;// 对方数据库表字段id(此字段对应厂家数据库字段的索引)

		private boolean ignore = false;

		private int srcType;// 厂家字段类型

		private String src;// 数据来源

		//
		public Column(String name, int index, int type, String format) {
			this.name = name;
			this.index = index;
			this.type = type;
			this.format = format;
		}

		public Column() {

		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public String getFormat() {
			return format;
		}

		public void setFormat(String format) {
			this.format = format;
		}

		public int getExtIndex() {
			return srcIndex;
		}

		public void setExtIndex(int extIndex) {
			this.srcIndex = extIndex;
		}

		public int getSrcType() {
			return srcType;
		}

		public void setSrcType(int srcType) {
			this.srcType = srcType;
		}

		public boolean isIgnore() {
			return ignore;
		}

		public void setIgnore(boolean ignore) {
			this.ignore = ignore;
		}

		public String getSrc() {
			return src;
		}

		public void setSrc(String src) {
			this.src = src;
		}

	}

	public class HashCodeComparator implements Comparator<Object> {

		@Override
		public int compare(Object o1, Object o2) {
			return ((Integer) (o1.hashCode())).compareTo((Integer) (o2.hashCode()));
		}
	}
}
