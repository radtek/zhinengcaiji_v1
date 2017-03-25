package util;

/**
 * 描述列类型
 * 
 * @author ChenSijiang
 * @since 1.0
 */
public class ColumnType {

	private String columnName; // 列名

	private String type; // 类型

	private String format; // 格式

	public ColumnType() {
		super();
	}

	public ColumnType(String type, String format, String cn) {
		super();
		this.type = type;
		this.format = format;
		columnName = cn;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj instanceof ColumnType) {
			ColumnType ct = (ColumnType) obj;
			return ct.getColumnName().equals(columnName) && ct.getFormat().equals(format) && ct.getType().equals(type);
		}

		return false;
	}
	
	@Override
	public int hashCode(){
		return this.columnName.hashCode() + this.format.hashCode() + this.type.hashCode();
	}
}
