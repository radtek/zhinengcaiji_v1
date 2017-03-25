package tools.templet;

/**
 * 字段信息 ColumnInfo
 * 
 * @author litp
 * @since 1.0
 */
class ColumnInfo {

	String srcColumn = "";

	String destColumn = "";

	String destType = "";

	String length = "-1";

	String allowNull = "Y";

	String primaryKeyFlag = "N";

	public String getLength() {
		return length;
	}

	public void setLength(String length) {
		this.length = length;
	}

	public String getSrcColumn() {
		return srcColumn;
	}

	public void setSrcColumn(String srcColumn) {
		this.srcColumn = srcColumn;
	}

	public String getDestColumn() {
		return destColumn;
	}

	public void setDestColumn(String destColumn) {
		this.destColumn = destColumn;
	}

	public String getDestType() {
		return destType;
	}

	public void setDestType(String destType) {
		this.destType = destType;
	}

	public String getAllowNull() {
		return allowNull;
	}

	public void setAllowNull(String allowNull) {
		this.allowNull = allowNull;
	}

	public String getPrimaryKeyFlag() {
		return primaryKeyFlag;
	}

	public void setPrimaryKeyFlag(String primaryKeyFlag) {
		this.primaryKeyFlag = primaryKeyFlag;
	}

}
