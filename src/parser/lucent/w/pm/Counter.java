package parser.lucent.w.pm;

/**
 * 一个counter的信息，即原始文件中的一个mt节点
 * 
 * @author ChenSijiang 2010-9-9
 */
class Counter {

	/**
	 * 在原始文件中的名字
	 */
	String sourceName = "";

	/**
	 * 短名，即CLT表中的列名
	 */
	String shortName = "";

	/**
	 * moid类型，即moid节点内容中，最后一个=号前的那个单词<br />
	 * 比如，moid中的内容是:<br />
	 * RncFunction=0,UtranCell=DQ1_0411rangqutankelvchengfengercunW_BOB11<br />
	 * 那么，此时的moidType是:UtranCell
	 */
	String moidName = "";

	/**
	 * 所属的CLT表名
	 */
	String tableName = "";

	/**
	 * 是否使用此字段
	 */
	boolean isUsed;

	public Counter(String sourceName, String shortName, String moidName, String tableName/*
																						 * , boolean isUsed
																						 */) {
		super();
		this.sourceName = sourceName;
		this.shortName = shortName;
		this.moidName = moidName;
		this.tableName = tableName;
		// this.isUsed = isUsed;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (obj instanceof Counter) {
			Counter me = (Counter) obj;
			return (me.sourceName.equals(this.sourceName) && me.shortName.equals(this.shortName) && me.tableName.equals(this.tableName));
		}
		return false;
	}

	@Override
	public String toString() {
		return "[sourceName:" + sourceName + ", shortName:" + shortName + ", tableName:" + tableName + "]";
	}
}
