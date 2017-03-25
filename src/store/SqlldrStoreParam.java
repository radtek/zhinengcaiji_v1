package store;

import templet.Table;

/**
 * sqlldr入库方式需要的参数
 * 
 * @author YangJian
 * @since 3.1
 * @see SqlldrStore
 */
public class SqlldrStoreParam extends StoreParam {

	private int templetID; // 对应分发模板中 templet 的id属性

	private Table table; // 对应分发模板中 templet节点下面的一个table节点

	public SqlldrStoreParam() {
		super();
	}

	public SqlldrStoreParam(int templetID, Table table) {
		super();
		this.templetID = templetID;
		this.table = table;
	}

	public int getTempletID() {
		return templetID;
	}

	public void setTempletID(int templetID) {
		this.templetID = templetID;
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

}
