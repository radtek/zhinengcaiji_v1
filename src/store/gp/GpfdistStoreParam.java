package store.gp;

import java.sql.Connection;
import java.util.List;

import store.StoreParam;
import templet.Table;

/**
 * 
 * GpfdistStoreParam
 * 
 * @author liangww 2012-6-30
 * @version 1.0<br>
 *          1.0.1 liangww 2012-09-12 增table成员，删除tableName成员<br>
 */
public class GpfdistStoreParam extends StoreParam {

	private Connection con = null;				// gp connection

	private String gpfdistRootUrl = null;		// gpfdist 根目录

	private String dir = null;				// 数据文件生成的位置

	private Table table;

	public Connection getCon() {
		return con;
	}

	public void setCon(Connection con) {
		this.con = con;
	}

	public String getGpfdistRootUrl() {
		return gpfdistRootUrl;
	}

	public void setGpfdistRootUrl(String gpfdistRootUrl) {
		this.gpfdistRootUrl = gpfdistRootUrl;
	}

	public String getDir() {
		return dir;
	}

	public void setDir(String dir) {
		this.dir = dir;
	}

	public String getTableName() {
		return this.table.getName();
	}

	public String getSplit() {
		return this.table.getSplitSign();
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

}
