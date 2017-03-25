package parser.eric.pm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 表元数据调整器 -- 联通二期W爱立信性能文件解析
 * 
 * @author YangJian
 * @since 1.0
 */
public class TableMetaAdjuster {

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String TABLE_PREFIX = "CLT_PM_W_ERIC_"; // 表名前缀

	private SqlldrParam table; // 表信息及数据

	private Map<String, ColumnMapInfo> columnMaps = new HashMap<String, ColumnMapInfo>(); // 列名映射关系

	public TableMetaAdjuster(SqlldrParam param) {
		this.table = param;
	}

	/**
	 * 调整表元数据
	 */
	public void adjust() {
		adjustTableName();

		// 加载所有列映射信息
		loadAllColumns();

		List<String> columnNames = mapKey2List(table.columnMap); // 把Map的key转化成一个list，因为下面要修改Map，否则会有concurrentModify异常
		for (String columnName : columnNames) {
			// 如果从数据库加载上来的映射表中包含原始字段，则存在映射关系，并应用映射关系
			if (columnMaps.containsKey(columnName)) {
				// 如果采集到数据中的字段名和数据库中的短名一致(说明原始名和短名是一样的)，则不操作
				String shortName = columnMaps.get(columnName).shortName;
				if (shortName.equalsIgnoreCase(columnName)) {
				} else {
					// 如果不一致，则以数据库中映射表的短名为准重新构建table.columnMap
					int index = table.columnMap.get(columnName);
					table.columnMap.remove(columnName);
					table.columnMap.put(shortName, index);
				}
			} else {
				logger.debug("发现映射表中没有定义的新指标: " + columnName);
				// 不包含则说明是新增加的指标，需要动态添加到映射表中并且需要增加对应表的此字段

				// 第一步：修改表结构，增加一个字段
				String newColName = newColumnName(columnName); // 获取新的命名,不超过指定长度则是原来的名称
				addCol(newColName); // 给表增加此字段
				// 重构一下table.columnMap
				int index = table.columnMap.get(columnName);
				table.columnMap.remove(columnName);
				table.columnMap.put(newColName, index);

				// 第二步: 把字段映射关系添加到映射表中
				addColToMapTable(columnName, newColName);

				// 第三步： 添加到此columnMaps中
				columnMaps.put(newColName, new ColumnMapInfo(columnName, newColName));
			}
		}

		existsOrCreateTable();
	}

	private List<String> mapKey2List(Map<String, Integer> map) {
		List<String> lst = new ArrayList<String>();
		Set<String> keys = map.keySet();
		for (String key : keys) {
			lst.add(key);
		}
		return lst;
	}

	/**
	 * 调整表名
	 */
	private void adjustTableName() {
		// 加表的前缀并且判断不能超过30位大小，否则缩写
		String tbName = table.tbName;
		tbName = TABLE_PREFIX + tbName;
		if (tbName.length() > 30)
			tbName = tbName.substring(0, 30);
		table.tbName = tbName;
	}

	/**
	 * 判断表是否存在或者不存在则创建之
	 */
	private void existsOrCreateTable() {
		if (table == null || Util.isNull(table.tbName))
			return;

		StringBuilder sql = new StringBuilder();
		sql.append("create table ").append(table.tbName).append(" (");
		sql.append("OMCID NUMBER,COLLECTTIME DATE,STAMPTIME DATE,RNC_NAME VARCHAR2(100),SUBNETWORKROOT VARCHAR2(100),SUBNETWORK VARCHAR2(100),MECONTEXT VARCHAR2(100),");

		Set<String> columnNames = table.columnMap.keySet();
		for (String columnName : columnNames) {
			sql.append(columnName).append(" VARCHAR(300),");
		}
		if (sql.charAt(sql.length() - 1) == ',') {
			sql.deleteCharAt(sql.length() - 1);
		}
		sql.append(")");

		try {
			// logger.debug(sql.toString());
			CommonDB.executeUpdate(sql.toString());
		} catch (SQLException e) {
			// 表已经存在
			if (e.getErrorCode() == 955) {
				logger.debug(table.tbName + " 表已存在.");
			} else
				logger.error("建表时异常：" + sql, e);
		}
	}

	/**
	 * 加载所有此表的列映射信息
	 */
	private void loadAllColumns() {
		String sql = "SELECT COL_NAME,SHORT_COL_NAME FROM CLT_PM_W_ERIC_MAP WHERE TAB_NAME='" + table.tbName + "'";

		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String key = rs.getString("COL_NAME");
				String shortName = rs.getString("SHORT_COL_NAME");
				columnMaps.put(key, new ColumnMapInfo(key, shortName));
			}
			rs.close();
		} catch (Exception e) {
			logger.error("加载映射表列信息时异常:" + sql, e);
		} finally {
			if (con != null) {
				try {
					if (ps != null) {
						ps.close();
					}
					con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	/**
	 * 创建一新的字段名
	 * 
	 * @param col
	 *            旧字段名
	 * @return
	 */
	private String newColumnName(String col) {
		if (col.length() <= 30) {
			return col.toUpperCase();
		}

		return "COL_" + getSeqNextval();
	}

	/**
	 * 添加一个字段映射到Map表中
	 * 
	 * @param col
	 *            原始字段名
	 * @param sn
	 *            裁剪后的字段名
	 */
	private void addColToMapTable(String col, String sn) {
		String sql = "INSERT INTO CLT_PM_W_ERIC_MAP " + "(COL_NAME,SHORT_COL_NAME,TAB_NAME,STAMPTIME) VALUES " + "('%s','%s','%s',sysdate)";
		sql = String.format(sql, col, sn.toUpperCase(), table.tbName.toUpperCase());

		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("向映射表添加记录时异常,原因:" + e.getMessage());
		}
	}

	/**
	 * 获取序列值
	 * 
	 * @return
	 */
	private int getSeqNextval() {
		String sql = "select SEQ_CLT_PM_W_ERIC.nextval from dual";

		int val = 0;

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				val = rs.getInt(1);
			}
		} catch (SQLException e) {
			logger.error("获取序列号时异常:" + sql, e);
			return 0;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (ps != null) {
					ps.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}

		return val;
	}

	/**
	 * 创建序列
	 * 
	 * @return
	 */
	public boolean createSeq() {
		String sql = "create sequence SEQ_CLT_PM_W_ERIC " + "minvalue 1 maxvalue 999999999999999999999999999" + " start with 1 increment by 1";
		try {
			CommonDB.executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				return true;
			} else {
				logger.error("创建序列时异常，sql:" + sql, e);
				return false;
			}
		}
	}

	/**
	 * 在表中添加一列
	 */
	private void addCol(String colName) {
		String sql = "alter table " + table.tbName + " add " + colName + " varchar2(300)";
		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			logger.error("增加列时异常:" + sql + " " + e.getMessage());
		}
	}

	/**
	 * 创建 CLT_PM_W_ERIC_MAP 表
	 * 
	 * @return
	 */
	public boolean createMapTable() {
		String sql = "CREATE TABLE CLT_PM_W_ERIC_MAP " + " (	 COL_NAME VARCHAR2(100)," + " SHORT_COL_NAME VARCHAR2(30), "
				+ " TAB_NAME VARCHAR2(30),STAMPTIME DATE,CONSTRAINT pk_CLT_PM_W_ERIC_MAP primary key (COL_NAME, TAB_NAME) ) ";
		try {
			CommonDB.executeUpdate(sql);
			return true;
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				return true;
			} else {
				logger.error("创建映射表时异常，sql:" + sql, e);
				return false;
			}
		}
	}

	/**
	 * 字段映射类
	 * 
	 * @author YangJian
	 * @since 1.0
	 */
	class ColumnMapInfo {

		String rawName; // 原始字段名

		String shortName; // 新字段名

		public ColumnMapInfo(String rawName, String shortName) {
			super();
			this.rawName = rawName;
			this.shortName = shortName;
		}
	}
}
