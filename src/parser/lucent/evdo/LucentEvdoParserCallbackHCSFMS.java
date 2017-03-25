package parser.lucent.evdo;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 朗讯evdo话单解释回调实现。
 * 
 * @author ChenSijiang 2009.01.26
 * @since 1.0
 */
public class LucentEvdoParserCallbackHCSFMS implements EvdoParserCallback {

	private String omcId;

	private EvdoSqlldrUtil sqlldr;

	private long taskId;

	private String stamptime;

	private final List<String> CARR_COLS = new ArrayList<String>();

	private final List<String> HCS_COLS = new ArrayList<String>();

	private final Map<String, List<String>> COLS = new HashMap<String, List<String>>();

	private boolean isCreatedMapping; // 是否已经创建了字段表

	// 入库表名，条数
	private final Map<String, Integer> COUNT = new HashMap<String, Integer>();

	// 避免频繁查询字段映射表，使用此map缓存已查过的字段 <列名,id>
	private final Map<String, String> COL_CACHE = new HashMap<String, String>();

	// insert语句
	private final List<String> INSERT_CARR = new ArrayList<String>();

	private final List<String> INSERT_HCS = new ArrayList<String>();

	// 已经创建了的表的名字放入此List，以方便判断是否已经创建表
	// private final List<String> TABLES = new ArrayList<String>();

	// 执行insert语句间隔条数，每100条执行并提交一次
	private static final int INSERT_INTERVAL = 50;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private String stime = null;

	@Override
	public void handleData(List<EvdoRecordPair> records, String owner, String id, String omcId, String stampTime, long taskID) {
		owner = "CLT_PM_LUC_EVDO_" + owner + "_HCSFMS";
		if (this.omcId == null) {
			this.omcId = omcId;
		}
		if (stime == null) {
			stime = stampTime;
		}
		this.taskId = taskID;
		this.stamptime = stampTime;
		try {
			if (sqlldr == null)
				sqlldr = new EvdoSqlldrUtil(this.taskId, Integer.parseInt(this.omcId), new Timestamp(Util.getDate1(this.stamptime).getTime()));
		} catch (Exception unused) {
		}

		if (records == null) // 数据已解完
		{
			// executeInsert(true, owner);
			sqlldr.commit();
			// try
			// {
			// Iterator<String> it = COUNT.keySet().iterator();
			// while (it.hasNext())
			// {
			// String tn = it.next();
			// int count = COUNT.get(tn);
			// dbLogger.log(Integer.parseInt(this.omcId), tn,
			// Util.getDate1(stime), count, taskID);
			// }
			// }
			// catch (Exception e)
			// {
			// logger.error("记录数据库日志是异常", e);
			// }

			return;
		}

		if (!isCreatedMapping) {
			createMappingSeq();
			createMappingTable();
		}

		if (isCreatedMapping && createTable(records, owner)) {
			if (CARR_COLS.size() == 0) {
				loadCarrCols();
			}
			if (HCS_COLS.size() == 0) {
				loadHcsCols();
			}
			COLS.put("CLT_PM_LUC_EVDO_CARR_HCSFMS", CARR_COLS);
			COLS.put("CLT_PM_LUC_EVDO_HCS_HCSFMS", HCS_COLS);
			String insertSql = createInsert(records, owner, id);
			// if ( owner.contains(LucentEvdoParserHCSFMS.HCS) )
			// {
			// INSERT_HCS.add(insertSql);
			// }
			// else if ( owner.contains(LucentEvdoParserHCSFMS.CARR) )
			// {
			// INSERT_CARR.add(insertSql);
			// }
			//
			// executeInsert(false, owner);
		}
	}

	/* 创建映射表序列 */
	private boolean createMappingSeq() {
		String sql = "create sequence clt_evdo_mapping_seq minvalue 1 " + "maxvalue 999999999999999999999999999 start with 1 increment by 1 cache 20";
		try {
			CommonDB.executeUpdate(sql);
		} catch (Exception e) {
			if (e instanceof SQLException) {
				SQLException sqlex = (SQLException) e;
				// logger.error("SQLException errorCode:" +
				// sqlex.getErrorCode());
				if (sqlex.getErrorCode() == 955) {
					return true;
				}
			}
			logger.error("创建字段表序列时异常", e);
			return false;
		}
		return true;
	}

	/* 创建列名映射表 */
	private boolean createMappingTable() {
		String sql = "create table clt_conf_luc_evdo_col_mapping (col_id varchar(50),col_name varchar(200),short_name varchar(30))";
		try {
			CommonDB.executeUpdate(sql);
			isCreatedMapping = true;
		} catch (Exception e) {
			if (e instanceof SQLException) {
				SQLException sqlex = (SQLException) e;
				// logger.error("SQLException errorCode:" +
				// sqlex.getErrorCode());
				if (sqlex.getErrorCode() == 955) {
					isCreatedMapping = true;
					return true;
				}
			}
			logger.error("创建字段表时异常", e);
			isCreatedMapping = false;
			return false;
		}
		return true;
	}

	/*
	 * 创建表，成功则返回true
	 */
	private boolean createTable(List<EvdoRecordPair> records, String tableName) {
		return true;
		/*
		 * if ( TABLES.contains(tableName) ) { // logger.debug("此表已存在，不再创建：" + tableName); return true; } logger.debug("开始创建表"); StringBuilder sql =
		 * new StringBuilder(); sql.append("create table ").append(tableName).append(" (\r\n"); sql.append(
		 * "\"OMCID\" NUMBER,\"COLLECTTIME\" date,\"STAMPTIME\" date,\"ID\" number(13,2),\r\n" ); for (int i = 0; i < records.size(); i++) { String
		 * colName = records.get(i).getName(); // 以下做字段名的映射处理 String tmp = getColShortName(colName); if ( tmp == null ) { colName =
		 * addColName(colName); } else { colName = tmp; } sql.append("\"").append (colName).append("\"").append(" number(13,2)"); if ( i !=
		 * records.size() - 1 ) { sql.append(","); } sql.append("\r\n"); } sql.append(")"); // logger.debug("建表语句为：\n" + sql); try {
		 * CommonDB.executeInsert(sql.toString()); TABLES.add(tableName); logger.info("创建表成功:" + tableName); return true; } catch (Exception e) { if (
		 * e instanceof SQLException ) { SQLException sqlex = (SQLException) e; logger.error("SQLException errorCode:" + sqlex.getErrorCode()); if (
		 * sqlex.getErrorCode() == 955 ) { logger.info("数据库中已存在此表：" + tableName); TABLES.add(tableName); return true; } } logger.error("创建表失败，表名：" +
		 * tableName + "， 信息：" + e.getMessage()); return false; }
		 */
	}

	/* 创建insert语句 */
	private String createInsert(List<EvdoRecordPair> records, String tableName, String id) {
		sqlldr.addRow(records, tableName, id, COLS.get(tableName));
		// StringBuilder sql = new StringBuilder();
		// sql.append("insert into ").append(tableName).append(" ( \"OMCID\", \"COLLECTTIME\", \"STAMPTIME\",  \"ID\",");
		// for (int i = 0; i < records.size(); i++)
		// {
		// String col = records.get(i).getName();
		// String sn = getColShortName(col);
		//
		// if ( tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_CARR_HCSFMS")
		// && CARR_COLS.contains(sn.toUpperCase()) )
		// {
		// sql.append("\"").append(sn).append("\"");
		// if ( i != records.size() - 1 )
		// {
		// sql.append(",");
		// }
		// }
		// else if ( tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_HCS_HCSFMS")
		// && HCS_COLS.contains(sn.toUpperCase()) )
		// {
		// sql.append("\"").append(sn).append("\"");
		// if ( i != records.size() - 1 )
		// {
		// sql.append(",");
		// }
		// }
		// else if ( !tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_HCS_HCSFMS")
		// && !tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_CARR_HCSFMS") )
		// {
		// sql.append("\"").append(sn).append("\"");
		// if ( i != records.size() - 1 )
		// {
		// sql.append(",");
		// }
		// }
		//
		// }
		//
		// if ( sql.toString().endsWith(",") )
		// {
		// sql.deleteCharAt(sql.length() - 1);
		// }
		//
		// sql.append(") values (").append(omcId).append(",sysdate,to_date('").append(stime).append("','YYYY-MM-DD HH24:MI:SS'),").append(id).append(",");
		//
		// for (int i = 0; i < records.size(); i++)
		// {
		// String value = records.get(i).getValue();
		// String sn = getColShortName(records.get(i).getName());
		//
		// if ( tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_CARR_HCSFMS")
		// && CARR_COLS.contains(sn.toUpperCase()) )
		// {
		// sql.append(value);
		// if ( i != records.size() - 1 )
		// {
		// sql.append(",");
		// }
		// }
		// else if ( tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_HCS_HCSFMS")
		// && HCS_COLS.contains(sn.toUpperCase()) )
		// {
		// sql.append(value);
		// if ( i != records.size() - 1 )
		// {
		// sql.append(",");
		// }
		// }
		// else if ( !tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_HCS_HCSFMS")
		// && !tableName.equalsIgnoreCase("CLT_PM_LUC_EVDO_CARR_HCSFMS") )
		// {
		// sql.append(value);
		// if ( i != records.size() - 1 )
		// {
		// sql.append(",");
		// }
		// }
		// }
		//
		// if ( sql.toString().endsWith(",") )
		// {
		// sql.deleteCharAt(sql.length() - 1);
		// }
		//
		// sql.append(")");
		// String s = sql.toString();
		// // logger.debug("insert: " + s);
		// return s;
		return null;
	}

	/*
	 * 执行insert语句 now : 是否立即插入，不做判断
	 */
	private void executeInsert(boolean now, String tableName) {
		if (now) {
			executeBatch(INSERT_CARR, tableName);
			executeBatch(INSERT_HCS, tableName);
		} else {
			if (INSERT_CARR.size() % INSERT_INTERVAL == 0) {
				executeBatch(INSERT_CARR, tableName);
			}
			if (INSERT_HCS.size() % INSERT_INTERVAL == 0) {
				executeBatch(INSERT_HCS, tableName);
			}
		}
	}

	/* 执行批量sql */
	private void executeBatch(List<String> inserts, String tableName) {
		Connection connection = DbPool.getConn();
		Statement statement = null;

		try {
			connection.setAutoCommit(false);
			statement = connection.createStatement();
			for (String sql : inserts) {
				statement.addBatch(sql);
			}
			statement.executeBatch();

			if (COUNT.containsKey(tableName)) {
				COUNT.put(tableName, COUNT.get(tableName) + inserts.size());
			} else {
				COUNT.put(tableName, inserts.size());
			}
		} catch (Exception e) {
			logger.error("插入数据时出现异常 ", e);
		} finally {
			try {
				inserts.clear();
				if (connection != null) {
					connection.commit();
				}
				if (statement != null) {
					statement.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}
	}

	/*
	 * 在字段表里查出字段名的id
	 */
	private String getColShortName(String colName) {
		String colId = COL_CACHE.get(colName);
		if (colId != null) {
			return colId;
		}
		Connection con = DbPool.getConn();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement("select short_name from clt_conf_luc_evdo_col_mapping where col_name=?");
			ps.setString(1, colName);
			rs = ps.executeQuery();
			if (rs.next()) {
				colId = rs.getString(1);
			}
			COL_CACHE.put(colName, colId);
		} catch (Exception e) {
			logger.error("获取列ID时异常", e);
			return null;
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

		if (colId == null) {
			colId = addColName(colName);
		}

		return colId;
	}

	/*
	 * 向字段表中添加一个字段名
	 */
	private String addColName(String colName) {
		Connection con = DbPool.getConn();
		PreparedStatement ps = null;
		ResultSet rs = null;

		String seq = getSeq();
		String shortName = null;
		if (colName.length() <= 30) {
			shortName = colName;
		} else {
			shortName = colName.substring(0, 26) + seq;
		}

		try {
			ps = con.prepareStatement("insert into clt_conf_luc_evdo_col_mapping values" + " ('" + "COL_" + seq + "',?,?)");
			ps.setString(1, colName);
			ps.setString(2, shortName);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("添加字段名时异常");
			return null;
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

		String id = getColShortName(colName);

		COL_CACHE.put(colName, id);

		return id;
	}

	private void loadCarrCols() {
		String sql = "select * from CLT_PM_LUC_EVDO_CARR_HCSFMS ";

		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			meta = rs.getMetaData();
			int count = meta.getColumnCount();
			for (int i = 0; i < count; i++) {
				CARR_COLS.add(meta.getColumnName(i + 1));
			}
		} catch (Exception e) {
			CARR_COLS.clear();
		} finally {
			try {
				rs.close();
				st.close();
				con.close();
			} catch (Exception e) {
			}
		}
	}

	private void loadHcsCols() {
		String sql = "select   * from  CLT_PM_LUC_EVDO_HCS_HCSFMS ";

		Connection con = DbPool.getConn();
		Statement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			st = con.createStatement();
			rs = st.executeQuery(sql);
			meta = rs.getMetaData();
			int count = meta.getColumnCount();
			for (int i = 0; i < count; i++) {
				HCS_COLS.add(meta.getColumnName(i + 1));
			}
		} catch (Exception e) {
			HCS_COLS.clear();
		} finally {
			try {
				rs.close();
				st.close();
				con.close();
			} catch (Exception e) {
			}
		}
	}

	private String getSeq() {
		Connection con = DbPool.getConn();
		PreparedStatement ps = null;
		ResultSet rs = null;

		String seq = null;

		try {
			ps = con.prepareStatement("select clt_evdo_mapping_seq.nextval from dual");
			rs = ps.executeQuery();
			if (rs.next()) {
				seq = String.valueOf(rs.getInt(1));
			}
			return seq;
		} catch (Exception e) {
			logger.error("获取clt_evdo_mapping_seq.nextval时异常", e);
			return null;
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
	}

	public static void main(String[] args) throws Exception {
		EvdoParserCallback call = new LucentEvdoParserCallbackHCSFMS();
		LucentEvdoParserHCSFMS p = new LucentEvdoParserHCSFMS(new FileInputStream("E:\\资料\\解析\\lucent\\广州电信\\201004061100GMT.HCSFMS028"));
		p.parse(call, "119", new Timestamp(new Date().getTime()), true, 989);
	}
}
