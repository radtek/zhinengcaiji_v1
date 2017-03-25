package parser.eric.pm;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 爱立信原始COUNTER文件解析接口实现。
 * 
 * @author 陈思江 2010-3-1
 */
public class EricssonPmParserImp implements EricssonPmParser {

	// STAX解析器
	private XMLStreamReader xmlReader;

	private int omcId;

	// 以下5个是公有字段，同一个原始文件中，所有表都是同样的值．
	private String publicFFV = "";

	private String publicSN = "";

	private String publicST = "";

	private String publicVN = "";

	private String publicCBT = "";

	// 以下6个公有字段，每个表有独有的值．
	private String publicNEUN = "";

	private String publicNEDN = "";

	private String publicNESW = "";

	private String publicMTS = "";

	private String publicGP = "";

	private String publicMOID = "";

	private String tableName;// 当前表名

	private String stamptime;

	private static final Map<String, String> COL_MAPS = new HashMap<String, String>();

	// 存放已创建或已存在的表名
	private final List<String> TABLES = new ArrayList<String>();

	private final Map<String, Integer> COUNT = new HashMap<String, Integer>(); // 计数

	// 缓存<字段表,表名>,避免频繁查询数据库
	private final Map<String, String> BUFFER = new HashMap<String, String>();

	// private static final String TABLE_NAME = "clt_pm_w_eric_meas_"; // 表名

	private static final String MAP_TABLE_NAME = "clt_pm_w_eric_meas_map"; // 表名－列名映射表

	private static final String MAP_SEQ_NAME = "SEQ_CLT_CONF_PM_ERIC_MAP"; // 序列名

	private final List<String> INSERTS = new ArrayList<String>();

	// logger
	private final Logger logger = LogMgr.getInstance().getSystemLogger();

	// 数据库日志
	private final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static final int INTERVAL = 100; // insert 间隔

	// ///////////////////////////
	// 以下是一些标签名和属性名////
	// //////////////////////////
	private static final String FFV = "ffv";

	private static final String SN = "sn";

	private static final String ST = "st";

	private static final String VN = "vn";

	private static final String CBT = "cbt";

	private static final String MD = "md";

	private static final String NEUN = "neun";

	private static final String NEDN = "nedn";

	private static final String NESW = "nesw";

	private static final String MTS = "mts";

	private static final String GP = "gp";

	private static final String MT = "mt";

	private static final String R = "r";

	private static final String MOID = "moid";

	static {
		COL_MAPS.put("pmEulHarqTransmTti2Srb", "clt_pm_w_eric_meas_1175");
		COL_MAPS.put("pmSamplesPacketLatencyPsStreamHs", "clt_pm_w_eric_meas_1369");
		COL_MAPS.put("pmNoOutgoingEulHardHoSuccess", "clt_pm_w_eric_meas_1556");
		COL_MAPS.put("pmUlTrafficVolumePsStr32", "clt_pm_w_eric_meas_1591");
		COL_MAPS.put("pmSamplesHsDlRlcTotPacketThp", "clt_pm_w_eric_meas_1487");
		COL_MAPS.put("pmNoSuccOutCnhhoSpeech", "clt_pm_w_eric_meas_1692");
		COL_MAPS.put("pmSamplesEulRlcTotPacketThp", "clt_pm_w_eric_meas_1362");
		COL_MAPS.put("pmNoSuccessOutIratHoSpeech", "clt_pm_w_eric_meas_1500");
	}

	@Override
	public void parse(String file, int omcId, Timestamp stampTime, int taskID) throws EricssonPmParserException {
		this.omcId = omcId;
		this.stamptime = Util.getDateString(stampTime);
		InputStream in = null;
		try {
			in = new FileInputStream(file);
			xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(in);

			int eventCode = xmlReader.getEventType();
			boolean isMTEnd = false; // 当前md是否结束
			List<String> mtList = new ArrayList<String>(); // 存放一个表的字段

			List<String> rList = new ArrayList<String>(); // 存放一个表的值

			createMapSeq();
			createMapTable();

			while (true) {
				switch (eventCode) {
					case XMLStreamConstants.START_ELEMENT :
						// 读取公有字段<ffv>
						if (xmlReader.getLocalName().equals(FFV)) {
							publicFFV = xmlReader.getElementText();
						}
						// 读取公有字段<sn>
						else if (xmlReader.getLocalName().equals(SN)) {
							publicSN = xmlReader.getElementText();
						}
						// 读取公有字段<st>
						else if (xmlReader.getLocalName().equals(ST)) {
							publicST = xmlReader.getElementText();
						}
						// 读取公有字段<vn>
						else if (xmlReader.getLocalName().equals(VN)) {
							publicVN = xmlReader.getElementText();
						}
						// 读取公有字段<cbt>
						else if (xmlReader.getLocalName().equals(CBT)) {
							publicCBT = xmlReader.getElementText();
						}
						// 遇到<md>标签，表示一个表的内容开始了．
						else if (xmlReader.getLocalName().equals(MD)) {
						}
						// 获取<neun>内容
						else if (xmlReader.getLocalName().equals(NEUN)) {
							publicNEUN = xmlReader.getElementText();
						}
						// 获取<nedn>内容
						else if (xmlReader.getLocalName().equals(NEDN)) {
							publicNEDN = xmlReader.getElementText();
						}
						// 获取<nesw>内容
						else if (xmlReader.getLocalName().equals(NESW)) {
							publicNESW = xmlReader.getElementText();
						}
						// 获取<mts>内容
						else if (xmlReader.getLocalName().equals(MTS)) {
							publicMTS = xmlReader.getElementText();
						}
						// 获取<gp>内容
						else if (xmlReader.getLocalName().equals(GP)) {
							publicGP = xmlReader.getElementText();
						}
						// 获取<moid>内容
						else if (xmlReader.getLocalName().equals(MOID)) {
							publicMOID = xmlReader.getElementText();
						}
						// 取<mt>内容
						else if (xmlReader.getLocalName().equals(MT)) {
							if (isMTEnd) {
								mtList.clear();
								addPublicCol(mtList);
								isMTEnd = false;
							}
							if (mtList.size() == 0) {
								addPublicCol(mtList);
							}
							String text = xmlReader.getElementText();
							// text = subCol(text);
							if (!contains(mtList, text)) {
								mtList.add(text);
							}
						}
						// 取<r>内容
						else if (xmlReader.getLocalName().equals(R)) {
							isMTEnd = true;
							if (rList.size() == mtList.size()) {
								if (rList.size() == 0) {
									addPublicCol(mtList);
									addPublicValues(rList, String.valueOf(omcId));
								}
								createTable(mtList);
								String insertSql = createInsert(mtList, rList);
								if (insertSql != null) {
									INSERTS.add(insertSql);
								}
								try {
									insert(INSERTS, tableName, false);
								} catch (Exception e) {
									logger.error("解析爱立信性能文件，插入数据时异常", e);
								}
							}
							rList.add(xmlReader.getElementText());
						}
						break;

					case XMLStreamConstants.END_ELEMENT :
						// 遇到</md>标签，表示一个表的内容结束了．
						if (xmlReader.getLocalName().equals(MD)) {
							mtList.clear();
							addPublicCol(mtList);
							rList.clear();
							addPublicValues(rList, String.valueOf(omcId));
						}
						break;

					default :
						break;
				}

				eventCode = xmlReader.next();
				if (!xmlReader.hasNext()) {
					try {
						insert(INSERTS, tableName, true);
					} catch (Exception e) {
						logger.error("解析爱立信性能文件，插入数据时异常", e);
					}
					Iterator<String> keys = COUNT.keySet().iterator();
					while (keys.hasNext()) {
						String key = keys.next();
						dbLogger.log(omcId, key, publicMTS.substring(0, publicMTS.length() - 1), COUNT.get(key), taskID);
					}
					COUNT.clear();
					break;
				}
			}
		} catch (Exception e) {
			throw new EricssonPmParserException("解析爱立信性能文件时异常", e);
		} finally {
			try {
				Iterator<String> keys = COUNT.keySet().iterator();
				while (keys.hasNext()) {
					String key = keys.next();
					dbLogger.log(omcId, key, publicMTS.substring(0, publicMTS.length() - 1), COUNT.get(key), taskID);
				}
				COUNT.clear();
				if (in != null) {
					in.close();
				}
				if (xmlReader != null) {
					xmlReader.close();
				}
			} catch (Exception e) {
			}
		}
		EricssonPmParser exParser = new EricssonPmParserDom4jImp();
		exParser.parse(file, omcId, stampTime, taskID);
	}

	/**
	 * 创建insert语句
	 * 
	 * @param cols
	 * @param values
	 * @return
	 * @throws SQLException
	 */
	private String createInsert(List<String> cols, List<String> values) throws SQLException {

		if (cols.size() != values.size()) {
			return null;
		}

		StringBuffer insert = new StringBuffer();

		String tableName = null;

		for (int i = 14; i < cols.size(); i++) {
			if (COL_MAPS.containsKey(cols.get(i))) {
				tableName = COL_MAPS.get(cols.get(i));
			}
		}

		if (tableName == null) {
			return null;
		}

		this.tableName = tableName;

		if (tableName.equals("clt_pm_w_eric_meas_1556")) {
			cols.remove(cols.size() - 1);
			cols.remove(cols.size() - 1);
			values.remove(values.size() - 1);
			values.remove(values.size() - 1);
		}

		insert.append("insert into ").append(tableName).append(" (");
		for (int i = 0; i < cols.size(); i++) {
			String c = subCol(cols.get(i));
			insert.append(c);
			if (cols.size() - 1 != i) {
				insert.append(",");
			}
		}
		insert.append(") values (");
		if (values.get(2).equals("0")) {
			insert.append(values.get(0)).append(",").append("sysdate,sysdate,");
		} else {
			insert.append(values.get(0)).append(",").append("sysdate,to_date('").append(values.get(2)).append("','YYYY-MM-DD HH24:MI:SS'),");
		}
		for (int i = 3; i < values.size(); i++) {
			insert.append("'").append(values.get(i)).append("'");
			if (values.size() - 1 != i) {
				insert.append(",");
			}
		}
		insert.append(")");

		values.clear();
		addPublicValues(values, String.valueOf(omcId));
		return insert.toString();

	}

	/**
	 * 创建子表名使用的序列
	 * 
	 * @throws SQLException
	 */
	private void createMapSeq() throws SQLException {
		if (TABLES.contains(MAP_SEQ_NAME)) {
			return;
		}
		String sql = "CREATE SEQUENCE " + MAP_SEQ_NAME + " " + " MINVALUE 1 MAXVALUE 999999999999999999999999999"
				+ " INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE";
		try {
			CommonDB.executeUpdate(sql);
			TABLES.add(MAP_SEQ_NAME);
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				// logger.debug("序列已存在,序列名:" + MAP_SEQ_NAME);
				return;
			} else {
				logger.error("执行SQL时异常:" + sql);
				throw e;
			}
		}
	}

	/**
	 * 获取SEQ_CLT_CONF_PM_ERIC_MAP序列的nextval
	 * 
	 * @return SEQ_CLT_CONF_PM_ERIC_MAP序列的nextval
	 * @throws SQLException
	 */
	// private int getSeqNextval() throws SQLException
	// {
	// String sql = "select " + MAP_SEQ_NAME + ".nextval from dual";
	//
	// int val = 0;
	//
	// Connection con = null;
	// PreparedStatement ps = null;
	// ResultSet rs = null;
	//
	// try
	// {
	// con = DbPool.getConn();;
	// ps = con.prepareStatement(sql);
	// rs = ps.executeQuery();
	// if ( rs.next() )
	// {
	// val = rs.getInt(1);
	// }
	// }
	// catch (SQLException e)
	// {
	// logger.error("执行SQL时异常:" + sql);
	// throw e;
	// }
	// finally
	// {
	// if ( rs != null )
	// {
	// rs.close();
	// }
	// if ( ps != null )
	// {
	// ps.close();
	// }
	// if ( con != null )
	// {
	// con.close();
	// }
	// }
	//
	// return val;
	// }

	/**
	 * 根据一个列名，查询出所属表的表名
	 * 
	 * @param colName
	 *            列名
	 * @return 所属表的表名
	 * @throws SQLException
	 */
	private String getTableNameByColName(String colName) throws SQLException {
		String tableName = BUFFER.get(colName);

		if (tableName != null) {
			return tableName;
		}

		String sql = "select t.tab_name from clt_pm_w_eric_meas_map t where t.col_name='" + colName + "'";

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				tableName = rs.getString(1);
			}
		} catch (SQLException e) {
			logger.error("执行SQL时异常:" + sql);
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (con != null) {
				con.close();
			}
		}

		if (Util.isNull(tableName)) {
			return null;
		} else {
			BUFFER.put(colName, tableName);
			return tableName;
		}

	}

	/**
	 * 向clt_pm_w_eric_meas_map中添加记录
	 * 
	 * @param colName
	 *            列名
	 * @param tableName
	 *            表名
	 * @throws SQLException
	 */
	// private void addCol(String colName, String shortColName, String
	// tableName) throws SQLException
	// {
	// if ( BUFFER.containsKey(colName) ) { return; }
	//
	// String sql = "insert into " + MAP_TABLE_NAME + " values ("
	// + MAP_SEQ_NAME + ".nextval,'" + colName + "','" + shortColName
	// + "','" + tableName + "')";
	//
	// Connection con = null;
	// PreparedStatement ps = null;
	//
	// try
	// {
	// con = DbPool.getConn();
	// ps = con.prepareStatement(sql);
	// ps.executeUpdate();
	// BUFFER.put(colName, tableName);
	// }
	// catch (SQLException e)
	// {
	// if ( e.getErrorCode() != 1 )
	// {
	// logger.error("执行SQL时异常:" + sql);
	// throw e;
	// }
	// }
	// finally
	// {
	// if ( ps != null )
	// {
	// ps.close();
	// }
	// if ( con != null )
	// {
	// con.close();
	// }
	// }
	//
	// }

	private static String getTableName(String insert) {
		String tableName = insert.substring(insert.indexOf("into ") + 4, insert.indexOf(" (omcid"));

		return tableName.trim();
	}

	/**
	 * @param inserts
	 *            insert语句
	 * @param tableName
	 *            表名
	 * @param insertNow
	 *            是否立即插入
	 * @throws Exception
	 */
	private void insert(List<String> inserts, String tableName, boolean insertNow) throws Exception {
		String tbname = "";
		if (insertNow || inserts.size() % INTERVAL == 0) {
			Connection con = null;
			Statement st = null;
			try {
				con = DbPool.getConn();
				con.setAutoCommit(false);

				st = con.createStatement();

				for (String sql : inserts) {
					tbname = getTableName(sql);
					st.addBatch(sql);
					// logger.debug(sql);
					if (COUNT.containsKey(tbname)) {
						COUNT.put(tbname, COUNT.get(tbname) + 1);
					} else {
						COUNT.put(tbname, 1);
					}
				}

				st.executeBatch();
			} catch (Exception e) {
				// if ( e instanceof BatchUpdateException )
				// {
				// BatchUpdateException bue = (BatchUpdateException) e;
				// if ( bue.getErrorCode() == 942
				// || bue.getErrorCode() == 17081 ) { return; }
				// }
				logger.error("插入数据出现异常，表名：" + tbname);
				throw e;
			} finally {
				inserts.clear();
				if (con != null) {
					con.commit();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			}
		}
	}

	/**
	 * 添加公共列
	 * 
	 * @param cols
	 */
	private void addPublicCol(List<String> cols) {
		cols.add("omcid");
		cols.add("collecttime");
		cols.add("stamptime");
		cols.add("ffv");
		cols.add("sn");
		cols.add("st");
		cols.add("vn");
		cols.add("cbt");
		cols.add("neun");
		cols.add("nedn");
		cols.add("nesw");
		cols.add("mts");
		cols.add("gp");
		cols.add("moid");
	}

	/**
	 * 添加公共值
	 * 
	 * @param values
	 */
	private void addPublicValues(List<String> values, String omcid) {
		values.add(omcid);
		values.add("sysdate");
		values.add(stamptime);
		values.add(publicFFV);
		values.add(publicSN);
		values.add(publicST);
		values.add(publicVN);
		values.add(publicCBT);
		values.add(publicNEUN);
		values.add(publicNEDN);
		values.add(publicNESW);
		values.add(publicMTS);
		values.add(publicGP);
		values.add(publicMOID);
	}

	/**
	 * 创建映射表
	 * 
	 * @throws SQLException
	 */
	private void createMapTable() throws SQLException {
		if (TABLES.contains(MAP_TABLE_NAME)) {
			return;
		}
		String sql = "create table " + MAP_TABLE_NAME
				+ " (col_id number primary key,col_name varchar2(200) unique,short_col_name varchar(30),tab_name varchar2(30))";
		try {
			CommonDB.executeUpdate(sql);
			TABLES.add(MAP_TABLE_NAME);
		} catch (SQLException e) {
			if (e.getErrorCode() == 955) {
				// logger.debug("表已存在,表名:" + MAP_TABLE_NAME);
				return;
			} else {
				logger.error("执行SQL时异常:" + sql);
				throw e;
			}
		}
	}

	/**
	 * 创建数据库表
	 * 
	 * @param fileds
	 *            除公有字段外，所要附加的字段
	 * @throws SQLException
	 */
	private void createTable(List<String> fileds) throws SQLException {
		String tableName = null;

		tableName = getTableNameByColName(fileds.get(14));
		if (tableName != null) {
			return;
		}

		// int seqval = getSeqNextval();
		// tableName = TABLE_NAME + seqval;
		//
		// if ( TABLES.contains(tableName) ) { return; }
		//
		// StringBuffer buffer = new StringBuffer();
		// buffer.append("create table ").append(tableName).append("(omcid number,collecttime date,stamptime date,");
		// if ( fileds.size() > 0 )
		// {
		// for (int i = 3; i < fileds.size(); i++)
		// {
		// String c = subCol(fileds.get(i));
		// buffer.append(c).append(" varchar2(200)");
		// if ( i != fileds.size() - 1 )
		// {
		// buffer.append(",");
		// }
		// }
		// }
		// buffer.append(")");
		//
		// String sql = buffer.toString();
		//
		// try
		// {
		// CommonDB.executeUpdate(sql);
		// TABLES.add(tableName);
		// for (int i = 14; i < fileds.size(); i++)
		// {
		// addCol(fileds.get(i), fileds.get(i).length() > 30 ?
		// subCol(fileds.get(i)) : fileds.get(i), tableName);
		// }
		//
		// }
		// catch (SQLException e)
		// {
		// logger.error("创建表时异常，表名为：" + tableName + "  sql:" + sql);
		// throw e;
		// }
	}

	private boolean contains(List<String> cols, String col) {
		List<String> tmp = new ArrayList<String>();
		for (String s : cols) {
			tmp.add(subCol(s));
		}

		return tmp.contains(subCol(col));
	}

	/**
	 * 截取列名,保持30字符
	 * 
	 * @param col
	 * @return
	 */
	private String subCol(String col) {
		if (col.length() <= 30) {
			return col;
		}
		String colName = col.substring(col.length() - 30, col.length());

		return colName;
	}

	public static void main(String[] args) {
		EricssonPmParser parser = new EricssonPmParserImp();
		try {
			parser.parse("d:\\A20100302.1600+0800-1615+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC01,MeContext=DGRNC01_statsfile.xml", 121,
					new Timestamp(new Date().getTime()), 989);
		} catch (EricssonPmParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// public static void main(String[] args)
	// {
	// StringBuffer buffer = new StringBuffer();
	// buffer.append("create table ").append(" abc ").append("( omcid number,");
	// buffer.append("collecttime date, stamptime date,").append(FFV).append(" varchar(50),");
	// buffer.append(SN).append(" varchar2(50),").append(ST).append(" varchar2(50),").append(VN).append(" varchar2(50),");
	// buffer.append(CBT).append(" varchar2(50)");
	// String s = "";
	// System.out.println(buffer);
	// }
}
