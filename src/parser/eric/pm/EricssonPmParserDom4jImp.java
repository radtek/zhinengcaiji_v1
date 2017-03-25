package parser.eric.pm;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.Util;

/**
 * 联通二期爱立信性能dom4j方式解析。
 * 
 * @author ChenSijiang 2010-4-14
 */
public class EricssonPmParserDom4jImp implements EricssonPmParser {

	private String omcId;

	private String stampTime;

	private String collectTime = "sysdate";

	private String ffv;

	private String sn;

	private String st;

	private String vn;

	private String cbt;

	private String neun;

	private String nedn;

	private String nesw;

	private String mts;

	private String gp;

	private String moid;

	private int count;

	private final List<String> TABLE_COLS = new ArrayList<String>();

	private final List<String> INSERTS = new ArrayList<String>();

	private final DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static final int INTERVAL = 50; // insert 间隔

	// private static final String MAP_SEQ_NAME = "SEQ_CLT_CONF_PM_ERIC_MAP"; // 序列名
	// private static final String MAP_TABLE_NAME = "clt_pm_w_eric_meas_map"; // 表名－列名映射表
	private static final String TABLE_NAME = "clt_pm_w_eric_meas_1800"; // 表名

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	@SuppressWarnings("unchecked")
	@Override
	public void parse(String file, int omcId, Timestamp stampTime, int taskID) throws EricssonPmParserException {
		this.omcId = String.valueOf(omcId);
		this.stampTime = Util.getDateString(stampTime);

		Document doc = null;

		try {
			SAXReader reader = new SAXReader();
			reader.setEntityResolver(new IgnoreDTDEntityResolver());
			doc = reader.read(new File(file));
			this.ffv = getElementText(doc, "/mdc/mfh/ffv");
			this.sn = getElementText(doc, "/mdc/mfh/sn");
			this.st = getElementText(doc, "/mdc/mfh/st");
			this.vn = getElementText(doc, "/mdc/mfh/vn");
			this.cbt = getElementText(doc, "/mdc/mfh/cbt");

			List<Element> mdElements = doc.selectNodes("/mdc/md");
			if (mdElements == null || mdElements.size() == 0) {
				logger.warn("未找到/mdc/md节点，解析结束");
				return;
			}
			loadCols();
			List<String> mtList = new ArrayList<String>();
			List<String> rList = new ArrayList<String>();
			for (int i = 0; i < mdElements.size(); i++) {
				Element mdEl = mdElements.get(i);
				this.neun = getElementText(mdEl, "neid/neun");
				this.nedn = getElementText(mdEl, "neid/nedn");
				this.nesw = getElementText(mdEl, "neid/nesw");
				this.mts = getElementText(mdEl, "mi/mts");
				this.gp = getElementText(mdEl, "mi/gp");

				List<Element> mtEls = mdEl.selectNodes("mi/mt");
				mtList.clear();
				for (Element e : mtEls) {
					String txt = e.getTextTrim();
					mtList.add(txt);
				}
				List<Element> mvEls = mdEl.selectNodes("mi/mv");
				for (int j = 0; j < mvEls.size(); j++) {
					Element mvEl = mvEls.get(j);
					this.moid = getElementText(mvEl, "moid");
					List<Element> rEls = mvEl.selectNodes("r");
					rList.clear();
					for (Element e : rEls) {
						rList.add(e.getTextTrim());
					}
					String insert = createInsert(mtList, rList);
					if (insert != null) {
						INSERTS.add(insert);
						try {
							insert(INSERTS, false);
						} catch (Exception e) {
							logger.error("插入数据时异常", e);
						}
					}
				}
			}
			insert(INSERTS, true);
			dbLogger.log(omcId, TABLE_NAME, stampTime, count, taskID);
		} catch (Exception e) {
			throw new EricssonPmParserException(e);
		} finally {
		}
	}

	private String createInsert(List<String> mtList, List<String> rList) {
		boolean isFind = false;
		for (String key : mtList) {
			if (TABLE_COLS.contains(subCol(key).toUpperCase())) {
				isFind = true;
			}
		}

		if (!isFind) {
			return null;
		}

		StringBuilder insert = new StringBuilder();
		insert.append("insert into ").append(TABLE_NAME).append(" (omcid,collecttime,stamptime");
		insert.append(",ffv,sn,st,vn,cbt,neun,nedn,nesw,mts,gp,moid,");
		List<Integer> indexis = new ArrayList<Integer>();
		for (int i = 0; i < mtList.size(); i++) {
			String name = mtList.get(i);
			String sname = subCol(name);
			if (TABLE_COLS.contains(sname.toUpperCase())) {
				insert.append(sname);
				if (i != mtList.size() - 1) {
					insert.append(",");
				}
				// addColToTable(sname.toUpperCase());
				// try
				// {
				// addCol(name, sname, TABLE_NAME);
				// TABLE_COLS.add(sname.toUpperCase());
				// }
				// catch (Exception e)
				// {
				// logger.error("向map表加记录时异常", e);
				// }
			} else {
				indexis.add(i);
			}
		}
		if (insert.charAt(insert.length() - 1) == ',') {
			insert.deleteCharAt(insert.length() - 1);
		}
		insert.append(") values ('").append(omcId).append("',").append(collectTime).append(",to_date('");
		insert.append(stampTime).append("','YYYY-MM-DD HH24:MI:SS'),'").append(ffv).append("','");
		insert.append(sn).append("','").append(st).append("','").append(vn).append("','").append(cbt);
		insert.append("','").append(neun).append("','").append(nedn).append("','").append(nesw).append("','");
		insert.append(mts).append("','").append(gp).append("','").append(moid).append("',");
		for (int i = 0; i < rList.size(); i++) {
			String val = rList.get(i);
			if (!indexis.contains(i)) {
				insert.append("'").append(val).append("'");
				if (i != mtList.size() - 1) {
					insert.append(",");
				}
			}
		}
		if (insert.charAt(insert.length() - 1) == ',') {
			insert.deleteCharAt(insert.length() - 1);
		}
		insert.append(")");

		return insert.toString();

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

	private void loadCols() {
		String sql = "select * from " + TABLE_NAME;
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;

		try {
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			meta = rs.getMetaData();

			int count = meta.getColumnCount();
			for (int i = 0; i < count; i++) {
				TABLE_COLS.add(meta.getColumnName(i + 1));
			}
		} catch (Exception e) {
			logger.error("读取列时异常", e);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (Exception e) {
			}
		}
	}

	// private void addColToTable(String colName)
	// {
	// String sql = "alter table " + TABLE_NAME + " add (" + colName
	// + " varchar(200))";
	// try
	// {
	// CommonDB.executeUpdate(sql);
	// }
	// catch (Exception e)
	// {
	// logger.error("增加列时异常", e);
	// }
	// }

	private String getElementText(Node e, String path) {
		Node n = e.selectSingleNode(path);
		if (n == null) {
			return "";
		}
		if (n.getText() == null) {
			return "";
		}
		return n.getText();
	}

	private String subCol(String col) {
		if (col.length() <= 30) {
			return col;
		}
		String colName = col.substring(0, 3) + col.substring(col.length() - 27, col.length());

		return colName;
	}

	/**
	 * @param inserts
	 *            insert语句
	 * @param insertNow
	 *            是否立即插入
	 * @throws Exception
	 */
	private void insert(List<String> inserts, boolean insertNow) throws Exception {
		if (insertNow || inserts.size() % INTERVAL == 0) {
			Connection con = null;
			Statement st = null;
			try {
				con = DbPool.getConn();
				con.setAutoCommit(false);

				st = con.createStatement();

				for (String sql : inserts) {
					st.addBatch(sql);
					count++;
				}

				st.executeBatch();
			} catch (Exception e) {
				logger.error("插入数据出现异常，表名：" + TABLE_NAME);
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

	private class IgnoreDTDEntityResolver implements EntityResolver {

		@Override
		public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
			return new InputSource(new ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
		}

	}

	public static void main(String[] args) {
		EricssonPmParser p = new EricssonPmParserDom4jImp();
		try {
			p.parse("d:\\A20100302.1600+0800-1615+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC01,MeContext=DGRNC01_statsfile.xml", 89,
					new Timestamp(Util.getDate1("2010-02-03 12:00:00").getTime()), 989);
		} catch (EricssonPmParserException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
