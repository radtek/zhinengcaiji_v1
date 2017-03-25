package parser.eric.pm;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import task.CollectObjInfo;
import util.CommonDB;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.LogAnalyzer;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 爱立信性能解析入库。新方式，分成11张表，不自动建表建字段。 sqlldr入库。
 * 
 * @author ChenSijiang 20100418
 */
public class EricssonPmParserSqlLdrImp implements EricssonPmParser {

	private String omcId;

	private String stampTime;

	private String rncName;

	private CollectObjInfo collectInfo;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private final Map<String, Pair> MOID_MAPS = new HashMap<String, Pair>();

	private final Map<String, List<String>> COLS = new HashMap<String, List<String>>();

	private final Map<String, String> BUFFERS = new HashMap<String, String>();

	private final Map<String, Integer> COUNT = new HashMap<String, Integer>();

	private final Map<String, SqlldrInfo> SQLLDR_INFOS = new HashMap<String, SqlldrInfo>();

	private static final int ATMPORT = 0;

	private static final int EUL = 1;

	private static final int GSMRELATION = 2;

	private static final int HSDSCH = 3;

	private static final int IUBLINK = 4;

	private static final int IURLINK = 5;

	private static final int LOADCONTROL = 6;

	private static final int RNCFUNCTION = 7;

	private static final int UTRANCELL = 8;

	private static final int UTRANRELATION = 9;

	private static final int VCLTP = 10;

	public EricssonPmParserSqlLdrImp(CollectObjInfo info) {
		this.collectInfo = info;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void parse(String file, int omcId, Timestamp stampTime, int taskID) throws EricssonPmParserException {
		this.omcId = String.valueOf(omcId);
		this.stampTime = Util.getDateString(stampTime);

		SAXReader reader = new SAXReader();
		reader.setEntityResolver(new EntityResolver() {

			@Override
			public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
				return new InputSource(new ByteArrayInputStream("<?xml version='1.0' encoding='UTF-8'?>".getBytes()));
			}
		});

		Document doc = null;
		try {
			doc = reader.read(new File(file));
			List<Element> mdElements = doc.selectNodes("/mdc/md");
			Element snEl = (Element) doc.selectSingleNode("/mdc/mfh/sn");
			String[] ss = snEl.getTextTrim().split("=");
			this.rncName = ss[ss.length - 1];
			loadCols();
			for (int i = 0; i < mdElements.size(); i++) {
				Element mdEl = mdElements.get(i);
				List<Element> mtEls = mdEl.selectNodes("mi/mt");
				List<Element> mvEls = mdEl.selectNodes("mi/mv");
				if (mvEls.size() == 0) {
					continue;
				}

				List<String> mtList = new ArrayList<String>();
				List<String> rList = new ArrayList<String>();
				for (Element e : mtEls) {
					mtList.add(e.getTextTrim().toUpperCase());
				}
				for (Element e : mvEls) {
					List<Element> rEles = e.selectNodes("r");
					Element moidEl = (Element) e.selectSingleNode("moid");
					String strMoid = moidEl.getTextTrim();
					if (getNeedMoid(strMoid) < 0) {
						continue;
					}
					List<String> moidNames = new ArrayList<String>();
					List<String> moidVals = new ArrayList<String>();
					String[] items = strMoid.split(",");
					for (String s : items) {
						String name = s.split("=")[0];
						String val = s.split("=")[1];
						moidNames.add(name.trim().toUpperCase());
						moidVals.add(val.trim());
					}
					for (Element re : rEles) {
						rList.add(re.getTextTrim());
					}

					if (MOID_MAPS.containsKey(strMoid)) {
						Pair p = MOID_MAPS.get(strMoid);
						if (p.moidNames.size() == 0) {
							p.moidNames.addAll(moidNames);
							p.moidValues.addAll(moidVals);
						}
					} else {
						Pair p = new Pair();
						if (p.moidNames.size() == 0) {
							p.moidNames.addAll(moidNames);
							p.moidValues.addAll(moidVals);
						}
						MOID_MAPS.put(strMoid, p);
					}
					MOID_MAPS.get(strMoid).names.addAll(mtList);
					MOID_MAPS.get(strMoid).values.addAll(rList);

					rList.clear();
				}
			}

			String tableName = null;
			Iterator<String> moidKeys = MOID_MAPS.keySet().iterator();
			while (moidKeys.hasNext()) {
				String moidKey = moidKeys.next();
				Pair pair = MOID_MAPS.get(moidKey);
				int moidType = getNeedMoid(moidKey);
				switch (moidType) {
					case ATMPORT :
						tableName = "CLT_PM_W_ERIC_ATMPORT";
						break;
					case EUL :
						tableName = "CLT_PM_W_ERIC_EUL";
						break;
					case GSMRELATION :
						tableName = "CLT_PM_W_ERIC_GSMRELATION";
						break;
					case HSDSCH :
						tableName = "CLT_PM_W_ERIC_HSDSCH";
						break;
					case IUBLINK :
						tableName = "CLT_PM_W_ERIC_IUBLINK";
						break;
					case IURLINK :
						tableName = "CLT_PM_W_ERIC_IURLINK";
						break;
					case LOADCONTROL :
						tableName = "CLT_PM_W_ERIC_LOADCONTROL";
						break;
					case RNCFUNCTION :
						tableName = "CLT_PM_W_ERIC_RNCFUNCTION";
						break;
					case UTRANCELL :
						tableName = "CLT_PM_W_ERIC_UTRANCELL";
						break;
					case UTRANRELATION :
						tableName = "CLT_PM_W_ERIC_UTRANRELATION";
						break;
					case VCLTP :
						tableName = "CLT_PM_W_ERIC_VCLTP";
						break;
					default :
						break;
				}

				if (pair.names.size() > 0 && Util.isNotNull(pair.moidValues.get(pair.moidValues.size() - 1))) {
					pair.names.addAll(pair.moidNames);
					pair.values.addAll(pair.moidValues);

					handleDatas(pair.names, pair.values, tableName);

					createSqlldr(pair.names, pair.values, tableName, moidKey);
				}
			}
			runSqlldr();

			Iterator<String> keys = SQLLDR_INFOS.keySet().iterator();
			LogAnalyzer anl = new SqlLdrLogAnalyzer();
			while (keys.hasNext()) {
				SqlldrInfo info = SQLLDR_INFOS.get(keys.next());
				if (SystemConfig.getInstance().isDeleteLog()) {
					new File(info.cltFile).delete();
					new File(info.badFile).delete();
					new File(info.txtFile).delete();
				}
				File f = new File(info.logFile);
				FileInputStream fis = null;
				SqlldrResult result = null;
				try {
					fis = new FileInputStream(f);
					result = anl.analysis(fis);
					dbLogger.log(omcId, result.getTableName(), stampTime, result.getLoadSuccCount(), taskID);
				} catch (Exception e) {
					logger.error("", e);
				} finally {
					if (fis != null) {
						fis.close();
					}
					if (SystemConfig.getInstance().isDeleteLog()) {
						f.delete();
					}
				}
			}

		} catch (Exception e) {
			throw new EricssonPmParserException("解析联通二期爱立信性能时异常", e);
		} finally {
			try {
				Iterator<String> keys = COUNT.keySet().iterator();
				while (keys.hasNext()) {
					String key = keys.next();
					dbLogger.log(omcId, key, this.stampTime, COUNT.get(key), taskID);
				}
				COUNT.clear();
			} catch (Exception e) {
			}
		}
	}

	private void handleDatas(List<String> mtList, List<String> rList, String tn) {
		List<Integer> del = new ArrayList<Integer>();

		List<String> tmp = new ArrayList<String>();

		for (int i = 0; i < mtList.size(); i++) {
			String col = getColName(mtList.get(i)).toUpperCase();
			if (!COLS.get(tn).contains(col) || tmp.contains(col)) {
				del.add(i);
				mtList.set(i, "");
			} else {
				mtList.set(i, col);
				tmp.add(col);
			}
		}

		for (int i = 0; i < rList.size(); i++) {
			if (del.contains(i)) {
				rList.set(i, "");
			}
		}
	}

	private void runSqlldr() {
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();
		Iterator<String> keys = SQLLDR_INFOS.keySet().iterator();
		while (keys.hasNext()) {
			SqlldrInfo info = SQLLDR_INFOS.get(keys.next());
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=0 control=%s bad=%s log=%s errors=9999", strOracleUserName, strOraclePassword,
					strOracleBase, info.cltFile, info.badFile, info.logFile);
			ExternalCmd externalCmd = new ExternalCmd();
			externalCmd.setCmd(cmd);

			try {
				externalCmd.execute();
			} catch (Exception e) {
				logger.error("", e);
			}
		}

	}

	private int getNeedMoid(String moid) {
		String[] items = moid.split(",");
		String lastMo = items[items.length - 1].split("=")[0].trim().toLowerCase();

		if (lastMo.equals("utrancell")) {
			return UTRANCELL;
		}
		if (lastMo.equals("gsmrelation")) {
			return GSMRELATION;
		}
		if (lastMo.equals("utranrelation")) {
			return UTRANRELATION;
		}
		if (lastMo.equals("eul")) {
			return EUL;
		}
		if (lastMo.equals("hsdsch")) {
			return HSDSCH;
		}
		if (lastMo.equals("vcltp")) {
			return VCLTP;
		}
		if (lastMo.equals("iurlink")) {
			return IURLINK;
		}
		if (lastMo.equals("iublink")) {
			return IUBLINK;
		}
		if (lastMo.equals("rncfunction")) {
			return RNCFUNCTION;
		}
		if (lastMo.equals("atmport")) {
			return ATMPORT;
		}
		if (lastMo.equals("loadcontrol")) {
			return LOADCONTROL;
		}
		return -1;
	}

	Map<String, List<Integer>> DEL_LISTS = new HashMap<String, List<Integer>>();

	private void createSqlldr(List<String> mtList, List<String> rList, String tn, String strMoid) {

		SqlldrInfo sqlldrInfo = null;
		List<Integer> del = new ArrayList<Integer>();
		if (SQLLDR_INFOS.containsKey(tn)) {
			sqlldrInfo = SQLLDR_INFOS.get(tn);
		} else {
			sqlldrInfo = new SqlldrInfo();
			String currDate = Util.getDateString_yyyyMMddHHmmssSSS(new Date());
			String name = SystemConfig.getInstance().getCurrentPath() + "\\" + collectInfo.getTaskID() + "_" + currDate + "_" + tn;
			sqlldrInfo.cltFile = name + ".ctl";
			sqlldrInfo.logFile = name + ".log";
			sqlldrInfo.badFile = name + ".bad";
			sqlldrInfo.txtFile = name + ".txt";
			File f = new File(sqlldrInfo.cltFile);

			try {
				PrintWriter pw = new PrintWriter(f);
				pw.println("load data");
				pw.println("CHARACTERSET ZHS16GBK ");
				pw.println("infile '" + sqlldrInfo.txtFile + "' append into table " + tn);
				pw.println("FIELDS TERMINATED BY \";\"");
				pw.println("TRAILING NULLCOLS");
				pw.print("(OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS',RNCNAME,");
				StringBuilder tmp = new StringBuilder();

				for (int i = 0; i < mtList.size(); i++) {
					String col = mtList.get(i);
					if (Util.isNotNull(col)) {
						tmp.append(col).append(",");
					} else {
						del.add(i);
					}
				}

				if (tmp.charAt(tmp.length() - 1) == ',') {
					tmp.deleteCharAt(tmp.length() - 1);
				}
				pw.print(tmp);
				pw.print(")");
				pw.flush();
				pw.close();
			} catch (Exception e) {
				logger.error(" ", e);
			}

			SQLLDR_INFOS.put(tn, sqlldrInfo);
		}

		FileWriter fw = null;
		try {

			fw = new FileWriter(new File(sqlldrInfo.txtFile), true);

			StringBuilder tmp = new StringBuilder();
			tmp.append(omcId).append(";").append(Util.getDateString(new Date())).append(";").append(stampTime).append(";");
			tmp.append(rncName).append(";");
			for (int i = 0; i < rList.size(); i++) {
				if (!del.contains(i) && Util.isNotNull(rList.get(i))) {
					tmp.append(rList.get(i)).append(";");
				}
			}
			if (tmp.charAt(tmp.length() - 1) == ';') {
				tmp.deleteCharAt(tmp.length() - 1);
			};
			fw.write(tmp.toString());
			fw.write("\n");
			fw.flush();
			fw.close();
		} catch (Exception e) {
			logger.error(" ", e);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
				}
			}
		}

	}

	private String getColName(String raw) {
		if (raw.length() <= 30) {
			return raw;
		}

		if (BUFFERS.get(raw) != null) {
			return BUFFERS.get(raw);
		}

		String sql = "SELECT T.SHORT_COL_NAME FROM CLT_PM_W_ERIC_MAP T WHERE " + "UPPER(T.COL_NAME)=UPPER('%s')";
		sql = String.format(sql, raw);
		String sn = "";
		Connection con = null;
		try {
			con = DbPool.getConn();
			ResultSet rs = con.prepareStatement(sql).executeQuery();
			if (rs.next()) {
				sn = rs.getString(1);
			}
			rs.close();
			BUFFERS.put(raw, sn);
		} catch (Exception e) {
			logger.error("查找列时异常:" + sql, e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return sn;
	}

	private void loadCols() {
		String strTbs = "clt_pm_w_eric_atmport, clt_pm_w_eric_eul, clt_pm_w_eric_gsmrelation,"
				+ " clt_pm_w_eric_hsdsch, clt_pm_w_eric_iublink, clt_pm_w_eric_iurlink,"
				+ " clt_pm_w_eric_loadcontrol, clt_pm_w_eric_rncfunction, clt_pm_w_eric_utran"
				+ "cell, clt_pm_w_eric_utranrelation, clt_pm_w_eric_vcltp".toUpperCase();
		String[] tables = strTbs.split(",");
		for (String s : tables) {
			String tn = s.trim().toUpperCase();
			Connection con = null;
			try {
				con = DbPool.getConn();
				ResultSetMetaData meta = con.prepareStatement("select * from " + tn).executeQuery().getMetaData();
				int count = meta.getColumnCount();
				for (int i = 0; i < count; i++) {
					String cn = meta.getColumnName(i + 1);
					if (COLS.containsKey(tn)) {
						List<String> list = COLS.get(tn);
						list.add(cn);
						COLS.put(tn, list);
					} else {
						List<String> list = new ArrayList<String>();
						list.add(cn);
						COLS.put(tn, list);
					}
				}
			} catch (Exception e) {
				logger.error("读取列名时异常", e);
			} finally {
				if (con != null) {
					try {
						con.close();
					} catch (SQLException e) {
					}
				}
			}
		}
	}

	private String subCol(String col) {
		if (col.length() <= 30) {
			return col;
		}

		return "COL_" + getSeqNextval();
	}

	private int getSeqNextval() {
		String sql = "select SEQ_CLT_PM_W_ERIC.nextval from dual";

		int val = 0;

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			con = DbPool.getConn();;
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				val = rs.getInt(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
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

	private void addCol(String col, String sn, String tn) {
		String sql = "INSERT INTO CLT_PM_W_ERIC_MAP " + "(COL_NAME,SHORT_COL_NAME,TAB_NAME) VALUES " + "('%s','%s','%s')";
		sql = String.format(sql, col, sn, tn);

		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			if (e.getErrorCode() != 1) {
				e.printStackTrace();
			}
		}
	}

	private void createTab(List<String> cols, String tn) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(tn).append(" (OMCID NUMBER,");
		sql.append("COLLECTTIME DATE,STAMPTIME DATE,");
		for (int i = 0; i < cols.size(); i++) {
			sql.append(cols.get(i)).append(" NUMBER(13,2)");
			if (i != cols.size() - 1) {
				sql.append(",");
			}
		}
		sql.append(")");

		try {
			System.out.println(sql);
			CommonDB.executeUpdate(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	// 建表，测试用。
	private void makeTable() throws Exception {
		final String PREFIX = "CLT_PM_W_ERIC_";
		File file = new File("d:\\chensijiang\\要入库的moid.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String line = null;
		String shuffix = null;
		String tn = null;
		List<String> cols = new ArrayList<String>();
		while ((line = reader.readLine()) != null) {
			line = line.trim();
			if (line.equals("")) {
				continue;
			}

			if (line.startsWith("[")) {
				if (cols.size() > 0 && tn != null) {
					createTab(cols, tn);
					cols.clear();
					tn = null;
				}
				String[] items = line.split("  ");
				shuffix = items[items.length - 1];
				shuffix = shuffix.replaceAll("]", "").trim().toUpperCase();
				tn = PREFIX + shuffix;
			} else {
				String col = line.toUpperCase().trim();
				String sn = subCol(col);
				addCol(col, sn, tn);
				cols.add(sn);
			}
		}
		if (cols.size() > 0) {
			createTab(cols, tn);
			cols.clear();
			tn = null;
		}
		reader.close();
	}

	class Pair {

		List<String> names = new ArrayList<String>();

		List<String> values = new ArrayList<String>();

		List<String> moidNames = new ArrayList<String>();

		List<String> moidValues = new ArrayList<String>();
	}

	class SqlldrInfo {

		String txtFile;

		String logFile;

		String badFile;

		String cltFile;
	}

	public static void main(String[] args) {
		EricssonPmParserSqlLdrImp parser = new EricssonPmParserSqlLdrImp(new CollectObjInfo(20100418));
		try {
			long curr = System.currentTimeMillis();
			parser.parse("d:\\A20100417.1600+0800-1615+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC06,MeContext=DGRNC06_statsfile.xml", 777,
					new Timestamp(Util.getDate1("2010-04-04 12:00:00").getTime()), 989);
			System.out.println((System.currentTimeMillis() - curr) / 1000);
		} catch (EricssonPmParserException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
