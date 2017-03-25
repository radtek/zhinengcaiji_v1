package parser.hw.am;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;

import util.DBLogger;
import util.DbPool;
import util.LogMgr;
import util.LogicSpliter;
import util.Util;
import framework.SystemConfig;

/**
 * csv格式华为告警解析。
 * 
 * @author ChenSijiang 2010.03.16
 */
public class CV1CSV extends Parser {

	// 逻辑分隔器
	private LogicSpliter spliter;

	private String splite = ",";

	private String omcid;

	private String stamptime;

	private final Map<String, String> HISTORY_MAP = new HashMap<String, String>();

	private final Map<String, String> EVENT_MAP = new HashMap<String, String>();

	private final List<String> INSERTS = new ArrayList<String>();

	// private String historyTableName;

	// private String eventTableName;

	private int insertInterval = 100;

	private int count;

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	@Override
	public boolean parseData() {
		try {
			parse();
		} catch (Exception e) {
			logger.error("解析数据时发生异常", e);
			return false;
		}
		return true;
	}

	public void parse() throws HuaWeiAlarmParseException {
		String source = getFileName();
		File f = new File(source);
		String fileName = f.getName();

		this.omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		this.stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());

		BufferedReader reader = null;
		String tablename = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			if (loadConfig()) {
				spliter = new LogicSpliter(splite.charAt(0));
				if (fileName.contains("event")) {

				} else {
				}
			} else {
				spliter = new LogicSpliter(splite.charAt(0));
				String line = reader.readLine();

				if (line != null) {
					if (line.split(splite).length == 21) {
						tablename = "CLT_AM_EVENT_HW";
					} else {
						tablename = "CLT_AM_CURRENT_HISTORY_HW";
					}
				} else {
					return;
				}
				line = reader.readLine();
				while (line != null) {
					INSERTS.add(createInsert(line, tablename));
					executeInsert(INSERTS, false);
					line = reader.readLine();
				}
			}

			executeInsert(INSERTS, true);
			dbLogger.log(collectObjInfo.getDevInfo().getOmcID(), tablename, collectObjInfo.getLastCollectTime(), count, collectObjInfo.getTaskID());
			count = 0;
		} catch (Exception e) {
			throw new HuaWeiAlarmParseException("解析华为告警文件时异常", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}

	}

	private String createInsert(String line, String tablename) {
		StringBuilder insert = new StringBuilder();

		String[] spliedItems = spliter.apply(line);

		insert.append("insert into ").append(tablename).append(" values(").append(this.omcid).append(",sysdate,to_date('").append(this.stamptime)
				.append("','YYYY-MM-DD HH24:MI:SS'),");

		for (int i = 0; i < spliedItems.length; i++) {
			insert.append("'").append(spliedItems[i]).append("'");
			if (i != spliedItems.length - 1) {
				insert.append(",");
			} else {
				if (tablename.equals("CLT_AM_CURRENT_HISTORY_HW") && spliedItems.length != 25) {
					insert.append(",''");
				} else if (tablename.equals("CLT_AM_EVENT_HW") && spliedItems.length != 21) {
					insert.append(",''");
				}
			}
		}

		insert.append(")");
		return insert.toString();
	}

	@SuppressWarnings("unchecked")
	private boolean loadConfig() {
		Document doc = null;
		try {
			doc = new SAXReader().read(new File(SystemConfig.getInstance().getTempletPath() + File.separator + "clt_am_hw_csv_20100316_parsea.xml"));
			insertInterval = Integer.parseInt(doc.selectSingleNode("/template/public/insert-interval").getText().trim());
			splite = doc.selectSingleNode("/template/public/splite-char").getText().trim();
			List<Element> fileElements = doc.getRootElement().element("files").elements("file");
			for (Element e : fileElements) {
				if (e.elementText("type-id").trim().equals("1")) {
					// historyTableName = e.elementText("table-name");
					List<Element> maps = e.element("col-mappings").elements();
					for (Element map : maps) {
						HISTORY_MAP.put(map.elementTextTrim("name"), map.elementTextTrim("col"));
					}
				} else if (e.elementText("type-id").trim().equals("2")) {
					// eventTableName = e.elementText("table-name");
					List<Element> maps = e.element("col-mappings").elements();
					for (Element map : maps) {
						EVENT_MAP.put(map.elementTextTrim("name"), map.elementTextTrim("col"));
					}
				}
			}
		} catch (Exception e) {
			logger.error("配置文件读取失败，将使用默认设置。");
			return false;
		}

		if (EVENT_MAP.size() == 0 || HISTORY_MAP.size() == 0) {
			logger.error("配置文件中的映射不正常，将使用默认设置。");
			return false;
		}

		return true;
	}

	private void executeInsert(List<String> inserts, boolean insertNow) throws Exception {
		if (insertNow || inserts.size() % insertInterval == 0) {
			Connection con = null;
			Statement st = null;
			try {
				con = DbPool.getConn();
				con.setAutoCommit(false);

				st = con.createStatement();

				for (String sql : inserts) {
					st.addBatch(sql);
					// logger.debug(sql);
					count++;
				}

				st.executeBatch();
				logger.debug("批量插入成功，数量：" + inserts.size());
			} catch (Exception e) {
				logger.error("插入数据出现异常");
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

	public static void main(String[] args) {
		/*
		 * //HuaWeiAlarmParser parser = new CSVHuaWeiAlarmParser(); try { parser.parse("E:\\ftp_root\\test\\20100223175656-history-alarm-auto-1.csv",
		 * 41, new Timestamp(new Date().getTime())); } catch (HuaWeiAlarmParseException e) { // TODO Auto-generated catch block e.printStackTrace(); }
		 */
	}

}
