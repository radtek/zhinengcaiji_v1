package parser.lucent.w.pm;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.lucent.w.pm.STAXParserUtil.SNStruct;
import task.CollectObjInfo;
import task.DevInfo;
import util.DBLogger;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class STAXParser {

	private CollectObjInfo info;

	private String key;

	private String omcId;

	private String stamptime;

	private String rncName;

	private String subnetwork;

	private String managedElement;

	private Map<MOID, List<String>> moidFields = new HashMap<MOID, List<String>>();

	private Map<MOID, List<Counters>> moidValues = new HashMap<MOID, List<Counters>>();

	private CounterMgr counterMgr = CounterMgr.getInstance();

	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	private static final String LF = System.getProperty("line.separator");

	public boolean parse(CollectObjInfo info, String fileName) {
		if (this.info == null) {
			key = String.format("[taskId-%s][%s]", info.getTaskID(), Util.getDateString(info.getLastCollectTime()));
			omcId = String.valueOf(info.getDevInfo().getOmcID());
			stamptime = Util.getDateString(info.getLastCollectTime());
			this.info = info;
		}
		File file = new File(fileName);
		InputStream in = null;
		XMLStreamReader reader = null;
		boolean isFindMV = false;// 是否遇到了mv节点
		MOID tmpMoid = null;
		List<String> tmpMts = null;
		List<String> tmpRs = null;
		try {
			in = new FileInputStream(file);
			XMLInputFactory fac = XMLInputFactory.newInstance();
			fac.setProperty("javax.xml.stream.supportDTD", false);
			fac.setProperty("javax.xml.stream.isValidating", false);
			reader = fac.createXMLStreamReader(in);
			// reader = new UwayXMLStreamReaderImpl(in, new PropertyManager(1));
			int type = -1;
			while (reader.hasNext()) {
				type = reader.next();
				String tagName = null;
				if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
					tagName = reader.getLocalName();
				if (tagName == null) {
					continue;
				}

				String txt = null;
				switch (type) {
					case XMLStreamConstants.START_ELEMENT :
						if (tagName.equals("mt")) {
							if (!isFindMV) {
								txt = reader.getElementText();
								if (tmpMts == null) {
									tmpMts = new ArrayList<String>();
								}
								tmpMts.add(txt);
							}
						} else if (tagName.equals("r")) {
							if (isFindMV) {
								if (tmpRs == null) {
									tmpRs = new ArrayList<String>();
								}
								txt = reader.getElementText();
								if (txt != null && txt.equalsIgnoreCase("null"))
									txt = "";
								tmpRs.add(txt);
							}
						} else
						// 取sn节点中的公共字段
						if (tagName.equals("sn")) {
							txt = reader.getElementText();
							SNStruct snStruct = STAXParserUtil.parserSN(txt);
							if (snStruct == null) {
								logger.error(key + "<sn>内容解析失败:" + txt);
							} else {
								rncName = snStruct.subnetwork1;
								subnetwork = snStruct.subnetwork2;
								managedElement = snStruct.managedElement;
							}
						}

						else if (tagName.equals("mv")) {
							isFindMV = true;
						} else if (tagName.equals("moid")) {
							txt = reader.getElementText();
							MOID moid = new MOID(txt);
							tmpMoid = moid;
							if (!moidFields.containsKey(moid) && tmpMts != null) {
								moidFields.put(moid, tmpMts);
								tmpMts = null;
							}
						}
						break;

					case XMLStreamConstants.END_ELEMENT :
						// mi节点结束，在这里进行文件中一张表的入库
						if (tagName.equals("mi")) {
							MOID currMoid = moidFields.entrySet().iterator().next().getKey();
							if (currMoid.getLastName().equalsIgnoreCase("rncfunction")) {
								List<String> counterNames = moidFields.get(currMoid);
								List<Counters> countersList = moidValues.get(currMoid);
								List<String> rnc1CounterNames = new ArrayList<String>();
								List<Counters> rnc1countersList = new ArrayList<Counters>();
								for (int i = 0; i < 500; i++) {
									rnc1CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 0; i < 500; i++) {
										ls.add(counters.getValues().get(i));
									}
									rnc1countersList.add(new Counters(ls));
								}

								List<String> rnc2CounterNames = new ArrayList<String>();
								List<Counters> rnc2countersList = new ArrayList<Counters>();
								for (int i = 500; i < counterNames.size(); i++) {
									rnc2CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 500; i < counters.getValues().size(); i++) {
										ls.add(counters.getValues().get(i));
									}
									rnc2countersList.add(new Counters(ls));
								}
								moidFields.clear();
								moidValues.clear();
								moidFields.put(new MOID("rncfunction1", null), rnc1CounterNames);
								moidValues.put(new MOID("rncfunction1", null), rnc1countersList);
								writeSqlldr();
								moidFields.clear();
								moidValues.clear();
								rnc2CounterNames.add("RNCFUNCTION");
								moidFields.put(new MOID("rncfunction2", null), rnc2CounterNames);
								moidValues.put(new MOID("rncfunction2", null), rnc2countersList);
								writeSqlldr();
							} else if (currMoid.getLastName().equalsIgnoreCase("utrancell")) {
								List<String> counterNames = moidFields.get(currMoid);
								List<Counters> countersList = moidValues.get(currMoid);
								List<String> cell1CounterNames = new ArrayList<String>();
								List<Counters> cell1countersList = new ArrayList<Counters>();
								for (int i = 0; i < counterNames.size(); i++) {
									cell1CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 0; i < counterNames.size(); i++) {
										ls.add(counters.getValues().get(i));
									}
									ls.add(counters.getValues().get(counters.getValues().size() - 1));
									cell1countersList.add(new Counters(ls));
								}

								List<String> cell2CounterNames = new ArrayList<String>();
								List<Counters> cell2countersList = new ArrayList<Counters>();
								for (int i = 0; i < counterNames.size(); i++) {
									cell2CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 0; i < counterNames.size(); i++) {
										ls.add(counters.getValues().get(i));
									}
									ls.add(counters.getValues().get(counters.getValues().size() - 1));
									cell2countersList.add(new Counters(ls));
								}

								List<String> cell3CounterNames = new ArrayList<String>();
								List<Counters> cell3countersList = new ArrayList<Counters>();
								for (int i = 0; i < counterNames.size(); i++) {
									cell3CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 0; i < counterNames.size(); i++) {
										ls.add(counters.getValues().get(i));
									}
									ls.add(counters.getValues().get(counters.getValues().size() - 1));
									cell3countersList.add(new Counters(ls));
								}

								List<String> cell4CounterNames = new ArrayList<String>();
								List<Counters> cell4countersList = new ArrayList<Counters>();
								for (int i = 0; i < counterNames.size(); i++) {
									cell4CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 0; i < counterNames.size(); i++) {
										ls.add(counters.getValues().get(i));
									}
									ls.add(counters.getValues().get(counters.getValues().size() - 1));
									cell4countersList.add(new Counters(ls));
								}

								List<String> cell5CounterNames = new ArrayList<String>();
								List<Counters> cell5countersList = new ArrayList<Counters>();
								for (int i = 0; i < counterNames.size(); i++) {
									cell5CounterNames.add(counterNames.get(i));
								}
								for (Counters counters : countersList) {
									List<String> ls = new ArrayList<String>();
									for (int i = 0; i < counterNames.size(); i++) {
										ls.add(counters.getValues().get(i));
									}
									ls.add(counters.getValues().get(counters.getValues().size() - 1));
									cell5countersList.add(new Counters(ls));
								}

								moidFields.clear();
								moidValues.clear();
								cell1CounterNames.add("UTRANCELL");
								moidFields.put(new MOID("UTRANCELL1", null), cell1CounterNames);
								moidValues.put(new MOID("UTRANCELL1", null), cell1countersList);
								writeSqlldr();

								moidFields.clear();
								moidValues.clear();
								cell2CounterNames.add("UTRANCELL");
								moidFields.put(new MOID("UTRANCELL2", null), cell2CounterNames);
								moidValues.put(new MOID("UTRANCELL2", null), cell2countersList);
								writeSqlldr();

								moidFields.clear();
								moidValues.clear();
								cell3CounterNames.add("UTRANCELL");
								moidFields.put(new MOID("UTRANCELL3", null), cell3CounterNames);
								moidValues.put(new MOID("UTRANCELL3", null), cell3countersList);
								writeSqlldr();

								moidFields.clear();
								moidValues.clear();
								cell4CounterNames.add("UTRANCELL");
								moidFields.put(new MOID("UTRANCELL4", null), cell4CounterNames);
								moidValues.put(new MOID("UTRANCELL4", null), cell4countersList);
								writeSqlldr();

								moidFields.clear();
								moidValues.clear();
								cell5CounterNames.add("UTRANCELL");
								moidFields.put(new MOID("UTRANCELL5", null), cell5CounterNames);
								moidValues.put(new MOID("UTRANCELL5", null), cell5countersList);
								writeSqlldr();
							} else {
								writeSqlldr();
							}
						} else if (tagName.equals("mv")) {
							isFindMV = false;
							String[] names = tmpMoid.listNames();
							for (String name : names) {
								tmpRs.add(tmpMoid.getValueByName(name));
							}
							if (moidValues.containsKey(tmpMoid)) {
								List<Counters> cs = moidValues.get(tmpMoid);
								cs.add(new Counters(tmpRs));
							} else {
								List<Counters> cs = new ArrayList<Counters>();
								cs.add(new Counters(tmpRs));
								moidValues.put(tmpMoid, cs);
							}
							tmpMoid = null;
							tmpRs = null;
						}
						break;

					default :
						break;
				}
			}

		} catch (Exception e) {
			logger.error(key + "解析阿朗性能文件时异常", e);
			return false;
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (XMLStreamException unused) {
			}
			// moidFields.clear();
			// moidValues.clear();
			// tmpMts = null;
			// tmpRs = null;
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
				}
			}
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}
			}
			// System.gc();
			// info();
		}

		return true;
	}

	class SqlldrFiles implements Closeable {

		File txt;

		File ctl;

		File bad;

		File log;

		PrintWriter txtWriter;

		PrintWriter ctlWriter;

		List<Integer> delList;

		public SqlldrFiles(File txt, File ctl, File bad, File log) {
			super();
			this.txt = txt;
			this.ctl = ctl;
			this.bad = bad;
			this.log = log;
			try {
				txtWriter = new PrintWriter(txt);
				ctlWriter = new PrintWriter(ctl);
			} catch (Exception e) {
				logger.error(key + "创建Writer失败", e);
			}
		}

		@Override
		public String toString() {
			return "SqlldrFiles [txt=" + txt + ", ctl=" + ctl + ", bad=" + bad + ", log=" + log + "]";
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(txtWriter);
			IOUtils.closeQuietly(ctlWriter);
		}

	}

	public boolean startSqlldr() {
		boolean b = true;
		Iterator<Entry<String, SqlldrFiles>> it = sqlldrFileMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, SqlldrFiles> en = it.next();
			File txt = en.getValue().txt;
			File ctl = en.getValue().ctl;
			File bad = en.getValue().bad;
			File log = en.getValue().log;
			en.getValue().close();
			String serviceName = SystemConfig.getInstance().getDbService();
			String uid = SystemConfig.getInstance().getDbUserName();
			String pwd = SystemConfig.getInstance().getDbPassword();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", uid, pwd, serviceName,
					ctl.getAbsoluteFile(), bad.getAbsoluteFile(), log.getAbsoluteFile());
			ExternalCmd externalCmd = new ExternalCmd();
			logger.debug(key + "当前执行的SQLLDR命令为：" + cmd.replace(uid, "*").replace(pwd, "*"));
			int ret = -1;
			try {
				ret = externalCmd.execute(cmd);
			} catch (Exception e) {
				logger.error(key + "执行sqlldr命令失败(" + cmd + ")", e);
				b = false;
			}
			SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
			try {
				SqlldrResult result = analyzer.analysis(new FileInputStream(log));
				logger.debug(key + "ret=" + ret + " SQLLDR日志分析结果: omcid=" + omcId + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
						+ result.getTableName() + " 数据时间=" + stamptime + " sqlldr日志=" + log.getAbsolutePath());

				dbLogger.log(info.getDevInfo().getOmcID(), result.getTableName(), info.getLastCollectTime(), result.getLoadSuccCount(),
						info.getTaskID());
			} catch (Exception e) {
				logger.error(key + " sqlldr日志分析失败，文件名：" + log.getAbsolutePath() + "，原因: ", e);
				b = false;
			}
			if (SystemConfig.getInstance().isDeleteLog()) {
				if (ret == 0) {
					txt.delete();
					ctl.delete();
					bad.delete();
					log.delete();
				} else if (ret == 2) {
					txt.delete();
					ctl.delete();
					// bad.delete();
				}
			}

		}

		return b;
	}

	private Map<String, SqlldrFiles> sqlldrFileMap = new HashMap<String, STAXParser.SqlldrFiles>();

	Date now = new Date();

	String strNow = Util.getDateString(now);

	private void writeSqlldr() throws Exception {
		MOID moid = moidFields.entrySet().iterator().next().getKey();
		String tableName = "CLT_PM_W_AL_" + moid.getLastName().toUpperCase();
		List<String> counterNames = moidFields.get(moid);
		List<Counters> countersList = moidValues.get(moid);
		File txt = null;
		File log = null;
		File ctl = null;
		File bad = null;
		PrintWriter ctlWriter = null;
		PrintWriter txtWriter = null;
		SqlldrFiles sq = null;
		boolean containsFlag = false;
		List<Integer> delList = null;
		if (sqlldrFileMap.containsKey(tableName)) {
			containsFlag = true;
			sq = sqlldrFileMap.get(tableName);
			txt = sq.txt;
			log = sq.log;
			ctl = sq.ctl;
			bad = sq.bad;
			delList = sq.delList;
		} else {

			String today = Util.getDateString_yyyyMMdd(now);
			// 存放阿朗性能数据临时文件的文件夹
			File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "al_pm" + File.separator
					+ today);
			if (!dir.exists()) {
				dir.mkdirs();
			}

			String mainFileName = info.getTaskID() + "_" + tableName + "_" + Util.getDateString_yyyyMMddHHmmss(info.getLastCollectTime()) + "_"
					+ Util.getDateString_yyyyMMddHHmmssSSS(new Date());
			txt = new File(dir, mainFileName + ".txt");
			log = new File(dir, mainFileName + ".log");
			ctl = new File(dir, mainFileName + ".ctl");
			bad = new File(dir, mainFileName + ".bad");
			sq = new SqlldrFiles(txt, ctl, bad, log);
			sqlldrFileMap.put(tableName, sq);
		}
		ctlWriter = sq.ctlWriter;
		txtWriter = sq.txtWriter;

		if (!containsFlag) {
			// 写txt中的表头，和ctl文件
			ctlWriter.println("load data");
			ctlWriter.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			ctlWriter.println("infile '" + txt.getAbsolutePath() + "' append into table " + tableName);
			ctlWriter.println("FIELDS TERMINATED BY \";\"");
			ctlWriter.println("TRAILING NULLCOLS");
			ctlWriter.print("(RNC_NAME,SUBNETWORK,MANAGEDELEMENT");
			txtWriter.print("RNC_NAME;SUBNETWORK;MANAGEDELEMENT");
			delList = new ArrayList<Integer>();
			sq.delList = delList;
			for (String s : counterNames) {
				Counter counter = counterMgr.getBySourceName(s, tableName);
				if (counter != null && isContaisCol(tableName, counter.shortName)) {
					txtWriter.print(";" + counter.shortName);
					ctlWriter.print("," + counter.shortName);
				} else {
					delList.add(counterNames.indexOf(s));

				}
			}
			if (!tableName.contains("CLT_PM_W_AL_UTRANCELL")) {
				String[] moidNames = moid.listNames();
				for (String s : moidNames) {
					String up = s.toUpperCase();
					txtWriter.print(";" + up);
					ctlWriter.print("," + up);
				}
			}
			txtWriter.print(";OMCID;COLLECTTIME;STAMPTIME" + LF);
			ctlWriter.print(",OMCID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS')");
			txtWriter.flush();
			ctlWriter.flush();
			ctlWriter.close();
		}
		// 写txt中的内容
		for (int i = 0; i < countersList.size(); i++) {
			Counters counters = countersList.get(i);
			txtWriter.print(rncName + ";" + subnetwork + ";" + managedElement);
			for (int j = 0; j < counters.getValues().size(); j++) {
				if (!delList.contains(j)) {
					String val = counters.getValues().get(j);
					val = (val == null || val.trim().equalsIgnoreCase("null")) ? "" : val;
					txtWriter.print(";" + val);
				}
			}
			txtWriter.print(";" + omcId + ";" + strNow + ";" + stamptime + LF);
			if (i % 50 == 0) {
				txtWriter.flush();
			}
		}
		txtWriter.flush();

		moidValues.clear();
		moidFields.clear();

	}

	private Map<String, List<String>> tbCols = new HashMap<String, List<String>>();

	private boolean isContaisCol(String tableName, String col) {
		if (!tbCols.containsKey(tableName)) {
			loadCols(tableName);
		}
		return tbCols.get(tableName).contains(col.toUpperCase());
	}

	private void loadCols(String tableName) {
		String sql = "select * from " + tableName + " where 1=2";
		List<String> cols = new ArrayList<String>();
		Connection con = DbPool.getConn();
		PreparedStatement st = null;
		ResultSet rs = null;
		ResultSetMetaData meta = null;
		try {
			st = con.prepareStatement(sql);
			rs = st.executeQuery();
			meta = rs.getMetaData();
			int count = meta.getColumnCount();
			for (int i = 0; i < count; i++) {
				cols.add(meta.getColumnName(i + 1));
			}
			tbCols.put(tableName, cols);
		} catch (Exception e) {
			logger.error("读取列的元数据时异常：" + sql, e);
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

	public static void main(String[] args) throws Exception {
		CollectObjInfo c = new CollectObjInfo(1122);
		DevInfo d = new DevInfo();
		d.setOmcID(99);
		c.setDevInfo(d);
		c.setLastCollectTime(new Timestamp(new Date().getTime()));

		// String fileName =
		// "C:\\Users\\ChenSijiang\\Desktop\\A20101018.1100+0800-1200+0800_NodeB-DQ1_cejinglouqu.xml";

		String fileName = "F:\\ftp_root\\w\\al\\pm\\A20101018.1000+0800-1100+0800_RNCCN-DQRNC01";
		STAXParser pp = new STAXParser();
		pp.parse(c, fileName);
		pp.parse(c, fileName);
		pp.startSqlldr();
	}
}
