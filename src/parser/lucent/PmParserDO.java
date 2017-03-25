package parser.lucent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import parser.lucent.PmParser1X.Field;
import parser.lucent.PmParser1X.SLInfo;
import parser.lucent.evdo.EvdoRecordPair;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/* 解hdrfms/hcsfms文件。*/
public class PmParserDO extends Parser {

	static final Logger log = LogMgr.getInstance().getSystemLogger();

	static final int HDRFMS = 1;

	static final int HCSFMS = 2;

	static final Map<String, String> TYPE_TABLE_HDR = new HashMap<String, String>();

	static final Map<String, String> TYPE_TABLE_HCS = new HashMap<String, String>();

	long taskid;

	String omcid;

	String collecttime;

	String stamptime = "";

	String starttime = "";

	String endtime = "";

	String hdrcRelease = "";

	String rnc = "";

	String rncGroupId = "";

	String ohm = "";

	String hcs = "";

	String release = "";

	String version = "";

	String factype = "";

	String contl = "";

	int type;

	Map<String, List<Field>> templets;

	Map<String, SLInfo> sqlldrs;

	Map<String, String> typeMap;

	static {
		TYPE_TABLE_HCS.put("SECT", "CLT_PM_HCS_EVDO_CARR_HCSFMS");
		TYPE_TABLE_HCS.put("EVM", "CLT_PM_HCS_EVDO_EVM");
		TYPE_TABLE_HCS.put("DATALINK", "CLT_PM_HCS_EVDO_DATALINK");
		TYPE_TABLE_HCS.put("HCS", "CLT_PM_HCS_EVDO_BTS_HCSFMS");

		TYPE_TABLE_HDR.put("RNC", "CLT_PM_HDR_EVDO_RNC");
		TYPE_TABLE_HDR.put("RNC_PGPF", "CLT_PM_HDR_EVDO_RNC_PG");
		TYPE_TABLE_HDR.put("AP", "CLT_PM_HDR_EVDO_AP");
		TYPE_TABLE_HDR.put("OHM", "CLT_PM_HDR_EVDO_OHM");
		TYPE_TABLE_HDR.put("OHM_PGPF", "CLT_PM_HDR_EVDO_OHM_PG");
		TYPE_TABLE_HDR.put("TP", "CLT_PM_HDR_EVDO_TP");
		TYPE_TABLE_HDR.put("HCS", "CLT_PM_HDR_EVDO_HCS");
		TYPE_TABLE_HDR.put("SECT", "CLT_PM_HDR_EVDO_CARR");
		TYPE_TABLE_HDR.put("CARR", "CLT_PM_HDR_EVDO_BTS_CARRIER");
	}

	@Override
	public boolean parseData() throws Exception {
		taskid = collectObjInfo.getTaskID();
		omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		collecttime = Util.getDateString(new Date());
		stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		type = (FilenameUtils.getName(fileName).toUpperCase().contains("HDRFMS") ? HDRFMS : HCSFMS);
		if (type == HDRFMS)
			typeMap = TYPE_TABLE_HDR;
		else
			typeMap = TYPE_TABLE_HCS;

		templets = parsetemp(collectObjInfo.getParseTmpID());
		if (templets == null || templets.isEmpty()) {
			log.error(taskid + " 模板不正确或为空。");
			return false;
		}
		initSqlldr();

		log.debug(taskid + " 开始解析 - " + fileName);
		InputStream in = null;
		Reader r = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(fileName);
			r = new InputStreamReader(in);
			br = new BufferedReader(r);
			List<EvdoRecordPair> tmpList = new ArrayList<EvdoRecordPair>();
			String currTable = null;
			String lastTable = null;
			String currFlag = null;
			String lastFlag = null;
			int pgIndex = 0;
			String line = null;
			while ((line = br.readLine()) != null) {
				currFlag = findFlag(line);
				if (currFlag != null && currFlag.equalsIgnoreCase("CARR")
						&& (currTable.equalsIgnoreCase("CLT_PM_HCS_EVDO_CARR_HCSFMS") || currTable.equalsIgnoreCase("CLT_PM_HDR_EVDO_CARR"))) {
					currFlag = null;
				}

				// if ( currTable != null && currFlag != null
				// && currTable.equals("CLT_PM_HDR_EVDO_CARR")
				// && currFlag.equals("CARR") )
				// {
				// handLine(currTable, tmpList);
				// tmpList.clear();
				// }

				if (line.startsWith("PGPF ")) {
					if (lastFlag != null) {
						if (lastFlag.equalsIgnoreCase("RNC"))
							currFlag = "RNC_PGPF";
						else if (lastFlag.equals("OHM")) {
							currFlag = "OHM_PGPF";
							pgIndex = 0;
						}
						if (!tmpList.isEmpty() && lastTable != null) {
							handLine(currTable, tmpList);
							tmpList.clear();
						}
					}
				}

				if (currFlag == null && lastFlag != null && currTable != null) {
					String[] sp = null;
					if (line.contains(","))
						sp = split0(line);
					else
						sp = split1(line);
					if (currTable.equals("CLT_PM_HDR_EVDO_RNC")) {
						if (sp[0].equalsIgnoreCase("RNC"))
							rnc = sp[1];
						else if (sp[0].equalsIgnoreCase("RNC_GROUP_ID"))
							rncGroupId = sp[1];
					} else if (currTable.equalsIgnoreCase("CLT_PM_HDR_EVDO_OHM")) {
						if (sp[0].equalsIgnoreCase("OHM"))
							ohm = sp[1];
					} else if (currTable.equalsIgnoreCase("CLT_PM_HCS_EVDO_BTS_HCSFMS") || currTable.equalsIgnoreCase("CLT_PM_HDR_EVDO_HCS")) {
						if (sp[0].equalsIgnoreCase("HCS"))
							hcs = sp[1];
					}
					if (currTable.equalsIgnoreCase("CLT_PM_HCS_EVDO_BTS_HCSFMS")) {
						if (sp[0].equalsIgnoreCase("VERSION"))
							version = sp[1];
						else if (sp[0].equalsIgnoreCase("FACTYPE"))
							factype = sp[1];
						else if (sp[0].equalsIgnoreCase("CONTL"))
							contl = sp[1];
						else if (sp[0].equalsIgnoreCase("RELEASE"))
							release = sp[1];
					}

					if (sp[0].toUpperCase().startsWith("PAGING_AREA_FOR_")) {
						if (sp[0].contains("_FIRST_")) {
							pgIndex = 1;
						} else if (sp[0].contains("_SECOND_")) {
							pgIndex = 2;
						} else if (sp[0].contains("_THIRD_")) {
							pgIndex = 3;
						} else if (sp[0].contains("_FOURTH_")) {
							pgIndex = 4;
						} else if (sp[0].contains("_FIFTH_")) {
							pgIndex = 5;
						} else if (sp[0].contains("_SIXTH_")) {
							pgIndex = 6;
						} else if (sp[0].contains("_SEVENTH_")) {
							pgIndex = 7;
						} else if (sp[0].contains("_EIGHTH_")) {
							pgIndex = 8;
						} else {
							pgIndex = 0;
						}
					}
					if (currTable != null && currTable.equalsIgnoreCase("CLT_PM_HDR_EVDO_OHM_PG") && pgIndex > 0) {
						sp[0] = sp[0] + "_" + pgIndex;
						// if ( pgIndex >= 8 )
						// pgIndex = 0;
					}
					tmpList.add(new EvdoRecordPair(sp[0], sp[1]));
					continue;
				}

				if (currFlag == null) {
					if (line.startsWith("StartTime,")) {
						String[] sp = split0(line);
						starttime = sp[1].substring(0, sp[1].lastIndexOf(" "));
					} else if (line.startsWith("EndTime,")) {
						String[] sp = split0(line);
						endtime = sp[1].substring(0, sp[1].lastIndexOf(" "));
					} else if (line.startsWith("HDRC_RELEASE ")) {
						hdrcRelease = split1(line)[1];
					}
					continue;
				}

				if (typeMap.containsKey(currFlag)) {
					lastTable = currTable;
					currTable = typeMap.get(currFlag);
				} else {
					// if ( currFlag.equalsIgnoreCase("CARR")
					// &&
					// currTable.equalsIgnoreCase("CLT_PM_HCS_EVDO_CARR_HCSFMS")
					// )
					// {
					//
					// }
					// else
					// {
					currTable = null;
					// }
				}
				if (lastFlag == null || currFlag.equalsIgnoreCase("OHM") || currFlag.equalsIgnoreCase("RNC"))
					lastFlag = currFlag;
				if (currTable == null) {
					currFlag = null;
				} else {
					if (!tmpList.isEmpty()) {
						handLine(lastTable == null ? currTable : lastTable, tmpList);
						tmpList.clear();
					}
					String[] sp = split1(line);
					if (currTable.equals("CLT_PM_HDR_EVDO_RNC")) {
						if (sp[0].equalsIgnoreCase("RNC"))
							rnc = sp[1];
						else if (sp[0].equalsIgnoreCase("RNC_GROUP_ID"))
							rncGroupId = sp[1];
					} else if (currTable.equalsIgnoreCase("CLT_PM_HDR_EVDO_OHM")) {
						if (sp[0].equalsIgnoreCase("OHM"))
							ohm = sp[1];
					} else if (currTable.equalsIgnoreCase("CLT_PM_HCS_EVDO_BTS_HCSFMS") || currTable.equalsIgnoreCase("CLT_PM_HDR_EVDO_HCS")) {
						if (sp[0].equalsIgnoreCase("HCS"))
							hcs = sp[1];
					}
					if (currTable.equalsIgnoreCase("CLT_PM_HCS_EVDO_BTS_HCSFMS")) {
						if (sp[0].equalsIgnoreCase("VERSION"))
							version = sp[1];
						else if (sp[0].equalsIgnoreCase("FACTYPE"))
							factype = sp[1];
						else if (sp[0].equalsIgnoreCase("CONTL"))
							contl = sp[1];
						else if (sp[0].equalsIgnoreCase("RELEASE"))
							release = sp[1];
					}

					tmpList.add(new EvdoRecordPair(sp[0], sp[1]));
				}
				continue;

			}
			if (!tmpList.isEmpty()) {
				if (lastTable != null)
					handLine(lastTable, tmpList);
				tmpList.clear();
			}
			log.debug(taskid + " 解析完成 - " + fileName + "，开始入库");
			store();
			log.debug(taskid + " 入库完成 - " + fileName);
		} catch (Exception e) {
			log.error(taskid + " 解析时发生例外。", e);
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(r);
			IOUtils.closeQuietly(in);
		}
		return true;
	}

	private void store() {
		if (sqlldrs == null)
			return;

		Iterator<String> it = sqlldrs.keySet().iterator();
		while (it.hasNext()) {
			String table = it.next();
			SLInfo sl = sqlldrs.get(table);
			sl.close();
			int ret = -1;
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
					.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(),
					sl.ctl.getAbsoluteFile(), sl.bad.getAbsoluteFile(), sl.log.getAbsoluteFile());
			log.debug(taskid + " 执行sqlldr：" + cmd);
			try {
				ret = new ExternalCmd().execute(cmd);
			} catch (Exception e) {
				log.error(taskid + " sqlldr异常。", e);
			}
			try {
				SqlldrResult sr = new SqlLdrLogAnalyzer().analysis(sl.log.getAbsolutePath());
				log.debug(taskid + " sqlldr结果：ret=" + ret + "，omcid=" + omcid + "，表名=" + sr.getTableName() + "，入库条数=" + sr.getLoadSuccCount()
						+ "，log=" + sl.log.getAbsolutePath());
				LogMgr.getInstance()
						.getDBLogger()
						.log(collectObjInfo.getDevInfo().getOmcID(), sr.getTableName(), collectObjInfo.getLastCollectTime(), sr.getLoadSuccCount(),
								taskid);
			} catch (Exception e) {
				log.error(taskid + " 分析sqlldr日志时异常：" + sl.log, e);
			}
			if (ret == 0) {
				sl.log.delete();
				sl.txt.delete();
				sl.ctl.delete();
				sl.bad.delete();
			} else if (ret == 2) {
				// sl.bad.delete();
				// sl.txt.delete();
			}

		}
	}

	private void handLineFreq(String table, List<EvdoRecordPair> list) {
		if (table.equals("CLT_PM_HDR_EVDO_CARR")) {
			int carrCount = 0;
			int index = 0;
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i).getName().equals("CARR")) {
					carrCount++;
					if (carrCount == 2) {
						index = i;
						break;
					}
				}
			}
			if (carrCount == 2) {
				String sect = "";
				for (EvdoRecordPair erp : list) {
					if (erp.getName().equals("SECT")) {
						sect = erp.getValue();
						break;
					}
				}
				List<EvdoRecordPair> alist = new ArrayList<EvdoRecordPair>();
				alist.add(new EvdoRecordPair("SECT", sect));
				for (int i = index; i < list.size(); i++) {
					alist.add(list.get(i));
				}

				for (int i = list.size() - 1; i >= index; i--) {
					list.remove(i);
				}
				handLine(table, alist);
			}
		}
	}

	private void handLine(String table, List<EvdoRecordPair> list) {
		if (sqlldrs == null)
			return;
		handLineFreq(table, list);
		list.add(new EvdoRecordPair("StartTime", starttime));
		list.add(new EvdoRecordPair("EndTime", endtime));
		list.add(new EvdoRecordPair("HDRC_RELEASE", hdrcRelease));
		list.add(new EvdoRecordPair("RNC", rnc));
		list.add(new EvdoRecordPair("RNC_GROUP_ID", rncGroupId));
		list.add(new EvdoRecordPair("OHM", ohm));
		list.add(new EvdoRecordPair("HCS", hcs));
		list.add(new EvdoRecordPair("FACTYPE", factype));
		list.add(new EvdoRecordPair("VERSION", version));
		list.add(new EvdoRecordPair("CONTL", contl));
		list.add(new EvdoRecordPair("RELEASE", release));

		SLInfo sl = sqlldrs.get(table);
		List<Field> templet = templets.get(table);
		List<String> tmp = new ArrayList<String>();
		for (int i = 0; i < templet.size(); i++)
			tmp.add("");
		for (int i = 0; i < templet.size(); i++) {
			Field f = templet.get(i);
			int idx = -1;
			for (int j = 0; j < list.size(); j++) {
				if (list.get(j).getName().equalsIgnoreCase(f.raw)) {
					idx = j;
					break;
				}
			}
			if (idx > -1)
				tmp.set(i, list.get(idx).getValue());
			else
				tmp.set(i, "");
		}
		sl.writerForTxt.print(omcid + ";" + collecttime + ";" + stamptime + ";");
		for (int i = 0; i < tmp.size(); i++) {
			sl.writerForTxt.print(tmp.get(i));
			if (i < tmp.size() - 1)
				sl.writerForTxt.print(";");
		}
		sl.writerForTxt.println();
		sl.writerForTxt.flush();
	}

	private String findFlag(String line) {
		String currFlag = null;
		if (line.startsWith("CARR ")) {
			currFlag = "CARR";
		} else if (line.startsWith("EVM ")) {
			currFlag = "EVM";
		} else if (line.startsWith("DATALINK ")) {
			currFlag = "DATALINK";
		} else if (line.startsWith("HCS ")) {
			currFlag = "HCS";
		} else if (line.startsWith("RNC ")) {
			currFlag = "RNC";
		} else if (line.startsWith("AP ")) {
			currFlag = "AP";
		} else if (line.startsWith("OHM ")) {
			currFlag = "OHM";
		} else if (line.startsWith("TP ")) {
			currFlag = "TP";
		} else if (line.startsWith("PGPF ")) {
			currFlag = "PGPF";
		} else if (line.startsWith("SECT ")) {
			currFlag = "SECT";
		}
		return currFlag;
	}

	private static String[] split0(String line) {
		String[] sp = line.split(",");
		String[] result = new String[2];
		if (sp == null || sp.length < 1) {
			result[0] = "";
			result[1] = "";
		} else {
			result[0] = sp[0].replace(":", "").trim();
			String s = "";
			for (int i = 1; i < sp.length; i++)
				s += sp[i];
			result[1] = s.trim();
		}
		return result;
	}

	private static String[] split1(String line) {
		String[] sp = line.split(" ");
		String[] result = new String[2];
		if (sp == null || sp.length < 1) {
			result[0] = "";
			result[1] = "";
		} else {
			result[0] = sp[0].replace(":", "").trim();
			String s = "";
			for (int i = 1; i < sp.length; i++)
				s += sp[i];
			result[1] = s.trim();
		}
		return result;
	}

	private Map<String, List<Field>> parsetemp(int tmpid) {
		Map<String, List<Field>> map = new HashMap<String, List<Field>>();
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement st = null;
		String sql = "select tempfilename from igp_conf_templet where tmpid = ?";

		try {
			String tmpName = null;
			con = DbPool.getConn();
			st = con.prepareStatement(sql);
			st.setInt(1, tmpid);
			rs = st.executeQuery();
			if (rs.next()) {
				tmpName = rs.getString(1);
			} else {
				throw new Exception(taskid + " 模板未找到，sql - " + sql);
			}

			File tmpFile = new File(SystemConfig.getInstance().getTempletPath() + File.separator + tmpName);
			if (!tmpFile.exists() || !tmpFile.isFile())
				throw new Exception(taskid + " 模板文件不存在。文件：" + tmpFile);

			SAXReader r = new SAXReader();
			Document doc = r.read(tmpFile);
			@SuppressWarnings("unchecked")
			List<Element> els0 = doc.getRootElement().elements("field");
			for (Element el0 : els0) {
				String table = el0.attributeValue("table");
				String raw = el0.attributeValue("raw");
				String col = el0.attributeValue("col");
				List<Field> list = null;
				if (map.containsKey(table)) {
					list = map.get(table);
				} else {
					list = new ArrayList<Field>();
					map.put(table, list);
				}
				list.add(new Field(raw, col));
			}
		} catch (Exception e) {
			log.error(taskid + " 查找模板时发生异常。sql - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return map;
	}

	private void initSqlldr() {
		if (sqlldrs != null)
			sqlldrs.clear();
		sqlldrs = new HashMap<String, SLInfo>();
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "cdma_alu_do_pm"
				+ File.separator + taskid + File.separator + Util.getDateString_yyyyMMddHHmmss(collectObjInfo.getLastCollectTime()) + File.separator);
		dir.mkdirs();
		long mil = System.currentTimeMillis();
		for (String table : templets.keySet()) {
			String base = table + "_" + mil;
			File txt = new File(dir, base + ".txt");
			File bad = new File(dir, base + ".bad");
			File log = new File(dir, base + ".log");
			File ctl = new File(dir, base + ".ctl");
			SLInfo sl = new SLInfo(txt, log, bad, ctl);
			sl.writerForCtl.println("load data");
			sl.writerForCtl.println("CHARACTERSET ZHS16GBK");
			sl.writerForCtl.println("infile '" + txt.getAbsolutePath() + "' append into table " + table);
			sl.writerForCtl.println("FIELDS TERMINATED BY \";\"");
			sl.writerForCtl.println("TRAILING NULLCOLS (");
			sl.writerForCtl.println("omcid,");
			sl.writerForCtl.println("collecttime date 'yyyy-mm-dd hh24:mi:ss',");
			sl.writerForCtl.println("stamptime date 'yyyy-mm-dd hh24:mi:ss',");
			sl.writerForTxt.print("omcid;collecttime;stamptime;");
			List<Field> fields = templets.get(table);
			for (int i = 0; i < fields.size(); i++) {
				Field f = fields.get(i);
				if (f.col.equalsIgnoreCase("StartTime") || f.col.equalsIgnoreCase("EndTime"))
					sl.writerForCtl.print(f.col + " date 'hh24:mi:ss, mm/dd/yyyy'");
				else
					sl.writerForCtl.print(f.col);
				sl.writerForTxt.print(f.col);
				if (i < fields.size() - 1) {
					sl.writerForCtl.println(",");
					sl.writerForTxt.print(";");
				}
			}
			sl.writerForTxt.println();
			sl.writerForTxt.flush();
			sl.writerForCtl.println(")");
			sl.writerForCtl.close();
			sqlldrs.put(table, sl);
		}
	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(111);
		obj.setParseTmpID(120222021);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2012-03-01 00:00:00").getTime()));
		DevInfo di = new DevInfo();
		di.setOmcID(123);
		obj.setDevInfo(di);
		PmParserDO p = new PmParserDO();
		p.setCollectObjInfo(obj);
		// p.setFileName("F:\\资料\\程序文档\\20120217_电信朗讯升级\\性能\\AG_201202071500GMT.HDRFMS028");
		p.setFileName("C:\\Users\\ChenSijiang\\Desktop\\AG_201207171100GMT.HDRFMS021");
		p.parseData();
	}
}
