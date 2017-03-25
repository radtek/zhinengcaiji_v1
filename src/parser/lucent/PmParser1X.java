package parser.lucent;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
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

/*解hsmr数据*/
public class PmParser1X extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	long taskid;

	List<Templet> templets;

	Map<String, SLInfo> sqlldrs;

	String omcid;

	String collecttime;

	String stamptime;

	@Override
	public boolean parseData() throws Exception {
		taskid = collectObjInfo.getTaskID();
		omcid = String.valueOf(collectObjInfo.getDevInfo().getOmcID());
		collecttime = Util.getDateString(new Date());
		stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());

		templets = parsetemp(collectObjInfo.getParseTmpID());
		if (templets == null || templets.isEmpty()) {
			log.error(taskid + " 模板不正确或为空。");
			return false;
		}

		initSqlldr();

		/* 这里放的是原始数据里的列头顺序，HEADERXX为KEY */
		Map<String, List<String>> rawCols = new HashMap<String, List<String>>();

		log.debug(taskid + " 开始解析 - " + fileName);
		InputStream in = null;
		Reader r = null;
		BufferedReader br = null;
		try {
			in = new FileInputStream(fileName);
			r = new InputStreamReader(in);
			br = new BufferedReader(r);
			StringBuilder sb = new StringBuilder();
			String line = null;
			int count = 0;
			while ((line = br.readLine()) != null) {
				count++;
				Templet t = isDataLine(line);
				if (t != null) {
					/* 确认文件中有HEADER定义。 */
					if (rawCols.containsKey(t.headSign)) {
						List<String> rawHeads = rawCols.get(t.headSign);
						line = line.substring(t.dataSign.length());
						String[] sp = line.split(t.splitSign);
						if (sp.length != rawHeads.size()) {
							log.warn(taskid + " 数据行列数与HEADER定义列数不一致。文件：" + fileName + "，行数：" + count);
							continue;
						}
						SLInfo sl = sqlldrs.get(t.table);
						if (sl != null) {
							sb.setLength(0);
							sb.append(omcid).append(";").append(collecttime).append(";");
							sb.append(stamptime).append(";");
							List<String> tmp = new ArrayList<String>(t.fields.size());
							for (int i = 0; i < t.fields.size(); i++)
								tmp.add("");
							for (int i = 0; i < t.fields.size(); i++) {
								int index = find(rawHeads, t.fields.get(i).raw);
								if (index > -1) {
									tmp.set(i, sp[index]);
								} else {
									tmp.set(i, "");
								}
							}
							for (int i = 0; i < tmp.size(); i++) {
								sb.append(tmp.get(i));
								if (i < tmp.size() - 1) {
									sb.append(";");
								}
							}
							sl.writerForTxt.println(sb);
							sb.setLength(0);
							tmp.clear();
						}
					}
				} else {
					t = isHeadLine(line);
					if (t != null) {
						if (!rawCols.containsKey(t.headSign)) {
							rawCols.put(t.headSign, new ArrayList<String>());
							line = line.substring(t.headSign.length());
							String[] sp = line.split(t.splitSign);
							for (String s : sp)
								rawCols.get(t.headSign).add(s);
						}
					} else {
						// 非数据行，也非列头行。
					}
				}
			}
			log.debug(taskid + "解析完成 - " + fileName + "，开始入库。");
			store();
			log.debug(taskid + "入库完成，文件：" + fileName);
		} catch (Exception e) {
			log.error(taskid + " 解析时发生例外。", e);
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(r);
			IOUtils.closeQuietly(in);
		}
		return true;
	}

	private static int find(List<String> sp, String name) {
		for (int i = 0; i < sp.size(); i++) {
			if (sp.get(i).equalsIgnoreCase(name)) {
				return i;
			}
		}
		return -1;
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

	private void initSqlldr() {
		if (sqlldrs != null)
			sqlldrs.clear();
		sqlldrs = new HashMap<String, SLInfo>();
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "cdma_alu_hsmr"
				+ File.separator + taskid + File.separator + Util.getDateString_yyyyMMddHHmmss(collectObjInfo.getLastCollectTime()) + File.separator);
		dir.mkdirs();
		long mil = System.currentTimeMillis();
		for (Templet t : templets) {
			String base = t.table + "_" + mil;
			File txt = new File(dir, base + ".txt");
			File bad = new File(dir, base + ".bad");
			File log = new File(dir, base + ".log");
			File ctl = new File(dir, base + ".ctl");
			SLInfo sl = new SLInfo(txt, log, bad, ctl);
			sl.writerForCtl.println("load data");
			sl.writerForCtl.println("CHARACTERSET ZHS16GBK");
			sl.writerForCtl.println("infile '" + txt.getAbsolutePath() + "' append into table " + t.table);
			sl.writerForCtl.println("FIELDS TERMINATED BY \";\"");
			sl.writerForCtl.println("TRAILING NULLCOLS (");
			sl.writerForCtl.println("omcid,");
			sl.writerForCtl.println("collecttime date 'yyyy-mm-dd hh24:mi:ss',");
			sl.writerForCtl.println("stamptime date 'yyyy-mm-dd hh24:mi:ss',");
			sl.writerForTxt.print("omcid;collecttime;stamptime;");
			for (int i = 0; i < t.fields.size(); i++) {
				Field f = t.fields.get(i);
				sl.writerForCtl.print(f.col);
				sl.writerForTxt.print(f.col);
				if (i < t.fields.size() - 1) {
					sl.writerForCtl.println(",");
					sl.writerForTxt.print(";");
				}
			}
			sl.writerForTxt.println();
			sl.writerForTxt.flush();
			sl.writerForCtl.println(")");
			sl.writerForCtl.close();
			sqlldrs.put(t.table, sl);
		}
	}

	private Templet isDataLine(String line) {
		for (Templet t : templets) {
			if (line.startsWith(t.dataSign))
				return t;
		}
		return null;
	}

	private Templet isHeadLine(String line) {
		for (Templet t : templets) {
			if (line.startsWith(t.headSign))
				return t;
		}
		return null;
	}

	private List<Templet> parsetemp(int tmpid) {
		List<Templet> list = new ArrayList<Templet>();
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
			List<Element> els0 = doc.getRootElement().elements("templet");
			for (Element el0 : els0) {
				String table = el0.attributeValue("table");
				String headSign = el0.attributeValue("headSign");
				String dataSign = el0.attributeValue("dataSign");
				String splitSign = el0.attributeValue("splitSign");
				List<Field> fields = new ArrayList<Field>();
				List<Element> els1 = el0.elements("field");
				for (Element el1 : els1) {
					fields.add(new Field(el1.attributeValue("raw"), el1.attributeValue("col")));
				}
				list.add(new Templet(table, headSign, dataSign, splitSign, fields));
			}
		} catch (Exception e) {
			log.error(taskid + " 查找模板时发生异常。sql - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return list;
	}

	static class Templet {

		String table;

		String headSign;

		String dataSign;

		String splitSign;

		List<Field> fields;

		public Templet(String table, String headSign, String dataSign, String splitSign, List<Field> fields) {
			super();
			this.table = table;
			this.headSign = headSign;
			this.dataSign = dataSign;
			this.splitSign = splitSign;
			this.fields = fields;
		}

		@Override
		public String toString() {
			return "Templet [dataSign=" + dataSign + ", fields=" + fields + ", headSign=" + headSign + ", splitSign=" + splitSign + ", table="
					+ table + "]";
		}

	}

	static class Field {

		String raw;

		String col;

		public Field(String raw, String col) {
			super();
			this.raw = raw;
			this.col = col;
		}

		@Override
		public String toString() {
			return "Field [col=" + col + ", raw=" + raw + "]";
		}

	}

	static class SLInfo implements Closeable {

		File txt;

		File log;

		File bad;

		File ctl;

		PrintWriter writerForTxt;

		PrintWriter writerForCtl;

		public SLInfo(File txt, File log, File bad, File ctl) {
			super();
			this.txt = txt;
			this.log = log;
			this.bad = bad;
			this.ctl = ctl;
			try {
				writerForTxt = new PrintWriter(txt);
				writerForCtl = new PrintWriter(ctl);
			} catch (Exception ex) {
				ex.printStackTrace();
			}

		}

		@Override
		public void close() {
			IOUtils.closeQuietly(writerForCtl);
			IOUtils.closeQuietly(writerForTxt);

		}

	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(111);
		obj.setParseTmpID(120222011);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2012-02-20 12:00:00").getTime()));
		DevInfo di = new DevInfo();
		di.setOmcID(123);
		obj.setDevInfo(di);
		PmParser1X p = new PmParser1X();
		p.setCollectObjInfo(obj);
		p.setFileName("F:\\资料\\程序文档\\20120217_电信朗讯升级\\性能\\CMS19-2012020711.hsmr");
		p.parseData();
	}
}
