package parser.lucent.cm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import parser.Parser;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

public class LucntCdmaFtpPara extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private String omcid;

	@Override
	public boolean parseData() throws Exception {
		if (FilenameUtils.getExtension(fileName).equals("sct")) {
			new File(fileName).delete();
			return false;
		}

		omcid = collectObjInfo.getDevInfo().getOmcID() + "";

		if (SystemConfig.getInstance().isSPAS()) {
			log.debug("isSPAS=true");
			String nfileName = FilenameUtils.normalize(this.fileName);
			String zip = collectObjInfo.filenameMap.get(nfileName);
			if (zip == null || !zip.contains("_PARA_")) {
				log.warn(collectObjInfo.getTaskID() + " 文件" + fileName + "，未找到对应的原始压缩包名。list=" + collectObjInfo.filenameMap);
			} else {
				zip = FilenameUtils.getBaseName(zip);
				String[] sp = zip.split("_");
				omcid = sp[4];
			}
		}

		Map<String, List<TempletEntry>> temp = parseTemp(collectObjInfo.getParseTmpID());
		List<TempletEntry> list = temp.get(FilenameUtils.getName(fileName).toLowerCase());
		if (list == null) {
			logger.error(collectObjInfo.getTaskID() + " 文件" + fileName + "未找到对应模板");
			return false;
		}
		String table = list.get(0).pub.tableName;

		TempletPublic pub = list.get(0).pub;

		String collecttime = Util.getDateString(new Date());
		String stamptime = Util.getDateString(collectObjInfo.getLastCollectTime());
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + "cdma_lucnt_cm"
				+ File.separator + collectObjInfo.getTaskID());
		dir.mkdirs();

		String baseName = table + "_" + System.currentTimeMillis();
		File ctl = new File(dir, baseName + ".ctl");
		File bad = new File(dir, baseName + ".bad");
		File txt = new File(dir, baseName + ".txt");
		File log = new File(dir, baseName + ".log");

		PrintWriter ctlPW = new PrintWriter(ctl);
		PrintWriter txtPW = new PrintWriter(txt);

		ctlPW.println("load data");
		ctlPW.println("CHARACTERSET ZHS16GBK");
		ctlPW.println("infile '" + txt.getAbsolutePath() + "' append into table " + table);
		ctlPW.println("FIELDS TERMINATED BY \";\"");
		ctlPW.println("TRAILING NULLCOLS (");
		ctlPW.println("omcid,");
		ctlPW.println("collecttime date 'yyyy-mm-dd hh24:mi:ss',");
		ctlPW.println("stamptime date 'yyyy-mm-dd hh24:mi:ss',");
		txtPW.print("omcid;collecttime;stamptime;");
		for (int i = 0; i < list.size(); i++) {
			txtPW.print(list.get(i).dest);
			ctlPW.print(list.get(i).dest);
			if (i < list.size() - 1) {
				txtPW.print(";");
				ctlPW.print(",");
				ctlPW.println();
			}
		}
		txtPW.println();
		txtPW.flush();
		ctlPW.print(")");
		ctlPW.flush();
		ctlPW.close();

		InputStream in = null;
		BufferedReader br = null;
		Reader r = null;
		try {
			in = new FileInputStream(fileName);
			r = new InputStreamReader(in);
			br = new BufferedReader(r);
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] sp = line.split(pub.separator);
				txtPW.print(omcid + ";" + collecttime + ";" + stamptime + ";");
				for (int i = 0; i < list.size(); i++) {
					if (i < sp.length) {
						txtPW.print(sp[i] != null ? sp[i].trim() : "");

					} else {
						txtPW.print("");
					}
					if (i < list.size() - 1)
						txtPW.print(";");
				}
				txtPW.println();
				txtPW.flush();
			}

			txtPW.close();
			String cmd = "sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999";
			SystemConfig cfg = SystemConfig.getInstance();
			cmd = String.format(cmd, cfg.getDbUserName(), cfg.getDbPassword(), cfg.getDbService(), 1, ctl.getAbsolutePath(), bad.getAbsolutePath(),
					log.getAbsolutePath());
			logger.debug(collectObjInfo.getTaskID() + " 执行sqlldr - " + cmd);
			int ret = new ExternalCmd().execute(cmd);
			SqlldrResult re = new SqlLdrLogAnalyzer().analysis(log.getAbsolutePath());
			logger.debug(collectObjInfo.getTaskID() + " sqlldr完毕(ret=" + ret + ")，omcid=" + omcid + "，表=" + table + "，入库条数=" + re.getLoadSuccCount()
					+ "，log=" + log.getAbsolutePath());
			LogMgr.getInstance().getDBLogger()
					.log(Integer.parseInt(omcid), table, collectObjInfo.getLastCollectTime(), re.getLoadSuccCount(), collectObjInfo.getTaskID());
			if (ret == 0) {
				log.delete();
				txt.delete();
				bad.delete();
				ctl.delete();
			} else if (ret == 2) {
				txt.delete();
				bad.delete();
			}
		} catch (Exception e) {
			logger.error(collectObjInfo.getTaskID() + " 处理CDMA阿朗参数时异常。", e);
		} finally {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(r);
			IOUtils.closeQuietly(in);
		}

		return true;
	}

	static Map<String, List<TempletEntry>> parseTemp(int tmpId) {
		Map<String, List<TempletEntry>> map = new HashMap<String, List<TempletEntry>>();

		Connection con = null;
		ResultSet rs = null;
		Statement st = null;
		String sql = "select tempfilename from igp_conf_templet where tmpid=" + tmpId;

		try {
			String tmpName = null;
			con = DbPool.getConn();
			st = con.createStatement();
			rs = st.executeQuery(sql);
			if (rs.next()) {
				tmpName = rs.getString(1);
			} else {
				throw new Exception("模板未找到，sql - " + sql);
			}

			File tmpFile = new File(SystemConfig.getInstance().getTempletPath() + File.separator + tmpName);
			if (!tmpFile.exists() || !tmpFile.isFile())
				throw new Exception("模板文件不存在。文件：" + tmpFile);

			SAXReader r = new SAXReader();
			Document doc = r.read(tmpFile);
			List<Element> els0 = doc.getRootElement().elements("templet");
			for (Element el0 : els0) {
				TempletPublic tp = new TempletPublic();
				tp.tableName = el0.attributeValue("table");
				tp.sct = el0.attributeValue("sct");
				tp.log = el0.attributeValue("log");
				tp.separator = el0.attributeValue("separator");
				List<Element> els1 = el0.elements("field");
				List<TempletEntry> entries = new ArrayList<TempletEntry>();
				for (Element el1 : els1) {
					TempletEntry te = new TempletEntry();
					te.pub = tp;
					te.src = el1.attributeValue("src");
					te.dest = el1.attributeValue("dest");
					entries.add(te);
				}
				map.put(tp.log.toLowerCase(), entries);
			}
		} catch (Exception e) {
			logger.error("查找模板时发生异常。sql - " + sql, e);
		} finally {
			CommonDB.close(rs, st, con);
		}
		return map;
	}

	static class TempletEntry {

		TempletPublic pub;

		String src;

		String dest;
	}

	static class TempletPublic {

		String tableName;

		String sct;

		String log;

		String separator;
	}

}
