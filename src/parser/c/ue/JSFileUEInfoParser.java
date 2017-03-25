package parser.c.ue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.ZipUtil;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 针对江苏电信终端采集，之前是数据库方式的，现在改成了文件。 文件格式：竖线“|”分隔的CSV文件，有9列，格式为“序列号|手机号|IMSI|省名|市名|手机厂家名|手机型号|软件版本|注册时间”。 样例：
 * 
 * <pre>
 * A1000033C03B98|15358558017|460036710278890|江苏|扬州|华立时代|SHL-H3119|1.6.4443|2013-12-31 00:00:08
 * </pre>
 * 
 * @author chensijiang 2014-8-21
 * @see JSDbUEInfoParser
 */
public class JSFileUEInfoParser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final String MODEL_TABLE = "clt_terminal_tacinfo";

	/* 将原始终端信息入库的表。 */
	private static final String TAC_INFO_STORE_TABLE = "CLT_TAIZ_T_SA_TERMINAL";

	private Map<ModelInfo, Short> modelMap = new HashMap<ModelInfo, Short>();

	private long taskid;

	private Map<String/* 文件绝对路径 */, RandomAccessFile> writers = new HashMap<String, RandomAccessFile>();

	private File basedir;

	File sqlldrCtl;

	File sqlldrLog;

	File sqlldrTxt;

	File sqlldrBad;

	PrintWriter txtWriter;

	String stampTime;

	String omcId;

	String cltTime;

	@Override
	public boolean parseData() throws Exception {
		this.taskid = getCollectObjInfo().getTaskID();
		stampTime = Util.getDateString(getCollectObjInfo().getLastCollectTime());
		omcId = String.valueOf(getCollectObjInfo().getDevInfo().getOmcID());
		cltTime = Util.getDateString(new Date());

		log.debug(taskid + " 开始采集终端信息（生成IGP3索引文件）：" + this.fileName);
		loadModelMap();
		try {
			initWriters();
			initSqlldr();
			log.debug(taskid + " 目录初始化完成，基准目录：" + basedir);
		} catch (Exception e) {
			log.error(taskid + " 目录初始化失败，任务结束。dir：" + basedir, e);
			return false;
		}
		InputStream in = null;
		LineIterator lines = null;
		boolean suc = false;
		try {
			in = new FileInputStream(fileName);
			lines = IOUtils.lineIterator(in, "gbk");
			this.handleLines(lines);
			suc = true;
		} catch (Exception e) {
			log.error(taskid + " 采集终端信息（生成IGP3索引文件），执行失败。", e);
			return false;
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(txtWriter);
			log.debug(taskid + " 开始关闭索引文件。");
			Iterator<String> it = writers.keySet().iterator();
			while (it.hasNext()) {
				String file = it.next();
				RandomAccessFile raf = writers.get(file);
				if (raf != null)
					raf.close();
			}
			writers.clear();
			log.debug(taskid + " 索引文件关闭结束。");

			if (suc) {
				// 打包成zip
				File targetFile = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "IMSI_INDEX.zip");
				File bakFile = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "IMSI_INDEX.zip.bak."
						+ Util.getDateString_yyyyMMdd(new Date()));
				if (targetFile.exists())
					targetFile.renameTo(bakFile);
				log.debug("开始压缩 - " + targetFile + " ...");
				if (ZipUtil.zipDir(basedir, targetFile, false)) {
					log.debug("IMSI_INDEX.zip压缩成功。");
				} else {
					log.warn("IMSI_INDEX.zip压缩失败。");
				}
			}

			// sqlldr
			this.store();

		}

		return true;
	}

	static boolean isBlank(String string) {
		return string == null || "".equals(string.trim());
	}

	boolean validate(String vendor, String termodel) {
		return isBlank(vendor) || isBlank(termodel);
	}

	void handleLines(LineIterator lines) throws Exception {
		int count = 0;

		String line;
		while (lines.hasNext()) {
			line = lines.nextLine();
			count++;
			short imsiPart1 = 0;
			short imsiPart2 = 0;
			short imsiPart3 = 0;
			line =line.replace("NULL", "").replace(".000", "");
			String[] sp = line.split(",");
			String shortImsi = null;
			String imsi = null;
			imsi = sp[2].trim();

			String meid = sp[0].trim();

			String vendor = sp[5].trim();
			String termmodel = sp[6].trim();
			StringBuilder buf = new StringBuilder();
			buf.append(omcId).append("|");
			buf.append(cltTime).append("|");
			buf.append(stampTime).append("|");
			// buf.append(imsi).append("|");
			//
			// buf.append(sp[1].trim()).append("|");
			// buf.append(vendor).append("|");
			// buf.append(termmodel);

			for (int i = 0; i < sp.length - 1; i++) {
				if (i == 8) {
					sp[i] = sp[i].replace(".000", "");
				}

				if ("NULL".equals(sp[i]) || "null".equals(sp[i])) {
					sp[i] = "";
				}
				buf.append(sp[i]).append("|");

			}
			if ("NULL".equals(sp.length - 1)  || "null".equals(sp[sp.length - 1])) {
				sp[sp.length - 1] = "";
				sp[sp.length - 1] = sp[sp.length - 1].replace(".000", "");
			}
			
			buf.append(sp[sp.length - 1]).append("|");

			txtWriter.println(buf.toString());
			buf.setLength(0);
			buf = null;
			if (sp == null || sp.length < 9)
				continue;
			try {
				if (validate(vendor, termmodel)) {
					continue;
				}
				imsi = imsi.trim();
				vendor = vendor.trim();
				termmodel = termmodel.trim();

				if (imsi.length() < 5) {

					log.debug("imsi<5,   不符合条件的数据 " + line);
					continue;
				}

				shortImsi = imsi.substring(5);

				if (shortImsi.length() < 10) {

					log.debug(" imsi《16,  不符合条件的数据 " + line);
					continue;
				}

				imsiPart1 = Short.parseShort(shortImsi.substring(0, 2));
				imsiPart2 = Short.parseShort(shortImsi.substring(2, 6));
				imsiPart3 = Short.parseShort(shortImsi.substring(6, 10));
				ModelInfo mi = new ModelInfo((short) 0, vendor, termmodel);
				short typeid = -1;
				if (this.modelMap.containsKey(mi)) {
					Short s = this.modelMap.get(mi);
					if (s != null)
						typeid = s;
				} else {
					if (this.addModel(mi)) {
						Short s = this.modelMap.get(mi);
						if (s != null)
							typeid = s;
					}
				}

				if (typeid > -1) {
					File bin = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX" + File.separator + wrap2(imsiPart1)
							+ File.separator + wrap4(imsiPart2) + ".bin");
					RandomAccessFile raf = null;
					if (writers.containsKey(bin.getAbsolutePath())) {
						raf = writers.get(bin.getAbsolutePath());
					} else {
						bin.getParentFile().mkdirs();
						raf = new RandomAccessFile(bin, "rw");
						writers.put(bin.getAbsolutePath(), raf);
					}
					/* 每个数据单元占2字节，所以每次seek要乘以2. */
					raf.seek(imsiPart3 * 2);
					writeSwappedShort(raf, typeid);
				}
				if (count % 10000 == 0)
					log.debug(taskid + " 处理了" + count + "条记录.");

			} catch (NumberFormatException e) {
				continue;
			}
		}
	}

	boolean addModel(ModelInfo mi) {
		String sql = "insert into " + MODEL_TABLE + " (tactypeid,tac_vendor,mobile_model) values (" + "(select nvl(max(tactypeid),0)+1 from "
				+ MODEL_TABLE + "),?,?)";
		Connection conn = null;
		PreparedStatement pss = null;
		try {
			conn = DbPool.getConn();
			pss = conn.prepareStatement(sql);
			pss.setString(1, mi.vendor);
			pss.setString(2, mi.model);
			pss.executeUpdate();
		} catch (Exception e) {
			log.error(taskid + " 添加终端型号编号表失败，SQL：" + sql + "，mi：" + mi, e);
			return false;
		} finally {
			CommonDB.close(null, pss, conn);
		}
		loadModelMap();
		log.debug(taskid + " 成功添加了一条新终端型号： " + mi);
		return true;
	}

	void loadModelMap() {
		String sql = "select tac_vendor,mobile_model,max(tactypeid) as tactypeid from " + MODEL_TABLE + " group by tac_vendor,mobile_model";
		Connection conn = null;
		ResultSet rss = null;
		Statement stt = null;
		this.modelMap.clear();
		try {
			conn = DbPool.getConn();
			stt = conn.createStatement();
			// log.debug(taskid + " 查询终端编号表：" + sql);
			rss = stt.executeQuery(sql);
			while (rss.next()) {
				ModelInfo mi = new ModelInfo(rss.getShort("tactypeid"), rss.getString("tac_vendor"), rss.getString("mobile_model"));
				this.modelMap.put(mi, mi.id);
			}
		} catch (Exception e) {
			log.error(taskid + " 查询终端编号表失败：" + sql, e);
		} finally {
			CommonDB.close(rss, stt, conn);
		}
	}

	private void store() {
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
				.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), sqlldrCtl.getAbsoluteFile(),
				sqlldrBad.getAbsoluteFile(), sqlldrLog.getAbsoluteFile());
		log.debug("执行 " + cmd);
		ExternalCmd execute = new ExternalCmd();
		try {
			int ret = execute.execute(cmd);
			SqlldrResult result = new SqlLdrLogAnalyzer().analysis(sqlldrLog.getAbsolutePath());
			log.debug("exit=" + ret + " omcid=" + collectObjInfo.getDevInfo().getOmcID() + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
					+ result.getTableName() + " 数据时间=" + Util.getDateString(collectObjInfo.getLastCollectTime()) + " sqlldr日志="
					+ sqlldrLog.getAbsolutePath());
			LogMgr.getInstance()
					.getDBLogger()
					.log(collectObjInfo.getDevInfo().getOmcID(), result.getTableName(), collectObjInfo.getLastCollectTime(),
							result.getLoadSuccCount(), collectObjInfo.getTaskID());
			if (ret == 0) {
				sqlldrTxt.delete();
				sqlldrCtl.delete();
				sqlldrLog.delete();
				sqlldrBad.delete();
			} else if (ret == 2) {
				// sqlldrTxt.delete();
				// sqlldrBad.delete();
			}
		} catch (Exception ex) {
			log.error("sqlldr时异常", ex);
		}
	}

	private void initSqlldr() throws Exception {
		// sqlldr入库原始文件。
		File sqlldrDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator);
		if (!sqlldrDir.exists() && !sqlldrDir.mkdirs())
			throw new Exception("无法创建sqlldr临时目录：" + sqlldrDir);
		sqlldrCtl = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator + "store.ctl");
		sqlldrTxt = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator + "store.txt");
		sqlldrLog = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator + "store.log");
		sqlldrBad = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator + "store.bad");
		txtWriter = new PrintWriter(sqlldrTxt);
		txtWriter
				.println("OMCID|COLLECTTIME|STAMPTIME|MEID|SUBSCRIBER_NUMBER|IMSI|PROVINCE_NAME|CITY_NAME|VENDOR|TERMMODEL|TERM_VERSION|SMS_SEND_TIME|REGIST_TIME");
		PrintWriter ctlWriter = null;
		try {
			ctlWriter = new PrintWriter(sqlldrCtl);
			ctlWriter.println("LOAD DATA\n" + "CHARACTERSET ZHS16GBK\n" + "INFILE '" + sqlldrTxt.getAbsolutePath() + "' APPEND INTO TABLE "
					+ TAC_INFO_STORE_TABLE + "\n" + "FIELDS TERMINATED BY \"|\"\n" + "TRAILING NULLCOLS\n" + "(\n" + "  OMCID,\n"
					+ "  COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',\n" + "  STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',\n" + "  MEID,\n"
					+ "  SUBSCRIBER_NUMBER,\n" + "  IMSI,\n" + "  PROVINCE_NAME,\n" + "  CITY_NAME,\n" + "  VENDOR,\n" + "  TERMMODEL,\n"
					+ "  TERM_VERSION,\n" + "  SMS_SEND_TIME DATE 'YYYY-MM-DD HH24:MI:SS' ,\n" + "  REGIST_TIME DATE 'YYYYMMDDHH24MISS'\n" + ")");
		} finally {
			IOUtils.closeQuietly(ctlWriter);
		}
	}

	private void initWriters() throws Exception {

		basedir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info" + File.separator + collectObjInfo.getKeyID()
				+ File.separator);
		if (!basedir.exists() && !basedir.mkdirs()) {
			throw new Exception("创建数据目录失败 - " + basedir);
		}

		// 之前有老的imsi_index.zip的话，解压出来，以它的基础上新增/更新。
		File oldImsiIndexZip = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "IMSI_INDEX.zip");
		if (oldImsiIndexZip.exists() && oldImsiIndexZip.isFile()) {
			log.debug("存在IMSI_INDEX.zip，开始解压。");
			InputStream in = null;
			ZipInputStream zipIn = null;
			try {
				in = new FileInputStream(oldImsiIndexZip);
				zipIn = new ZipInputStream(in);
				ZipEntry entry;
				while ((entry = zipIn.getNextEntry()) != null) {
					// 是目录是，不用管，文件名上已经带有目录信息了。
					if (entry.isDirectory())
						continue;
					// 这个名字上带有目录的，比如“07/1919.bin”。
					String zipEntryFileName = entry.getName();
					File outFile = new File(basedir + File.separator + zipEntryFileName);
					File outFileDir = outFile.getParentFile();
					if (!outFileDir.exists() && !outFileDir.mkdirs())
						throw new Exception("无法创建目录：" + outFileDir);
					OutputStream out = null;
					try {
						out = new FileOutputStream(outFile);
						IOUtils.copy(zipIn, out);
						out.flush();
					} finally {
						IOUtils.closeQuietly(out);
					}
					log.debug("已解压文件：" + outFile);
				}
			} finally {
				IOUtils.closeQuietly(zipIn);
				IOUtils.closeQuietly(in);
			}
			log.debug("IMSI_INDEX.zip解压结束。");
		}

	}

	/* 数字不够2位的，前面补零。 */
	private static String wrap2(short val) {
		if (val > 9)
			return String.valueOf(val);
		return "0" + val;
	}

	/* 数字不够3位的，前面补零。 */
	private static String wrap3(short val) {
		if (val > 99)
			return String.valueOf(val);
		return "0" + wrap2(val);
	}

	/* 数字不够4位的，前面补零。 */
	private static String wrap4(short val) {
		if (val > 999)
			return String.valueOf(val);
		return "0" + wrap3(val);
	}

	static void writeSwappedShort(RandomAccessFile output, short value) throws IOException {
		output.write((byte) ((value >> 0) & 0xff));
		output.write((byte) ((value >> 8) & 0xff));
	}

	public static void main(String[] args) throws Exception {
		/*
		 * JSFileUEInfoParser p = new JSFileUEInfoParser(); p.fileName = "C:\\Users\\Admin\\Desktop\\phone_type_all\\phone_type_all.txt";
		 * CollectObjInfo task = new CollectObjInfo(1); task.setLastCollectTime(new Timestamp(Util.getDate1("2014-04-06 23:00:00").getTime()));
		 * DevInfo di = new DevInfo(); task.setDevInfo(di); p.setCollectObjInfo(task); p.parseData();
		 */

		JSFileUEInfoParser p = new JSFileUEInfoParser();
		p.fileName = SystemConfig.getInstance().getTmpTestpath();
		String omcId = SystemConfig.getInstance().getTmpTestOmcId();;
		String taskid = SystemConfig.getInstance().getTmpTestTaskId();;

		CollectObjInfo task = new CollectObjInfo(Integer.valueOf(taskid));

		task.setLastCollectTime(new Timestamp(Util.getDate1(SystemConfig.getInstance().getTmpTestDataTime()).getTime()));
		DevInfo di = new DevInfo();
		di.setOmcID(Integer.valueOf(omcId));
		task.setDevInfo(di);
		p.setCollectObjInfo(task);
		p.parseData();
	}

}
