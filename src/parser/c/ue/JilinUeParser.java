package parser.c.ue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.ZipUtil;
import util.loganalyzer.SqlLdrLogAnalyzer;
import util.opencsv.CSVParser;
import framework.SystemConfig;

/**
 * 吉林终端。
 * 
 * @author ChensSijiang 2013-5-13
 */
public class JilinUeParser extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String FILED_SEP = ",";

	private static final String TABLE_NAME_MODEL = "CLT_TERMINAL_TACINFO";

	private static final String SQL_MODEL_TABLE = "select tactypeid,tac_vendor,mobile_model from " + TABLE_NAME_MODEL;

	/* 将原始终端信息入库的表。 */
	private static final String TAC_INFO_STORE_TABLE = "CLT_TAIZ_T_SA_TERMINAL";

	private static final Map<String, ModelEntry> MODELS = new HashMap<String, ModelEntry>();

	private StringBuilder buff = new StringBuilder();

	private Map<String/* 文件绝对路径 */, RandomAccessFile> writers = new HashMap<String, RandomAccessFile>();

	private File basedir;

	private boolean imsiIndex = true;

	private boolean mdnIndex = true;

	private File sqlldrTxt;

	private File sqlldrCtl;

	private File sqlldrBad;

	private File sqlldrLog;

	private PrintWriter txtWriter;

	private PrintWriter ctlWriter;

	private String strStamptime;

	private String strCollecttime;

	static {
		loadModels();
	}

	private static void loadModels() {
		MODELS.clear();
		Result result = null;
		try {
			result = CommonDB.queryForResult(SQL_MODEL_TABLE);
		} catch (Exception e) {
			logger.error("载入手机型号信息时出现异常。", e);
		}
		SortedMap[] sm = result.getRows();
		for (SortedMap s : sm) {
			ModelEntry me = new ModelEntry();
			me.modelId = Short.parseShort(s.get("tactypeid").toString());
			me.modelName = s.get("mobile_model").toString();
			me.vendorName = s.get("tac_vendor").toString();
			MODELS.put(me.modelName, me);
		}

	}

	// 添加新型号。
	private static boolean addModel(ModelEntry me) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "insert into " + TABLE_NAME_MODEL + " (tactypeid,tac_vendor,mobile_model) values (" + "(select nvl(max(tactypeid),0)+1 from "
				+ TABLE_NAME_MODEL + "),?,?)";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setString(1, me.vendorName);
			ps.setString(2, me.modelName);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("添加新手机型号时出现异常 " + me, e);
			return false;
		} finally {
			CommonDB.close(null, ps, con);
		}
		loadModels();
		logger.debug("成功添加了一条新型号- " + me);
		return true;
	}

	@Override
	public boolean parseData() throws Exception {
		if (this.collectObjInfo instanceof RegatherObjInfo) {
			log.warn("此任务不允许补采任务：" + this.collectObjInfo.getTaskID());
			return true;
		}
		this.strCollecttime = Util.getDateString(new Date());
		this.strStamptime = Util.getDateString(this.collectObjInfo.getLastCollectTime());

		File sqlldrDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator);
		sqlldrDir.mkdirs();
		String basesqlldrname = TAC_INFO_STORE_TABLE + "_" + System.currentTimeMillis();
		sqlldrTxt = new File(sqlldrDir, basesqlldrname + ".txt");
		sqlldrLog = new File(sqlldrDir, basesqlldrname + ".log");
		sqlldrBad = new File(sqlldrDir, basesqlldrname + ".bad");
		sqlldrCtl = new File(sqlldrDir, basesqlldrname + ".ctl");

		BufferedReader br = null;
		// LineIterator lines = null;
		try {
			txtWriter = new PrintWriter(sqlldrTxt);
			ctlWriter = new PrintWriter(sqlldrCtl);
			initSqlldr();
			initWriters();
			// in = new FileInputStream(fileName);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName), "gbk"));
			// lines = IOUtils.lineIterator(in, null);
			String sp[] = null;
			int count = 0;
			String remaining = null;
			CSVParser csv = new CSVParser(',', '\"');
			br.readLine();
			String line = null;
			while ((line = br.readLine()) != null) {
				count++;
				try {
					sp = csv.parseLine(line);
					// 原始文件有表头，有9个字段，分别是："地市","手机号码","品牌","型号","MEID","入网时间","注册时间","IMSI","操作系统"。
					// 第1个字段跳过。
					if (sp.length < 5) {
						if (remaining != null) {
							line = remaining + line;
							remaining = null;
							sp = line.split(FILED_SEP);
						} else {
							remaining = line.trim();
							count--;
							continue;
						}
					}
					InfoEntry ie = new InfoEntry();
					String imsi = sp[7];
					String fullImsi = imsi;
					if (imsi.length() != 15) {
						logger.warn("imsi error:" + imsi + ",line:" + count);
						continue;
					}
					String mdn = sp[1].trim();
					if (mdn.length() != 11) {
						logger.warn("mdn error:" + mdn + ",line:" + count);
						continue;
					}
					ie.mdnPart1 = Short.parseShort(mdn.substring(0, 3));
					ie.mdnPart2 = Short.parseShort(mdn.substring(3, 7));
					ie.mdnPart3 = Short.parseShort(mdn.substring(7, 11));
					imsi = imsi.substring(5);
					ie.imsiPart1 = Short.parseShort(imsi.substring(0, 2));
					ie.imsiPart2 = Short.parseShort(imsi.substring(2, 6));
					ie.imsiPart3 = Short.parseShort(imsi.substring(6, 10));

					String vendor = sp[2].trim();
					if (Util.isNull(vendor))
						continue;
					String model = sp[3].trim();
					if (Util.isNull(model))
						continue;
					if (MODELS.containsKey(model)) {
						ie.model = MODELS.get(model).modelId;
					} else {
						if (addModel(new ModelEntry((short) 0, model, vendor))) {
							ie.model = MODELS.get(model).modelId;
						} else {
							continue;
						}
					}

					buff.setLength(0);
					buff.append(collectObjInfo.getDevInfo().getOmcID()).append("|");
					buff.append(strCollecttime).append("|");
					buff.append(strStamptime).append("|");
					buff.append(fullImsi).append("|");
					buff.append(mdn).append("|");// SUBSCRIBER_NUMBER
					buff.append(vendor).append("|");
					buff.append(model);
					txtWriter.println(buff);
					txtWriter.flush();

					// 以imsi作为索引。
					// 比如要查找IMSI为460030904590226的用户的手机型号，则先找到“设置的存放根目录/IMSI_INDEX/09/0459.bin”文件，
					// 再seek到226*2个字节的位置，再读取2个字节，即是手机型号的编号，拿到此编号后，在数据库表里查找具体型号内容。
					// 即imsi前面的46003去掉，省下0904590226，其中前两位09是目录，中间四位0459是文件名，最后4位0226是要seek的长度依据，以此方法较快的索引手机型号。
					if (imsiIndex) {
						File bin = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX" + File.separator + wrap2(ie.imsiPart1)
								+ File.separator + wrap4(ie.imsiPart2) + ".bin");
						RandomAccessFile raf = null;
						if (writers.containsKey(bin.getAbsolutePath())) {
							raf = writers.get(bin.getAbsolutePath());
						} else {
							bin.getParentFile().mkdirs();
							raf = new RandomAccessFile(bin, "rw");
							writers.put(bin.getAbsolutePath(), raf);
						}
						/* 每个数据单元占2字节，所以每次seek要乘以2. */
						raf.seek(ie.imsiPart3 * 2);
						ie.writeToFile(raf);
					}
					// 以mdn作为索引。
					// 比如要查找MDN为15389803333的用户的手机型号，则先找到“设置的存放根目录/MDN_INDEX/153/8980.bin”文件，
					// 再seek到333*2个字节的位置，再读取2个字节，即是手机型号的编号，拿到此编号后，在数据库表里查找具体型号内容。
					// 即mdn的前三位153是目录，中间四位8980是文件名，最后四位3333是要seek的长度依据，以此方法较快的索引手机型号。
					if (mdnIndex) {
						File bin = new File(basedir.getAbsolutePath() + File.separator + "MDN_INDEX" + File.separator + wrap3(ie.mdnPart1)
								+ File.separator + wrap4(ie.mdnPart2) + ".bin");
						RandomAccessFile raf = null;
						if (writers.containsKey(bin.getAbsolutePath())) {
							raf = writers.get(bin.getAbsolutePath());
						} else {
							bin.getParentFile().mkdirs();
							raf = new RandomAccessFile(bin, "rw");
							writers.put(bin.getAbsolutePath(), raf);
						}
						/* 每个数据单元占2字节，所以每次seek要乘以2. */
						raf.seek(ie.mdnPart3 * 2);
						ie.writeToFile(raf);
					}
				} catch (NumberFormatException e) {
					logger.warn("MDN或IMSI内容异常（包含非数字内容）。数据：" + line + "，行号：" + count);
				} catch (Exception e) {
					logger.warn("处理一行终端信息时出现意外。数据：" + line + "，行号：" + count, e);
				}
			}
			count = 0;
		} catch (Exception e) {
			logger.error("终端信息解析中出现异常。", e);
			return false;
		} finally {
			for (RandomAccessFile r : writers.values()) {
				if (r != null)
					r.close();
			}
			writers.clear();
			// LineIterator.closeQuietly(lines);
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(ctlWriter);
			IOUtils.closeQuietly(txtWriter);
		}

		// 索引文件打包为IMSI_INDEX.zip
		// currentpath要配到innerdata这一级目录。
		File imsi_target = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX.zip");
		File bakImsiIndex = new File(imsi_target.getAbsolutePath() + ".history." + Util.getDateString_yyyyMMddHHmmss(new Date()));
		if (imsi_target.exists()) {
			// IMSI_INDEX.zip已存在，备份。
			if (imsi_target.renameTo(bakImsiIndex)) {
				log.debug("已将文件+" + imsi_target + "备份为" + bakImsiIndex);
			} else {
				log.warn("将文件+" + imsi_target + "备份为" + bakImsiIndex + "时失败。");
			}
		}
		File srcDir = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX");
		log.debug("准备zip压缩，源目录：" + srcDir + "，目标文件：" + imsi_target);
		if (ZipUtil.zipDir(srcDir, imsi_target, false)) {
			log.debug("zip打包成功：" + imsi_target);
			if (!bakImsiIndex.delete()) {
				log.warn("删除备份文件失败：" + bakImsiIndex);
			} else {
				log.debug("成功删除了备份文件：" + bakImsiIndex);
			}
		} else {
			log.error("zip打包失败：" + imsi_target);
			if (bakImsiIndex.exists()) {
				if (!bakImsiIndex.renameTo(imsi_target))
					log.warn("将备份的" + bakImsiIndex + "还原成" + imsi_target + "失败。");
			}
		}

		// 索引文件打包为MDN_INDEX.zip
		// currentpath要配到innerdata这一级目录。
		File mdn_target = new File(basedir.getAbsolutePath() + File.separator + "MDN_INDEX.zip");
		File bakMdnIndex = new File(mdn_target.getAbsolutePath() + ".history." + Util.getDateString_yyyyMMddHHmmss(new Date()));
		if (mdn_target.exists()) {
			// IMDN_INDEX.zip已存在，备份。
			if (mdn_target.renameTo(bakMdnIndex)) {
				log.debug("已将文件+" + mdn_target + "备份为" + bakMdnIndex);
			} else {
				log.warn("将文件+" + mdn_target + "备份为" + bakMdnIndex + "时失败。");
			}
		}
		srcDir = new File(basedir.getAbsolutePath() + File.separator + "MDN_INDEX");
		log.debug("准备zip压缩，源目录：" + srcDir + "，目标文件：" + mdn_target);
		if (ZipUtil.zipDir(srcDir, mdn_target, false)) {
			log.debug("zip打包成功：" + mdn_target);
			if (!bakMdnIndex.delete()) {
				log.warn("删除备份文件失败：" + bakMdnIndex);
			} else {
				log.debug("成功删除了备份文件：" + bakMdnIndex);
			}
		} else {
			log.error("zip打包失败：" + mdn_target);
			if (bakMdnIndex.exists()) {
				if (!bakMdnIndex.renameTo(mdn_target))
					log.warn("将备份的" + bakMdnIndex + "还原成" + mdn_target + "失败。");
			}
		}

		log.debug("开始入库终端信息");
		// store();
		log.debug("终端信息入库完毕");
		return true;
	}

	private void store() {
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
				.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), sqlldrCtl.getAbsoluteFile(),
				sqlldrBad.getAbsoluteFile(), sqlldrLog.getAbsoluteFile());
		logger.debug("执行 " + cmd);
		ExternalCmd execute = new ExternalCmd();
		try {
			int ret = execute.execute(cmd);
			SqlldrResult result = new SqlLdrLogAnalyzer().analysis(sqlldrLog.getAbsolutePath());
			logger.debug("exit=" + ret + " omcid=" + collectObjInfo.getDevInfo().getOmcID() + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
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
				sqlldrTxt.delete();
				sqlldrBad.delete();
			}
		} catch (Exception ex) {
			logger.error("sqlldr时异常", ex);
		}
	}

	private void initSqlldr() throws Exception {
		ctlWriter.println("LOAD DATA\n" + "CHARACTERSET ZHS16GBK\n" + "INFILE '" + sqlldrTxt.getAbsolutePath() + "' APPEND INTO TABLE "
				+ TAC_INFO_STORE_TABLE + "\n" + "FIELDS TERMINATED BY \"|\"\n" + "TRAILING NULLCOLS\n" + "(\n" + "  OMCID,\n"
				+ "  COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',\n" + "  STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',\n" + "  IMSI,\n"
				+ "  SUBSCRIBER_NUMBER,\n" + "  VENDOR,\n" + "  TERMMODEL\n" + ")");
		IOUtils.closeQuietly(ctlWriter);
		txtWriter.println("OMCID|COLLECTTIME|STAMPTIME|IMSI|SUBSCRIBER_NUMBER|VENDOR|TERMMODEL");
		txtWriter.flush();
	}

	private void initWriters() throws Exception {
		basedir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info" + File.separator + collectObjInfo.getKeyID()
				+ File.separator);
		if (basedir.exists())
			return;
		if (!basedir.mkdirs()) {
			throw new Exception("创建数据目录失败 - " + basedir);
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

	private static class InfoEntry {

		String meid;

		/*
		 * MDN为手机号，分成三段存储，第一段是前3位数，第二段是中间4位，第三段是最后4位。
		 */
		short mdnPart1;

		short mdnPart2;

		short mdnPart3;

		/*
		 * IMSI分三段存储，第一段是前2位数，第二段是中间4位，第三段是最后4位。
		 */
		short imsiPart1;

		short imsiPart2;

		short imsiPart3;

		short province;

		short city;

		short vendor;

		short model;

		public InfoEntry() {
			super();
		}

		public InfoEntry(String meid, short mdnPart1, short mdnPart2, short mdnPart3, short imsiPart1, short imsiPart2, short imsiPart3,
				short province, short city, short vendor, short model) {
			super();
			this.meid = meid;
			this.mdnPart1 = mdnPart1;
			this.mdnPart2 = mdnPart2;
			this.mdnPart3 = mdnPart3;
			this.imsiPart1 = imsiPart1;
			this.imsiPart2 = imsiPart2;
			this.imsiPart3 = imsiPart3;
			this.province = province;
			this.city = city;
			this.vendor = vendor;
			this.model = model;
		}

		/* 这是里交换字节顺序，写入的bin文件，是给c++读取的，c++和java字节顺序不同，所以这里做交换。 */
		static void writeSwappedShort(RandomAccessFile output, short value) throws IOException {
			output.write((byte) ((value >> 0) & 0xff));
			output.write((byte) ((value >> 8) & 0xff));
		}

		/**
		 * 这里是将手机型号写入bin文件，每个手机型号占2个字段。 写入的是手机型号的编号，c++程序拿到此编号，在数据库表里查。
		 */
		public boolean writeToFile(RandomAccessFile out) {
			try {
				writeSwappedShort(out, model);
			} catch (Exception e) {
				logger.error("写入二进制数据时异常：" + this, e);
			} finally {
			}
			return true;
		}

		@Override
		public String toString() {
			return "InfoEntry [city=" + city + ", imsiPart1=" + imsiPart1 + ", imsiPart2=" + imsiPart2 + ", imsiPart3=" + imsiPart3 + ", mdnPart1="
					+ mdnPart1 + ", mdnPart2=" + mdnPart2 + ", mdnPart3=" + mdnPart3 + ", meid=" + meid + ", model=" + model + ", province="
					+ province + ", vendor=" + vendor + "]";
		}

	}

	private static class CityEntry {

		String cityName;

		short cityId;

		short provinceId;

		String provinceName;

		String provEn;

		int hqId;

		@Override
		public String toString() {
			return "CityEntry [cityId=" + cityId + ", cityName=" + cityName + ", hqId=" + hqId + ", provEn=" + provEn + ", provinceId=" + provinceId
					+ ", provinceName=" + provinceName + "]";
		}

	}

	private static class VendorEntry {

		short vendorId;

		String vendorName;

		public VendorEntry() {
			super();
		}

		public VendorEntry(short vendorId, String vendorName) {
			super();
			this.vendorId = vendorId;
			this.vendorName = vendorName;
		}

		@Override
		public String toString() {
			return "VendorEntry [vendorId=" + vendorId + ", vendorName=" + vendorName + "]";
		}
	}

	private static class ModelEntry {

		short modelId;

		String modelName;

		String vendorName;

		public ModelEntry() {
			super();
		}

		public ModelEntry(short modelId, String modelName, String vendorName) {
			super();
			this.modelId = modelId;
			this.modelName = modelName;
			this.vendorName = vendorName;
		}

		@Override
		public String toString() {
			return "ModelEntry [modelName=" + modelName + ", vendorName=" + vendorName + "]";
		}

	}

	public static void main(String[] args) throws Exception {
		JilinUeParser p = new JilinUeParser();
		p.fileName = "E:\\uway\\requiredment\\igp1\\吉林\\D0175--终端\\终端自注册用户明细数据\\终端自注册用户明细数据.csv";
		CollectObjInfo task = new CollectObjInfo(20140724);
		task.setLastCollectTime(new Timestamp(Util.getDate1("2012-09-30 00:00:00").getTime()));
		DevInfo di = new DevInfo();
		task.setDevInfo(di);
		p.setCollectObjInfo(task);
		p.parseData();
	}
}
