package parser.c.ue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import util.ZipUtil;
import framework.SystemConfig;

/**
 * 甘肃CSV终端采集。
 * 
 * @author yuy 2014-12-10
 */
public class GanSuCSVUeParser extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String FILED_SEP = ",";

	private static final String TABLE_NAME_MODEL = "CLT_TERMINAL_TACINFO";

	private static final String SQL_MODEL_TABLE = "select tactypeid,tac_vendor,mobile_model from " + TABLE_NAME_MODEL;

	private static int maxTactypeID = 0;

	// private static List<ModelEntry> disOrderEMList = new ArrayList<ModelEntry>();

	private static final Map<String, ModelEntry> MODELS = new HashMap<String, ModelEntry>();

	private Map<String/* 文件绝对路径 */, RandomAccessFile> writers = new HashMap<String, RandomAccessFile>();

	private File basedir;

	private boolean imsiIndex = true;

	static {
		getMaxTactypeID();
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

	private static void getMaxTactypeID() {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = "select nvl(max(tactypeid),0) from " + TABLE_NAME_MODEL;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				maxTactypeID = rs.getInt(1);
			}
		} catch (Exception e) {
		} finally {
			CommonDB.close(rs, ps, con);
		}
	}

	// 添加新型号。
	private static boolean addModel(ModelEntry me) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "insert into " + TABLE_NAME_MODEL + " (tactypeid,tac_vendor,mobile_model) values (?,?,?)";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setInt(1, ++maxTactypeID);
			ps.setString(2, me.vendorName);
			ps.setString(3, me.modelName);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("添加新手机型号时出现异常 " + me, e);
			return false;
		} finally {
			CommonDB.close(null, ps, con);
		}
		// loadModels();
		MODELS.put(me.modelName, me);
		logger.debug("成功添加了一条新型号- " + me);
		return true;
	}

	// 添加新型号。
	private static boolean updateModel(ModelEntry me) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "update " + TABLE_NAME_MODEL + " set TAC_VENDOR = ? where MOBILE_MODEL = ? ";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setString(1, me.vendorName);
			ps.setString(2, me.modelName);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("修改新手机型号时出现异常 " + me, e);
			return false;
		} finally {
			CommonDB.close(null, ps, con);
		}
		logger.debug("成功修改了一条新型号- " + me);
		return true;
	}

	@Override
	public boolean parseData() throws Exception {
		if (this.collectObjInfo instanceof RegatherObjInfo) {
			log.warn("此任务不允许补采任务：" + this.collectObjInfo.getTaskID());
			return true;
		}
		InputStream in = null;
		BufferedReader reader = null;
		LineIterator lines = null;
		try {
			initWriters();
			in = new FileInputStream(fileName);
			String line = null;
			String sp[] = null;
			int count = 0;
			String remaining = null;
			reader = new BufferedReader(new InputStreamReader(in, "gbk"));
			reader.readLine();
			while ((line = reader.readLine()) != null) {
				count++;
				try {
					sp = Util.split(line, ",");
					// 原始文件没有表头，有9个字段，分别是：忽略字段\t手机号\tIMSI\t省\t市\t厂家名\t型号名\t软件版本\t注册时间。
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
					String imsi = trim(sp[3]);
					if (imsi.length() != 15) {
						logger.warn("imsi error:" + imsi + ",line:" + count);
						continue;
					}
					InfoEntry ie = new InfoEntry();
					ie.imsiPart1 = Short.parseShort(imsi.substring(5, 7));
					ie.imsiPart2 = Short.parseShort(imsi.substring(7, 11));
					ie.imsiPart3 = Short.parseShort(imsi.substring(11, 15));

					String vendor = trim(sp[5]);
					if (Util.isNull(vendor))
						continue;
					String model = trim(sp[6]);
					if (Util.isNull(model))
						continue;
					if (MODELS.containsKey(model)) {
						ModelEntry me = MODELS.get(model);
						ie.model = me.modelId;
						// 判断是否乱码
						// if (!isChineseChar(me.vendorName) && !isEnglishChar(me.vendorName)) {
						// me.vendorName = vendor;
						// disOrderEMList.add(me);
						// }
					} else {
						if (addModel(new ModelEntry((short) 0, model, vendor))) {
							ie.model = MODELS.get(model).modelId;
						} else {
							continue;
						}
					}

					// 以imsi作为索引。
					// 比如要查找IMSI为460030904590226的用户的手机型号，则先找到“设置的存放根目录/IMSI_INDEX/09/0459.bin”文件，
					// 再seek到226*2个字节的位置，再读取2个字节，即是手机型号的编号，拿到此编号后，在数据库表里查找具体型号内容。
					// 即imsi前面的46003去掉，省下0904590226，其中前两位09是目录，中间四位0459是文件名，最后4位0226是要seek的长度依据，以此方法较快的索引手机型号。
					if (imsiIndex) {
						File dir = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX" + File.separator + wrap2(ie.imsiPart1));
						if (!dir.exists())
							dir.mkdirs();
						File bin = new File(dir.getAbsolutePath(), wrap4(ie.imsiPart2) + ".bin");
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
			LineIterator.closeQuietly(lines);
			IOUtils.closeQuietly(in);
		}

		// 索引文件打包为IMSI_INDEX.zip
		// currentpath要配到innerdata这一级目录。
		File imsi_target = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "IMSI_INDEX.zip");
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

		// 修正乱码
		// for (ModelEntry me : disOrderEMList) {
		// updateModel(me);
		// }

		return true;
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

	public static boolean isChineseChar(String str) {
		boolean temp = false;
		Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
		Matcher m = p.matcher(str);
		if (m.find()) {
			temp = true;
		}
		return temp;
	}

	public static boolean isEnglishChar(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (!(s.charAt(i) >= 'A' && s.charAt(i) <= 'Z') && !(s.charAt(i) >= 'a' && s.charAt(i) <= 'z')) {
				return false;
			}
		}
		return true;
	}

	public static String trim(String s) {
		return s.replace("\"", "").replace(" ", "");
	}

	public static void main(String[] args) throws Exception {
		GanSuCSVUeParser p = new GanSuCSVUeParser();
		p.fileName = "E:\\uway\\requiredment\\igp1\\甘肃\\终端\\clt_taiz_t_sa_terminal.csv";
		CollectObjInfo task = new CollectObjInfo(1);
		task.setLastCollectTime(new Timestamp(Util.getDate1("2012-09-30 00:00:00").getTime()));
		DevInfo di = new DevInfo();
		task.setDevInfo(di);
		p.setCollectObjInfo(task);
		p.parseData();
	}
}
