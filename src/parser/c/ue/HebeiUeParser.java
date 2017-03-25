package parser.c.ue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

import parser.Parser;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import util.ZipUtil;
import util.opencsv.CSVParser;
import framework.SystemConfig;

/**
 * 河北终端。
 * 
 * @author ChensSijiang 2013-5-13
 */
public class HebeiUeParser extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String FILED_SEP = ",";

	private static final String TABLE_NAME_MODEL = "CLT_TERMINAL_TACINFO";

	private static final String SQL_MODEL_TABLE = "select tactypeid,tac_vendor,mobile_model from " + TABLE_NAME_MODEL;

	/* 将原始终端信息入库的表。 */
	private static final String TAC_INFO_STORE_TABLE = "CLT_TAIZ_T_SA_TERMINAL";

	private static final Map<String, ModelEntry> MODELS = new HashMap<String, ModelEntry>();

	private Map<String/* 文件绝对路径 */, RandomAccessFile> writers = new HashMap<String, RandomAccessFile>();
	
	private File basedir;

	private boolean imsiIndex = true;

	private boolean mdnIndex = true;

	public String imsifile = null;
	
	private Connection conn = null;
	private PreparedStatement statement = null;
	private int currNum = 0;

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
		Map<String,String> imsimap = new HashMap<String, String>();
		FileInputStream fin = new FileInputStream(imsifile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
		String line0 = null;
		while((line0 = reader.readLine()) != null){
			int begin = line0.indexOf(",");
			int last = line0.lastIndexOf(",");
			imsimap.put(line0.substring(begin + 2, last - 1), line0.substring(last + 2, line0.length() - 1));
		}
		reader.close();
		fin.close();

		InputStream in = null;
		LineIterator lines = null;
		try {
			initWriters();
			in = new FileInputStream(fileName);
			lines = IOUtils.lineIterator(in, null);
			String line = null;
			String sp[] = null;
			int count = 0;
			String remaining = null;
			CSVParser csv = new CSVParser(',', '\"');
			while (lines.hasNext()) {
				count++;
				line = lines.nextLine();
				try {
					sp = csv.parseLine(line);
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
					InfoEntry ie = new InfoEntry();
					
					//城市
					String cityName = sp[0].trim();
					//手机号
					String mdn = sp[1].trim();
					//imsi
					String imsi = imsimap.get(mdn);
					//vendor 品牌
					String vendor = sp[2].trim();
					//model 手机型号
					String model = sp[3].trim();
					//meid
					String meid = sp[4].trim();
					//time
					String time = sp[5].trim();
					if(time.indexOf(".") > -1)
						time = time.substring(0,time.indexOf("."));
					
					Map<String, String> record = new LinkedHashMap<String,String>();
					record.put("OMCID", "0");
					record.put("COLLECTTIME", "2013-12-13 18:00:00");
					record.put("STAMPTIME", "2013-12-13 18:00:00");
					record.put("IMSI", imsi);
					record.put("SUBSCRIBER_NUMBER", mdn);
					record.put("VENDOR", vendor);
					record.put("TERMMODEL", model);
					record.put("CITY_NAME", cityName);
					record.put("MEID", meid);
					record.put("PROVINCE_NAME", "河北省");
					record.put("SMS_SEND_TIME", time);
					export(record);
					
					if (Util.isNull(imsi))
						continue;
					if (Util.isNull(vendor))
						continue;
					if (Util.isNull(model))
						continue;
					if (imsi.length() != 15) {
						logger.warn("imsi error:" + imsi + ",line:" + count);
						continue;
					}
					
					if (mdn.length() != 11) {
						logger.warn("mdn error:" + mdn + ",line:" + count);
						continue;
					}
					if (MODELS.containsKey(model)) {
						ie.model = MODELS.get(model).modelId;
					} else {
						if (addModel(new ModelEntry((short) 0, model, vendor))) {
							ie.model = MODELS.get(model).modelId;
						} else {
							continue;
						}
					}
					
					ie.mdnPart1 = Short.parseShort(mdn.substring(0, 3));
					ie.mdnPart2 = Short.parseShort(mdn.substring(3, 7));
					ie.mdnPart3 = Short.parseShort(mdn.substring(7, 11));
					imsi = imsi.substring(5);
					ie.imsiPart1 = Short.parseShort(imsi.substring(0, 2));
					ie.imsiPart2 = Short.parseShort(imsi.substring(2, 6));
					ie.imsiPart3 = Short.parseShort(imsi.substring(6, 10));

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
			LineIterator.closeQuietly(lines);
			IOUtils.closeQuietly(in);
		}

		// 索引文件打包为IMSI_INDEX.zip
		// currentpath要配到innerdata这一级目录。
		File imsi_target = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "termid" + File.separator + "IMSI_INDEX.zip");
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
		File mdn_target = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "termid" + File.separator + "MDN_INDEX.zip");
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
		endExport();
		log.debug("终端信息入库完毕");
		return true;
	}
	
	/**
	 * 初始化输出SQL
	 * 
	 * @return
	 */
	private String createInsertSql() {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ").append(TAC_INFO_STORE_TABLE).append(" (");
		sb.append("OMCID,COLLECTTIME,STAMPTIME,IMSI,SUBSCRIBER_NUMBER,VENDOR,TERMMODEL,CITY_NAME,MEID,PROVINCE_NAME,SMS_SEND_TIME) values (");
		sb.append("?,?,?,?,?,?,?,?,?,?,?)");
		return sb.toString();
	}
	
	public void export(Map<String, String> record) throws Exception {
		StringBuilder values = new StringBuilder();
		try {
			if(this.conn == null){
				this.conn = DbPool.getConn();
				this.statement = this.conn.prepareStatement(createInsertSql());
			}
			
			Iterator<String> it = record.keySet().iterator();
			int i = 1;
			while(it.hasNext()){
				String name = it.next();
				values.append(record.get(name) + ",");
				if(name.equals("SMS_SEND_TIME") || name.equals("COLLECTTIME") || name.equals("STAMPTIME")){// 处理Timestamp
					statement.setTimestamp(i, new Timestamp(getDate(record.get(name)).getTime()));
					i++;
					continue;
				}
				statement.setString(i, record.get(name));
				i++;
			}
			statement.addBatch();
			this.currNum++;
			if (this.currNum >= 1000) {
				this.currNum = 0;
				try{
					statement.executeBatch();
				}finally{
					statement.close();
					statement = null;
					statement = conn.prepareStatement(createInsertSql());
				}
			}
		} catch (Exception e) {
			log.debug(createInsertSql());
			log.debug(values);
			throw new Exception("写入数据库失败：" + TAC_INFO_STORE_TABLE + createInsertSql() , e);
		}
	}
	
	public void endExport()throws Exception {
		try{
			if(statement != null)
				statement.executeBatch();
		}finally{
			statement.close();
			conn.close();
			statement = null;
			conn = null;
		}
		
	}
	
	private void initWriters() throws Exception {
		basedir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + 
				"cdma_ue_info" + File.separator);
		if (basedir.exists())
			return;
		if (!basedir.mkdirs()) {
			throw new Exception("创建数据目录失败 - " + basedir);
		}
	}

	/** 把 yyyy-MM-dd HH:mm:ss形式的字符串 转换成 时间 */
	public static Date getDate(String str) throws ParseException {
		String pattern = "yyyy-MM-dd HH:mm:ss";
	
		return getDate(str, pattern);
	}
	
	/** 把 字符串 转换成 时间 */
	public static Date getDate(String str, String pattern) throws ParseException {
		SimpleDateFormat f = new SimpleDateFormat(pattern);
		Date d = f.parse(str);

		return d;
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
	
	//分拆
	public void cutBigFile() throws Exception{
		File file = new File("/home/yuy/my/requirement/c/D0076/20131203.csv");
		FileInputStream in = new FileInputStream(file);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String line = null;
		Map<String, BufferedWriter> clist = new HashMap<String, BufferedWriter>();
		while((line = reader.readLine()) != null){
			String cityname = line.substring(0, line.indexOf(","));
			if(clist.containsKey(cityname)){
				clist.get(cityname).write(line);
				clist.get(cityname).newLine();
			}else{
				File f = new File(file.getParentFile() + File.separator + "hebei_ter");
				if(!f.exists())
					f.mkdir();
				File f1 = new File(f.getPath() + File.separator + cityname + "IMSI.csv");
				FileOutputStream out = new FileOutputStream(f1, true);
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
				clist.put(cityname, writer);
				writer.write(line);
				writer.newLine();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String[] city = {"保定","唐山","廊坊","张家口","承德","沧州","石家庄","秦皇岛","衡水","邢台","邯郸"};
		HebeiUeParser p = new HebeiUeParser();
		for(String cityname : city){
			p.imsifile = "/home/yuy/my/requirement/c/D0076/imsi/"+cityname+"IMSI.csv";
			p.fileName = "/home/yuy/my/requirement/c/D0076/hebei_ter/"+cityname+"IMSI.csv";
			p.parseData();
		}
	}
}
