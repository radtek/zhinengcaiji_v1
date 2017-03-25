package parser.c.ue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.servlet.jsp.jstl.sql.Result;

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
import util.file.FileUtil;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * CDMA终端级优化，终端信息解析。 字段： MEID MDN IMSI 省公司 城市 终端厂商 手机型号 软件版本号 注册时间
 * 
 * @author ChenSijiang
 */
public class UEInfoParser extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final String FILED_SEP = "\\|";

	private static final String SQL_CITY_TABLE = "select city_name,nvl(city_id,0) as city_id,province_id,"
			+ "province_name,prov_enname,hq_id from app_cfg_city";

	private static final Map<String, CityEntry> CITYS = new HashMap<String, CityEntry>();

	private static final String TABLE_NAME_MODEL = "app_cfg_tacinfo";

	private static final String SQL_MODEL_TABLE = "select tactypeid,tac_vendor,mobile_model,tacvendorid from " + TABLE_NAME_MODEL;

	private static final String TABLE_NAME_VENDOR = "app_cfg_tacvendorinfo";

	private static final String SQL_VENDOR_TABLE = "select tacvendorid,tacvendorname from " + TABLE_NAME_VENDOR;

	/* 将原始终端信息入库的表。 */
	private static final String TAC_INFO_STORE_TABLE = "ds_ne_tac_info";

	/**
	 * 加载终端信息sql
	 */
	// private static final String SQL_UE_TABLE = "select t1.mdn,t1.imsi,t1.city_id,t2.tactypeid " + "from " + TAC_INFO_STORE_TABLE + " t1,"
	// + TABLE_NAME_MODEL + " t2 where t1.mobile_model = t2.mobile_model order by t1.start_time asc";

	private static final String SQL_UE_TABLE = "select mdn,imsi,city_id,mobile_model from " + TAC_INFO_STORE_TABLE;

	/**
	 * 加载终端信息sql --去重复
	 */
	// private static final String SQL_UE_TABLE = "select t1.mdn,t1.imsi,t1.city_id,t2.tactypeid from "
	// + "(select t.mdn,max(t.start_time)as start_time from " + TAC_INFO_STORE_TABLE + " t group by t.mdn) d," + TAC_INFO_STORE_TABLE + " t1,"
	// + TABLE_NAME_MODEL + " t2" + " where d.mdn = t1.mdn and d.start_time = t1.start_time and t1.mobile_model = t2.mobile_model";

	private static final Map<String, VendorEntry> VENDORS = new HashMap<String, VendorEntry>();

	private static final Map<String, ModelEntry> MODELS = new HashMap<String, ModelEntry>();

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

	static {
		loadCity();
		loadModels();
		loadVendors();
	}

	/**
	 * 加载终端数据，生成终端文件
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private void loadUEInfoToFile() throws Exception {
		logger.info("[" + collectObjInfo.getKeyID() + "]" + "开始从数据库中载入终端信息。");
		int count = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = CommonDB.getConnection();
			ps = conn.prepareStatement(SQL_UE_TABLE);
			rs = ps.executeQuery();
			InfoEntry ie = null;
			while (rs.next()) {
				ModelEntry me = MODELS.get(rs.getString("mobile_model"));
				if (me == null)
					continue;
				ie = new InfoEntry();
				ie.city = rs.getShort("city_id");
				ie.model = me.modelId;
				String mdn = null;
				String imsi = null;
				// 过滤掉非法imsi/mdn号
				try {
					mdn = rs.getString("mdn").trim();
					imsi = rs.getString("imsi").trim();
					getMdnPart(mdn, ie);
					getImsiPart(imsi, ie);
				} catch (Exception e) {
					log.error("[" + collectObjInfo.getKeyID() + "]" + "存在非法imsi或mdn号,imsi:" + imsi + ",mdn:" + mdn, e);
					continue;
				}
				// 写终端信息
				write(ie);
				count++;
				if (count % 100000 == 0 && count > 0)
					logger.info("[" + collectObjInfo.getKeyID() + "]" + "已经从数据库中载入" + count + "条信息。");
			}
		} catch (Exception e) {
			logger.error("[" + collectObjInfo.getKeyID() + "]" + "载入终端信息时出现异常。", e);
		} finally {
			CommonDB.closeDBConnection(conn, ps, rs);
			for (RandomAccessFile r : writers.values()) {
				if (r != null)
					r.close();
			}
			writers.clear();
			logger.info("[" + collectObjInfo.getKeyID() + "]" + "从数据库中载入终端信息完成，共载入" + count + "条信息。");
		}
	}

	@SuppressWarnings("rawtypes")
	private static void loadCity() {
		CITYS.clear();
		Result result = null;
		try {
			result = CommonDB.queryForResult(SQL_CITY_TABLE);
		} catch (Exception e) {
			logger.error("载入城市信息时出现异常。", e);
		}
		SortedMap[] sm = result.getRows();
		for (SortedMap s : sm) {
			try {
				CityEntry ce = new CityEntry();
				ce.cityName = s.get("city_name").toString();
				ce.cityId = Short.parseShort(s.get("city_id").toString());
				ce.provinceId = Short.parseShort(s.get("province_id").toString());
				ce.provEn = s.get("prov_enname").toString();
				ce.provinceName = s.get("province_name").toString();
				ce.hqId = Integer.parseInt(s.get("hq_id").toString());
				CITYS.put(ce.cityName, ce);
			} catch (Exception e) {
				logger.error("载入城市信息时出现了一个异常。", e);
			}
		}
	}

	@SuppressWarnings("rawtypes")
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
			me.vendorId = Short.parseShort(s.get("tacvendorid").toString());
			me.vendorName = s.get("tac_vendor").toString();
			MODELS.put(me.modelName, me);
		}

	}

	@SuppressWarnings("rawtypes")
	private static void loadVendors() {
		VENDORS.clear();
		Result result = null;
		try {
			result = CommonDB.queryForResult(SQL_VENDOR_TABLE);
		} catch (Exception e) {
			logger.error("载入手机厂商信息时出现异常。", e);
		}
		SortedMap[] sm = result.getRows();
		for (SortedMap s : sm) {
			VendorEntry ve = new VendorEntry();
			ve.vendorId = Short.parseShort(s.get("tacvendorid").toString());
			ve.vendorName = s.get("tacvendorname").toString();
			VENDORS.put(ve.vendorName, ve);
		}
	}

	private static boolean addCity(CityEntry ce) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "insert into app_cfg_city (hq_id,city_name,province_name,province_id,prov_enname) values"
				+ "((select max(hq_id)+1 from app_cfg_city),?,?,?,?)";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setString(1, ce.cityName);
			ps.setString(2, ce.provinceName);
			ps.setShort(3, ce.provinceId);
			ps.setString(4, ce.provEn);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("添加城市信息时出现异常 " + ce, e);
			return false;
		} finally {
			CommonDB.closeDBConnection(con, ps, null);
		}
		loadCity();
		logger.debug("成功添加了一条新城市 - " + ce);
		return true;
	}

	// 添加新厂家
	private static boolean addVendor(VendorEntry ve) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "insert into " + TABLE_NAME_VENDOR + " (tacvendorid,tacvendorname) values (" + "(select nvl(max(tacvendorid),0)+1 from "
				+ TABLE_NAME_VENDOR + "),?)";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setString(1, ve.vendorName);
			ps.executeUpdate();
		} catch (Exception e) {
			logger.error("添加厂家信息时出现异常 " + ve, e);
			return false;
		} finally {
			CommonDB.closeDBConnection(con, ps, null);
		}
		loadVendors();
		logger.debug("成功添加了一条新厂家 - " + ve);
		return true;
	}

	// 添加新型号。
	private static boolean addModel(ModelEntry me) {
		Connection con = null;
		PreparedStatement ps = null;
		String sql = "insert into " + TABLE_NAME_MODEL + " (tactypeid,tac_vendor,mobile_model,tacvendorid) values ("
				+ "(select nvl(max(tactypeid),0)+1 from " + TABLE_NAME_MODEL + "),?,?,?)";
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ps.setString(1, me.vendorName);
			ps.setString(2, me.modelName);
			ps.setShort(3, me.vendorId);
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
		// 第一次采集，先加载数据库的数据生成终端文件（终端文件的数据格式发生变化，必须要重新生成)
		if (initWriters()) {
			loadUEInfoToFile();
		} else {
			List<String> fileList = FileUtil.getFileNames(basedir.getAbsolutePath(), "*.bin");
			if (fileList == null || fileList.size() == 0) {
				loadUEInfoToFile();
			} else {
				logger.info("[" + collectObjInfo.getKeyID() + "]" + "目前已存在多少个bin文件：" + fileList.size());
			}
		}

		File sqlldrDir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator);
		sqlldrDir.mkdirs();
		String basesqlldrname = TAC_INFO_STORE_TABLE + "_" + System.currentTimeMillis();
		sqlldrTxt = new File(sqlldrDir, basesqlldrname + ".txt");
		sqlldrLog = new File(sqlldrDir, basesqlldrname + ".log");
		sqlldrBad = new File(sqlldrDir, basesqlldrname + ".bad");
		sqlldrCtl = new File(sqlldrDir, basesqlldrname + ".ctl");

		InputStream in = null;
		LineIterator lines = null;
		BufferedReader reader = null;
		try {
			txtWriter = new PrintWriter(sqlldrTxt);
			ctlWriter = new PrintWriter(sqlldrCtl);
			initSqlldr();
			initWriters();
			in = new FileInputStream(fileName);
			reader = new BufferedReader(new InputStreamReader(in, "gbk"));
			// lines = IOUtils.lineIterator(in, null);
			String line = null;
			String sp[] = null;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				try {
					sp = line.split(FILED_SEP);
					// 原始文件没有表头，有9个字段，分别是：meid、mdn、imsi、省名、市名、手机品牌、型号、手机软件版本、注册时间。
					if (sp.length < 9) {
						continue;
					}
					count++;
					String mdn = sp[1];
					if (mdn.length() != 11) {
						logger.warn("mdn error:" + mdn + ",line:" + count);
						continue;
					}
					String imsi = sp[2];
					if (imsi.length() != 15) {
						logger.warn("imsi error:" + imsi + ",line:" + count);
						continue;
					}
					InfoEntry ie = new InfoEntry();
					String city = sp[4];

					if (CITYS.containsKey(city)) {
						ie.city = CITYS.get(city).cityId;
						ie.province = CITYS.get(city).provinceId;
					} else {
						String prov = sp[3];
						if (prov.equals("内蒙古")) {
							prov = "内蒙";
						}
						CityEntry ce = findProv(prov);
						if (ce != null) {
							ce.cityName = city;
							if (addCity(ce)) {
								ie.city = CITYS.get(city).cityId;
								ie.province = CITYS.get(city).provinceId;
							} else {
								ie.city = 0;
								ie.province = 0;
							}
						}
					}
					txtWriter.println(sp[0] + "|" + sp[1] + "|" + sp[2] + "|" + sp[3] + "|" + (ie.province == 0 ? "" : ie.province) + "|" + sp[4]
							+ "|" + (ie.city == 0 ? "" : ie.city) + "|" + sp[5] + "|" + sp[6] + "|" + sp[7] + "|" + sp[8]);
					txtWriter.flush();

					getMdnPart(mdn, ie);

					getImsiPart(imsi, ie);

					String vendor = sp[5];
					if (VENDORS.containsKey(vendor)) {
						ie.vendor = VENDORS.get(vendor).vendorId;
					} else {
						if (addVendor(new VendorEntry((short) 0, vendor))) {
							ie.vendor = VENDORS.get(vendor).vendorId;
						} else {
							continue;
						}
					}
					String model = sp[6];
					if (MODELS.containsKey(model)) {
						ie.model = MODELS.get(model).modelId;
					} else {
						VendorEntry ve = VENDORS.get(vendor);
						if (addModel(new ModelEntry((short) 0, model, ve.vendorName, ve.vendorId))) {
							ie.model = MODELS.get(model).modelId;
						} else {
							continue;
						}
					}

					// 写终端信息
					write(ie);

				} catch (NumberFormatException e) {
					logger.warn("[" + collectObjInfo.getKeyID() + "]" + "MDN或IMSI内容异常（包含非数字内容）。数据：" + line + "，行号：" + count);
				} catch (Exception e) {
					logger.warn("[" + collectObjInfo.getKeyID() + "]" + "处理一行终端信息时出现意外。数据：" + line + "，行号：" + count, e);
				}
			}
			count = 0;
		} catch (Exception e) {
			logger.error("[" + collectObjInfo.getKeyID() + "]" + "终端信息解析中出现异常。", e);
			return false;
		} finally {
			for (RandomAccessFile r : writers.values()) {
				if (r != null)
					r.close();
			}
			writers.clear();
			LineIterator.closeQuietly(lines);
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(ctlWriter);
			IOUtils.closeQuietly(txtWriter);
		}

		// 打包
		if (collectObjInfo.getLastCollectTime().getTime() > Util.getDate("20141209", "yyyyMMdd").getTime())
			pack();

		log.debug("[" + collectObjInfo.getKeyID() + "]" + "开始入库终端信息");
		if (collectObjInfo.getLastCollectTime().getTime() > Util.getDate("20141207", "yyyyMMdd").getTime())
			store();
		log.debug("[" + collectObjInfo.getKeyID() + "]" + "终端信息入库完毕");
		return true;
	}

	/**
	 * 打包，将IMSI_INDEN目录下的子目录打包成IMSI_INDEX.zip，MDN_INDEX目录下的子目录打包成MDN_INDEX.zip
	 */
	public void pack() {
		// 索引文件打包为IMSI_INDEX.zip
		// currentpath要配到innerdata这一级目录。
		File imsi_target = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX.zip");
		File bakImsiIndex = new File(imsi_target.getAbsolutePath() + ".history." + Util.getDateString_yyyyMMddHHmmss(new Date()));
		if (imsi_target.exists()) {
			// IMSI_INDEX.zip已存在，备份。
			if (imsi_target.renameTo(bakImsiIndex)) {
				log.debug("[" + collectObjInfo.getKeyID() + "]" + "已将文件+" + imsi_target + "备份为" + bakImsiIndex);
			} else {
				log.warn("[" + collectObjInfo.getKeyID() + "]" + "将文件+" + imsi_target + "备份为" + bakImsiIndex + "时失败。");
			}
		}
		File srcDir = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX");
		log.debug("[" + collectObjInfo.getKeyID() + "]" + "准备zip压缩，源目录：" + srcDir + "，目标文件：" + imsi_target);
		if (ZipUtil.zipDir(srcDir, imsi_target, false)) {
			log.debug("[" + collectObjInfo.getKeyID() + "]" + "zip打包成功：" + imsi_target);
			if (!bakImsiIndex.delete()) {
				log.warn("[" + collectObjInfo.getKeyID() + "]" + "删除备份文件失败：" + bakImsiIndex);
			} else {
				log.debug("[" + collectObjInfo.getKeyID() + "]" + "成功删除了备份文件：" + bakImsiIndex);
			}
		} else {
			log.error("[" + collectObjInfo.getKeyID() + "]" + "zip打包失败：" + imsi_target);
			if (bakImsiIndex.exists()) {
				if (!bakImsiIndex.renameTo(imsi_target))
					log.warn("[" + collectObjInfo.getKeyID() + "]" + "将备份的" + bakImsiIndex + "还原成" + imsi_target + "失败。");
			}
		}

		// 索引文件打包为MDN_INDEX.zip
		// currentpath要配到innerdata这一级目录。
		File mdn_target = new File(basedir.getAbsolutePath() + File.separator + "MDN_INDEX.zip");
		File bakMdnIndex = new File(mdn_target.getAbsolutePath() + ".history." + Util.getDateString_yyyyMMddHHmmss(new Date()));
		if (mdn_target.exists()) {
			// IMDN_INDEX.zip已存在，备份。
			if (mdn_target.renameTo(bakMdnIndex)) {
				log.debug("[" + collectObjInfo.getKeyID() + "]" + "已将文件+" + mdn_target + "备份为" + bakMdnIndex);
			} else {
				log.warn("[" + collectObjInfo.getKeyID() + "]" + "将文件+" + mdn_target + "备份为" + bakMdnIndex + "时失败。");
			}
		}
		srcDir = new File(basedir.getAbsolutePath() + File.separator + "MDN_INDEX");
		log.debug("[" + collectObjInfo.getKeyID() + "]" + "准备zip压缩，源目录：" + srcDir + "，目标文件：" + mdn_target);
		if (ZipUtil.zipDir(srcDir, mdn_target, false)) {
			log.debug("[" + collectObjInfo.getKeyID() + "]" + "zip打包成功：" + mdn_target);
			if (!bakMdnIndex.delete()) {
				log.warn("[" + collectObjInfo.getKeyID() + "]" + "删除备份文件失败：" + bakMdnIndex);
			} else {
				log.debug("[" + collectObjInfo.getKeyID() + "]" + "成功删除了备份文件：" + bakMdnIndex);
			}
		} else {
			log.error("[" + collectObjInfo.getKeyID() + "]" + "zip打包失败：" + mdn_target);
			if (bakMdnIndex.exists()) {
				if (!bakMdnIndex.renameTo(mdn_target))
					log.warn("[" + collectObjInfo.getKeyID() + "]" + "将备份的" + bakMdnIndex + "还原成" + mdn_target + "失败。");
			}
		}
	}

	/**
	 * 写入终端文件
	 * 
	 * @param ie
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void write(InfoEntry ie) throws FileNotFoundException, IOException {
		// 以imsi作为索引。
		// 比如要查找IMSI为460030904590226的用户的手机型号，则先找到“设置的存放根目录/IMSI_INDEX/09/0459.bin”文件，
		// 再seek到226*2个字节的位置，再读取2个字节，即是手机型号的编号，拿到此编号后，在数据库表里查找具体型号内容。
		// 即imsi前面的46003去掉，省下0904590226，其中前两位09是目录，中间四位0459是文件名，最后4位0226是要seek的长度依据，以此方法较快的索引手机型号。
		if (imsiIndex) {
			File bin = new File(basedir.getAbsolutePath() + File.separator + "IMSI_INDEX" + File.separator + wrap2(ie.imsiPart1) + File.separator
					+ wrap4(ie.imsiPart2) + ".bin");
			write(ie, bin, true);
		}

		// 以mdn作为索引。
		// 比如要查找MDN为15389803333的用户的手机型号，则先找到“设置的存放根目录/MDN_INDEX/153/8980.bin”文件，
		// 再seek到333*2个字节的位置，再读取2个字节，即是手机型号的编号，拿到此编号后，在数据库表里查找具体型号内容。
		// 即mdn的前三位153是目录，中间四位8980是文件名，最后四位3333是要seek的长度依据，以此方法较快的索引手机型号。
		if (mdnIndex) {
			File bin = new File(basedir.getAbsolutePath() + File.separator + "MDN_INDEX" + File.separator + wrap3(ie.mdnPart1) + File.separator
					+ wrap4(ie.mdnPart2) + ".bin");
			write(ie, bin, false);
		}
	}

	public void getImsiPart(String imsi, InfoEntry ie) throws Exception {
		ie.imsiPart1 = Short.parseShort(imsi.substring(5, 7));
		ie.imsiPart2 = Short.parseShort(imsi.substring(7, 11));
		ie.imsiPart3 = Short.parseShort(imsi.substring(11, 15));
	}

	public void getMdnPart(String mdn, InfoEntry ie) throws Exception {
		ie.mdnPart1 = Short.parseShort(mdn.substring(0, 3));
		ie.mdnPart2 = Short.parseShort(mdn.substring(3, 7));
		ie.mdnPart3 = Short.parseShort(mdn.substring(7, 11));
	}

	/**
	 * 写终端文件
	 * 
	 * @param ie
	 * @param bin
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void write(InfoEntry ie, File bin, boolean isImsi) throws FileNotFoundException, IOException {
		RandomAccessFile raf = null;
		if (writers.containsKey(bin.getAbsolutePath())) {
			raf = writers.get(bin.getAbsolutePath());
		} else {
			bin.getParentFile().mkdirs();
			raf = new RandomAccessFile(bin, "rw");
			writers.put(bin.getAbsolutePath(), raf);
		}
		/* 每个数据单元占2字节,IMSI:cityid+terid共占4个字节，所以每次seek要乘以4; MDN:terid占2个字节 */
		raf.seek(isImsi ? ie.imsiPart3 * 4 : ie.mdnPart3 * 2);
		ie.writeToFile(raf, isImsi);
	}

	private void store() {
		String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
				.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), sqlldrCtl.getAbsoluteFile(),
				sqlldrBad.getAbsoluteFile(), sqlldrLog.getAbsoluteFile());
		logger.debug("[" + collectObjInfo.getKeyID() + "]" + "执行 " + cmd);
		ExternalCmd execute = new ExternalCmd();
		try {
			int ret = execute.execute(cmd);
			SqlldrResult result = new SqlLdrLogAnalyzer().analysis(sqlldrLog.getAbsolutePath());
			logger.debug("[" + collectObjInfo.getKeyID() + "]" + "exit=" + ret + " omcid=" + collectObjInfo.getDevInfo().getOmcID() + " 入库成功条数="
					+ result.getLoadSuccCount() + " 表名=" + result.getTableName() + " 数据时间=" + Util.getDateString(collectObjInfo.getLastCollectTime())
					+ " sqlldr日志=" + sqlldrLog.getAbsolutePath());
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
			logger.error("[" + collectObjInfo.getKeyID() + "]" + "sqlldr时异常", ex);
		}
	}

	private void initSqlldr() throws Exception {
		ctlWriter.println("LOAD DATA\n" + "CHARACTERSET ZHS16GBK\n" + "INFILE '" + sqlldrTxt.getAbsolutePath() + "' APPEND INTO TABLE "
				+ TAC_INFO_STORE_TABLE + "\n" + "FIELDS TERMINATED BY \"|\"\n" + "TRAILING NULLCOLS\n" + "(\n" + "    meid,\n" + "    mdn,\n"
				+ "    imsi,\n" + "    province_name,\n" + "    province_id,\n" + "    city_name,\n" + "    city_id,\n" + "    tac_vendor,\n"
				+ "    mobile_model,\n" + "    soft_version,\n" + "    start_time date 'yyyy-mm-dd hh24:mi:ss'\n" + ")");
		IOUtils.closeQuietly(ctlWriter);
		txtWriter.println("MEID|MDN|IMSI|PROVINCE_NAME|PROVINCE_ID|CITY_NAME|CITY_ID|TAC_VENDOR|MOBILE_MODEL|SOFT_VERSION|START_TIME");
		txtWriter.flush();
	}

	/**
	 * 初始化终端目录
	 * 
	 * @return false 不用初始化，已经存在；true 不存在，已经初始化生成
	 * @throws Exception
	 */
	private boolean initWriters() throws Exception {
		basedir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "cdma_ue_info_new" + File.separator
				+ collectObjInfo.getKeyID() + File.separator);
		if (basedir.exists())
			return false;
		if (!basedir.mkdirs()) {
			throw new Exception("创建数据目录失败 - " + basedir);
		}
		return true;
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

	private static CityEntry findProv(String prov) {
//		Iterator<CityEntry> it = CITYS.values().iterator();
//		while (it.hasNext()) {
//			CityEntry ce = it.next();
//			if (ce.provinceName.equals(prov))
//				return ce;
//		}
		//推荐，尤其是容量大时
		for (Map.Entry<String, CityEntry> entry : CITYS.entrySet()){
			if (entry.getValue().provinceName.equals(prov))
				return entry.getValue();
		}
		
		return null;
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

		/* 先写入city，后写入model */
		@SuppressWarnings("unused")
		static void writeSwappedShort(RandomAccessFile output, short city, short model) throws IOException {
			output.writeShort(city);
			output.writeShort(model);
		}

		static void writeSwappedShort(RandomAccessFile output, short data) throws IOException {
			output.writeShort(data);
		}

		/**
		 * 这里是将手机型号写入bin文件，每个手机型号占2个字段。 写入的是手机型号的编号，c++程序拿到此编号，在数据库表里查。
		 */
		public boolean writeToFile(RandomAccessFile out, boolean isImsi) {
			try {
				// writeSwappedShort(out, city, model);
				// IMSI需要写入城市ID
				if (isImsi) {
					writeSwappedShort(out, city);
				}
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

		short vendorId;

		public ModelEntry() {
			super();
		}

		public ModelEntry(short modelId, String modelName, String vendorName, short vendorId) {
			super();
			this.modelId = modelId;
			this.modelName = modelName;
			this.vendorName = vendorName;
			this.vendorId = vendorId;
		}

		@Override
		public String toString() {
			return "ModelEntry [modelId=" + modelId + ", modelName=" + modelName + ", vendorId=" + vendorId + ", vendorName=" + vendorName + "]";
		}

	}

	public static void main(String[] args) throws Exception {

		// RandomAccessFile raf = new
		// RandomAccessFile("E:\\datacollector_path\\cdma_ue_info\\31601\\20111212150000\\IMSI_INDEX\\09\\0717.bin",
		// "rw");
		// raf.seek(287 * 2);
		// InfoEntry.writeSwappedShort(raf, (short) 99);
		// raf.close();

		CollectObjInfo obj = new CollectObjInfo(31601);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setParseTmpID(11040801);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-12-12 15:00:00").getTime()));
		UEInfoParser p = new UEInfoParser();
		p.mdnIndex = true;
		p.collectObjInfo = obj;
		p.fileName = "E:\\uway\\bug\\igp1\\龙计划\\20141205_1376079.txt";
		p.parseData();

	}
}
