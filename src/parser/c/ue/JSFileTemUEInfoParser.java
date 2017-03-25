package parser.c.ue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

import parser.Parser;
import util.CommonDB;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.SqlldrRunner;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import framework.SystemConfig;

/**
 * 针对江苏电信终端采集，之前是数据库方式的，现在改成了文件。
 * 文件格式：竖线“|”分隔的CSV文件，有9列，格式为“序列号|手机号|IMSI|省名|市名|手机厂家名|手机型号|软件版本|注册时间”。 样例：
 * 
 * <pre>
 * A1000033C03B98|15358558017|460036710278890|江苏|扬州|华立时代|SHL-H3119|1.6.4443|2013-12-31 00:00:08
 * 
 * OMCID|COLLECTTIME|STAMPTIME|SUBSCRIBER_NUMBER|IMSI|MEID|VENDOR|TERMMODEL|REGIST_TIME
 * </pre>
 * 
 * @author linpeng 2015-5-13
 */
public class JSFileTemUEInfoParser extends Parser {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	/* 将原始终端信息入库的表。 */
	private static final String TAC_INFO_STORE_TABLE = "CLT_TAIZ_T_SA_TERMINAL";
	/* 用于校验手机号，正则表达式 */
	private static Pattern phoneMatch = Pattern.compile("^[1][0-9]{10}$");
	private static SimpleDateFormat fullsdf = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static final String FILE_CODE_GBK = "gbk";
	
	private static String phonePath = SystemConfig.getInstance().getCurrentSourcePath()
			+ File.separator + "phone.txt";
	//存放手机号前三位
	private static String phoneheadPath = SystemConfig.getInstance().getCurrentSourcePath()
			+ File.separator + "phonehead.txt";

	private long taskid;

	File sqlldrInsertCtl;
	File sqlldrInsertLog;
	File sqlldrInsertTxt;
	File sqlldrInsertBad;
	File updateSource;

	PrintWriter insertTxtWriter;
	PrintWriter updateTxtWriter;

	/* 缓存所有手机号 */
	private Map<String, BitSet> bitMap;

	String stampTime;

	String omcId;

	String cltTime;

	@Override
	public boolean parseData() throws Exception {
		this.taskid = getCollectObjInfo().getTaskID();
		stampTime = Util
				.getDateString(getCollectObjInfo().getLastCollectTime());
		omcId = String.valueOf(getCollectObjInfo().getDevInfo().getOmcID());
		cltTime = Util.getDateString(new Date());

		log.debug(taskid + " 开始采集终端信息：" + this.fileName);
		Util.printMemoryStatus();
		long begin = System.currentTimeMillis();
		initDataSource();
		try {
			log.debug("sqlldr初始化");
			initSqlldr();
		} catch (Exception e) {
			return false;
		}
		InputStream in = null;
		LineIterator lines = null;
		try {
			in = new FileInputStream(fileName);
			lines = IOUtils.lineIterator(in, FILE_CODE_GBK);
			log.debug("处理目标文件");
			this.handleLines(lines);
			Util.printMemoryStatus();
			log.debug("执行数据入库");
			this.storeInsert();
			// 文件保存
			dataToFile();
			log.debug("清理bitSet内存");
			bitMap.clear();
			log.debug("System.gc()...");
			System.gc();
			log.debug("System.gc()... down");
			this.storeUpdate();
		} catch (Exception e) {
			log.error(taskid + " 采集终端信息，执行失败。", e);
			return false;
		} finally {
			IOUtils.closeQuietly(in);
			long alltime = (System.currentTimeMillis() - begin)/1000;
			log.debug("总耗时："+alltime+"秒");
		}

		return true;
	}

	/**
	 * 将每天的新文件分类记录到插入与更新的sqlldr文件中去
	 * 
	 * @param lines
	 */
	private void handleLines(LineIterator lines) {
		long count = 0;
		long insertCount = 0;
		long updateCount = 0;
		String line;
		StringBuilder buf = new StringBuilder();
		while (lines.hasNext()) {
			buf.setLength(0);
			line = lines.nextLine();
			buf.append(omcId).append("|");
			buf.append(cltTime).append("|");
			buf.append(stampTime).append("|");
			buf.append(line);
			if(line.split("\\|").length<2){
				continue;
			}
			/* 第二列为手机号 */
			String phone = line.split("\\|")[1];
			if (!isPhone(phone)) {
				log.debug("该行第二个不是电话号码：" + line);
				continue;
			}
			count++;
			if (isExitInBitSet(bitMap, phone)) {
				updateTxtWriter.println(buf.toString());
				updateCount++;
			} else {
				insertTxtWriter.println(buf.toString());
				insertCount++;
			}

			if (count % 100000 == 0) {
				log.debug(taskid + " 处理了" + count + "条记录.");
				insertTxtWriter.flush();
				updateTxtWriter.flush();
			}
		}
		insertTxtWriter.flush();
		updateTxtWriter.flush();
		IOUtils.closeQuietly(insertTxtWriter);
		IOUtils.closeQuietly(updateTxtWriter);
		log.info("将要插入文件条数：" + insertCount + "   将要更新文件条数：" + updateCount);
	}

	/**
	 * 执行插入sqlldr
	 * 
	 * @throws Exception
	 */
	private void storeInsert() throws Exception {
		String cmd = String
				.format(SqlldrRunner.RUN_CMD,
						SystemConfig.getInstance().getDbUserName(),
						SystemConfig.getInstance().getDbPassword(),
						SystemConfig.getInstance().getDbService(),
						1,
						sqlldrInsertCtl.getAbsoluteFile(),
						sqlldrInsertBad.getAbsoluteFile(),
						sqlldrInsertLog.getAbsoluteFile());
		log.debug("执行 " + cmd);
		ExternalCmd execute = new ExternalCmd();
		int ret = 0;
		try {
			ret = execute.execute(cmd);
			SqlldrResult result = new SqlLdrLogAnalyzer()
					.analysis(sqlldrInsertLog.getAbsolutePath());
			log.debug("exit=" + ret + " omcid="
					+ collectObjInfo.getDevInfo().getOmcID() + " 入库成功条数="
					+ result.getLoadSuccCount() + " 表名="
					+ result.getTableName() + " 数据时间="
					+ Util.getDateString(collectObjInfo.getLastCollectTime())
					+ " sqlldr日志=" + sqlldrInsertLog.getAbsolutePath());
			LogMgr.getInstance()
					.getDBLogger()
					.log(collectObjInfo.getDevInfo().getOmcID(),
							result.getTableName(),
							collectObjInfo.getLastCollectTime(),
							result.getLoadSuccCount(),
							collectObjInfo.getTaskID());
			if (ret == 0) {
				sqlldrInsertTxt.delete();
				sqlldrInsertCtl.delete();
				sqlldrInsertLog.delete();
				sqlldrInsertBad.delete();
			} else if (ret == 2) {
				// sqlldrTxt.delete();
				// sqlldrBad.delete();
			}
		} catch (Exception ex) {
			log.error("insert sqlldr时异常", ex);
			throw ex;
		}
	}

	private void storeUpdate() throws IOException {
		LineIterator lines = null;
		String lineData = null;
		InputStream in = new FileInputStream(updateSource);
		lines = IOUtils.lineIterator(in, FILE_CODE_GBK);
		String updateSql = "UPDATE CLT_TAIZ_T_SA_TERMINAL SET OMCID=?, COLLECTTIME=?, STAMPTIME=?, MEID=?, IMSI=?, PROVINCE_NAME=?, CITY_NAME=?, VENDOR=?, TERMMODEL=?, TERM_VERSION=?, REGIST_TIME=? WHERE SUBSCRIBER_NUMBER=?";
		PreparedStatement ps = null;
		Connection conn = DbPool.getConn();
		int failnum = 0;
		int succnum = 0;
		try {
			ps = conn.prepareStatement(updateSql);
			conn.setAutoCommit(false);
			while (lines.hasNext()) {
				lineData = lines.nextLine();
				String[] lineDatas = lineData.split("\\|");
				try {
					ps.setLong(1, Long.valueOf(lineDatas[0]));
					ps.setTimestamp(2, new Timestamp(
							getDate(lineDatas[1], null).getTime()));
					ps.setTimestamp(3,new Timestamp(getDate(lineDatas[2],null).getTime()));
					ps.setString(4, lineDatas[3]);
					ps.setString(5, lineDatas[5]);
					ps.setString(6, lineDatas[6]);
					ps.setString(7, lineDatas[7]);
					ps.setString(8, lineDatas[8]);
					ps.setString(9, lineDatas[9]);
					ps.setString(10, lineDatas[10]);
					ps.setTimestamp(11,
							new Timestamp(getDate(lineDatas[11], null)
									.getTime()));
					ps.setString(12, lineDatas[4]);
					ps.addBatch();
					succnum++;
					if (succnum % 2000 == 0) {
						ps.executeBatch();
						log.info("已批量更新条数： "+succnum);
						conn.commit();
						ps.clearBatch();
					}
				} catch (Exception e) {
					failnum++;
					log.error("将更新的失败数据：" + lineData);
					log.error("更新异常：" + e);
				}
			}
			log.info("已加载批量更新条数： "+succnum);
			ps.executeBatch();
//			log.info("成功更新条数：" + tt.length);
			conn.commit();
			conn.setAutoCommit(true);
			
			if(failnum == 0){
				updateSource.delete();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			CommonDB.close(null, ps, conn);
			log.info("总更新成功条数：" + succnum);
			log.info("总更新失败条数：" + failnum);
		}
	}

	private Date getDate(String dateStr, String format) throws ParseException {
		if ("yyyy-MM-dd".equals(format)) {
			return sdf.parse(dateStr);
		} else {
			return fullsdf.parse(dateStr);
		}
	}

	private void initSqlldr() throws Exception {
		// sqlldr入库原始文件夹。
		String filesPath = SystemConfig.getInstance().getCurrentPath()
				+ File.separator + "cdma_ue_info_sqlldr" + File.separator
				+ collectObjInfo.getKeyID() + File.separator;
		File sqlldrDir = new File(filesPath);
		if (!sqlldrDir.exists() && !sqlldrDir.mkdirs())
			throw new Exception("无法创建sqlldr临时目录：" + sqlldrDir);
		initInsertSqlldr(filesPath);
		initUpdateSource(filesPath);
	}

	/**
	 * 初始化插入sqlldr文件
	 * 
	 * @param filesPath
	 * @throws Exception
	 */
	private void initInsertSqlldr(String filesPath) throws Exception {
		sqlldrInsertCtl = new File(filesPath + "Insertstore.ctl");
		sqlldrInsertTxt = new File(filesPath + "Insertstore.txt");
		sqlldrInsertLog = new File(filesPath + "Insertstore.log");
		sqlldrInsertBad = new File(filesPath + "Insertstore.bad");
		insertTxtWriter = new PrintWriter(sqlldrInsertTxt);
		insertTxtWriter
				.println("OMCID|COLLECTTIME|STAMPTIME|MEID|SUBSCRIBER_NUMBER|IMSI|PROVINCE_NAME|CITY_NAME|VENDOR|TERMMODEL|TERM_VERSION|REGIST_TIME");
		PrintWriter ctlWriter = null;
		try {
			ctlWriter = new PrintWriter(sqlldrInsertCtl);
			StringBuilder sbd = new StringBuilder();
			sbd.append("load data\nCHARACTERSET ZHS16GBK\n");
			sbd.append("infile '").append(sqlldrInsertTxt.getAbsolutePath())
					.append("'");
			sbd.append(" append into table ").append(TAC_INFO_STORE_TABLE)
					.append("\n");
			sbd.append("FIELDS TERMINATED BY \"|\"\n");
			sbd.append("TRAILING NULLCOLS\n");
			sbd.append("(OMCID,COLLECTTIME DATE 'YYYY-MM-DD HH24:MI:SS',STAMPTIME DATE 'YYYY-MM-DD HH24:MI:SS',MEID,SUBSCRIBER_NUMBER,");
			sbd.append("IMSI,PROVINCE_NAME,CITY_NAME,VENDOR,TERMMODEL CHAR(300),TERM_VERSION CHAR(300),REGIST_TIME DATE 'YYYY-MM-DD HH24:MI:SS')");
			ctlWriter.write(sbd.toString());
		} finally {
			IOUtils.closeQuietly(ctlWriter);
		}
	}

	private void initUpdateSource(String filesPath)
			throws FileNotFoundException {
		updateSource = new File(filesPath + "UpdateSource.txt");
		updateTxtWriter = new PrintWriter(updateSource);
	}

	/**
	 * 通过文件通道进行文件内容复制
	 * 
	 * @param sourceFile
	 * @param toFile
	 */
	public void fileChannelCopy(File s, File t) {
		FileInputStream fi = null;
		FileOutputStream fo = null;
		FileChannel in = null;
		FileChannel out = null;
		try {
			fi = new FileInputStream(s);
			fo = new FileOutputStream(t);
			in = fi.getChannel();// 得到对应的文件通道
			out = fo.getChannel();// 得到对应的文件通道
			in.transferTo(0, in.size(), out);// 连接两个通道，并且从in通道读取，然后写入out通道
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fi.close();
				in.close();
				fo.close();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 20个一行写入文件，以,为分隔符
	 * 
	 * @param map
	 * @param file
	 */
	private boolean dataToFile() {
		File phoneFile = new File(phonePath);
		if (phoneFile.exists()) {
//			Date date = new Date();
//			SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
			String bakfilePath = phoneFile.getParent() + File.separator
					+ phoneFile.getName().split("\\.")[0] + ".bak";
			File bakFile = new File(bakfilePath);
			fileChannelCopy(phoneFile, bakFile);
		}
		log.debug("开始将缓存中的所有手机号保存到文件：" + phonePath);
		FileWriter writer = null;
		FileWriter writerhead = null;
		try {
			writer = new FileWriter(phoneFile);
			writerhead = new FileWriter(phoneheadPath);
			long num = 0;
			StringBuilder sbd = new StringBuilder();
			StringBuilder sbdhead = new StringBuilder();
			for (String key : bitMap.keySet()) {
				sbdhead.append(key).append("\n");
				writerhead.write(sbdhead.toString());
				sbdhead.setLength(0);
				
				BitSet bitSet = bitMap.get(key);
				num += bitSet.cardinality();
				long lnum = Integer.valueOf(key) * 100000000l;
				for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet
						.nextSetBit(i + 1)) {
					sbd.append(lnum + i).append("\n");
					writer.write(sbd.toString());
					sbd.setLength(0);
				}
			}
			log.debug("写入文件" + phonePath + "电话号码个数：" + num);
		} catch (IOException e) {
			e.printStackTrace();
			log.error("文件读写异常");
			return false;
		} finally {
			IOUtils.closeQuietly(writer);
			IOUtils.closeQuietly(writerhead);
		}
		return true;
	}

	/**
	 * 从全量文件中加载所有手机号
	 * 
	 * @return
	 * @throws IOException
	 */
	private void initDataSource() throws IOException {
		bitMap = new HashMap<String, BitSet>();
		File sourceParent = new File(SystemConfig.getInstance()
				.getCurrentSourcePath());
		if (!sourceParent.exists()) {
			sourceParent.mkdirs();
		}
		File dataSourceFile = new File(phonePath);
		File dataHeadSourceFile = new File(phoneheadPath);
		String sourceParentPath = dataSourceFile.getParent()+File.separator;
		if (dataSourceFile.exists() && dataHeadSourceFile.exists()) {
			initBitSet(dataHeadSourceFile);
			initDatabitSource(dataSourceFile);
		} else {
			dataSourceFile = new File(sourceParentPath + "phone_type_all.txt");
			initDataSource(dataSourceFile);
		}
	}
	
	/**
	 * 初始化bitMap 的 key
	 * @param dataHeadSourceFile
	 * @throws IOException
	 */
	private void initBitSet(File dataHeadSourceFile) throws IOException{
		BufferedReader bufferdReader = null;
		FileReader fr = null;
		try {
			String lineData = null;
			fr = new FileReader(dataHeadSourceFile);
			bufferdReader = new BufferedReader(fr);
			while ((lineData = bufferdReader.readLine()) != null) {
				bitMap.put(lineData, new BitSet());
			}
		} catch (IOException e) {
			log.error("读取文件失败", e);
			throw e;
		} finally {
			IOUtils.closeQuietly(bufferdReader);
			IOUtils.closeQuietly(fr);
		}
	}

	private void initDatabitSource(File dataSourceFile) throws IOException {
		BufferedReader bufferdReader = null;
		FileReader fr = null;
		try {
			long num = 0;
			String lineData = null;
			log.debug("开始读取全量文件：" + dataSourceFile.getPath());
			fr = new FileReader(dataSourceFile);
			bufferdReader = new BufferedReader(fr);
			while ((lineData = bufferdReader.readLine()) != null) {
				// 无需校验
				num++;
				addToBitSet(bitMap, lineData);
				if (num % 5000000 == 0) {
					log.debug("已加载条数" + num);
				}
			}
			log.info("全量文件共计" + num + "条数据");
		} catch (IOException e) {
			log.error("读取全量文件失败", e);
			throw e;
		} finally {
			IOUtils.closeQuietly(bufferdReader);
			IOUtils.closeQuietly(fr);
		}
	}

	private void initDataSource(File dataSourceFile) throws IOException {
		BufferedReader bufferdReader = null;
		FileReader fr = null;
		try {
			long num = 0;
			String lineData = null;
			log.debug("开始原始读取全量文件：" + dataSourceFile.getPath());
			fr = new FileReader(dataSourceFile);
			bufferdReader = new BufferedReader(fr);
			while ((lineData = bufferdReader.readLine()) != null) {
				String[] lineDatas = lineData.split(",");
				if (!isPhone(lineDatas[1])) {
					continue;
				}
				num++;
				isExitInBitSet(bitMap, lineDatas[1]);
				if (num % 5000000 == 0) {
					log.debug("已加载条数" + num);
				}
			}
			log.info("全量文件共计" + num + "条数据");
		} catch (IOException e) {
			log.error("读取全量文件失败", e);
			throw e;
		} finally {
			IOUtils.closeQuietly(bufferdReader);
			IOUtils.closeQuietly(fr);
		}
	}

	/**
	 * 判断手机号是否已存在，存在则该数据对应数据库更新，不存在则插入
	 * 
	 * @param map
	 * @param phone
	 * @return
	 */
	private boolean isExitInBitSet(Map<String, BitSet> map, String phone) {
		int phoneCode = Integer.valueOf(phone.substring(3));
		String mapCode = phone.substring(0,3);
		boolean rtn = false;
		if(map.get(mapCode) == null){
			map.put(mapCode, new BitSet());
		}else{
			rtn = map.get(mapCode).get(phoneCode);
		}
		map.get(mapCode).set(phoneCode);
		return rtn;
	}
	
	/**
	 * 用作初始化加载
	 * @param map
	 * @param phone
	 */
	private void addToBitSet(Map<String, BitSet> map, String phone) {
		int phoneCode = Integer.valueOf(phone.substring(3));
		map.get(phone.substring(0,3)).set(phoneCode);
	}

	/**
	 * 简单判断是否为手机号
	 * 
	 * @param phone
	 * @return
	 */
	private boolean isPhone(String phone) {
		return phoneMatch.matcher(phone).find();
	}

	// public static void main(String[] args) throws Exception {
	// JSFileTemUEInfoParser p = new JSFileTemUEInfoParser();
	// p.fileName = "E:\\linpeng\\ss.txt";
	// p.omcId = "110";
	// p.taskid = 10;
	// p.stampTime = "2015-05-14";
	// p.cltTime = "2015-05-14 17:23:00";
	// p.parseData();
	// }

}
