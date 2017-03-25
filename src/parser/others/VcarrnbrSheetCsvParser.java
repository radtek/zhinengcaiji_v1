package parser.others;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import cn.uway.alarmbox.db.pool.DBUtil;
import collect.FtpUtils;

/**
 * 省际边界基站调整协调单数据csv VcarrnbrSheetCsvParser
 * 
 * @author liangww
 * @date 2012-10-12 下午3:32:53
 * @version 1.0.0
 */
public class VcarrnbrSheetCsvParser extends Parser {

	public Logger LOG = LogMgr.getInstance().getSystemLogger();

	// clt_vsite_vergeorder;
	// clt_vsite_vergeorder_sub;
	// clt_vsite_vergeorder_file;

	private String propertyFile = "." + File.separator + "conf"
			+ File.separator + "other" + File.separator + "vcarrnbr.properties";

	// "./conf/other/vcarrnbr.properties";
	Properties messages = new Properties();

	// String sql =
	// "select  * from  mod_filesmgr_serverinfo  t where t.protocal='VSITE_VERGEORDER_FTP'";

	public String province = "";

	public String pro_enname = "";

	private final static String mainSql = " insert into clt_vsite_vergeorder "
			+ "(ACTION_TIME,CODE,ORDER_TYPE,LOCAL_PRO,LOCAL_CITY,TARGET_PRO,TARGET_CITY,TITLE,"
			+ "ORDER_DESC,INITI_TIME,COMPLETE_TIME,INITIATOR,INITIATOR_CONTACT,RECIEVER,"
			+ "RECIEVER_CONTACT,STEP_TYPE,ACTION_TYPE,REMARK, ACTUAL_COMPLETE_TIME,ORDER_ATTRIBUTE, ISPROVINCE"
			+ ",OMCID, COLLECTTIME, STAMPTIME)"
			+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	// private final static String subSql =
	// " insert into clt_vsite_vergeorder_sub "
	// +
	// "(CODE, SUB_CODE, SUB_TITLE, NE_TYPE, NE_ID, NE_CHNAME, ADJUST_TYPE, ADJUST_CONTENT, ADJUST_RESULT,SUB_REMARK,FEEDBACK"
	// + ",OMCID, COLLECTTIME, STAMPTIME)"
	// + " values(?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)";

	private final static String subSql = " insert into clt_vsite_vergeorder_sub "
			+ "(CODE, SUB_CODE, SUB_TITLE, NE_TYPE, NE_ID, NE_CHNAME, ADJUST_TYPE, ADJUST_CONTENT, ADJUST_RESULT,SUB_REMARK,FEEDBACK"
			+ ",OMCID, COLLECTTIME, STAMPTIME)"
			+ " values(?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)";

	private final static String fileSql = " insert into clt_vsite_vergeorder_file "
			+ "(FILE_NAME, ORDER_CODE, ORDER_SUBCODE"
			+ ",OMCID, COLLECTTIME, STAMPTIME)" + " values(?, ?, ?,?,?,?)";

	private final static int MAIN_FIELDS_SIZE = 19;

	private final static int LOCAL_PRO_INDEX = 3; // 本地省份

	private final static int TARGET_PRO_INDEX = 5; // 目标省份

	private final static int STEP_TYPE_INDEX = 15; // 当前环节

	private final static int ACTION_TYPE_INDEX = 16; // 执行结果

	// ORDER_ATTRIBUTE，ISPROVINCE
	private final static String SEND = "0";

	private final static String RECV = "1";

	private Date collectionDate = null;

	private Set<String> mainCodeSet = new HashSet<String>();

	public VcarrnbrSheetCsvParser() {
		init();
	}

	public void init() {
		InputStream ins = null;
		try {
			ins = new FileInputStream(propertyFile);
			messages.load(ins);

		} catch (Exception e) {
			LOG.error("加载省级边界ftp属性文件失败 ，file =" + propertyFile, e);
		} finally {
			if (ins != null) {
				try {
					ins.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public boolean parseData() throws Exception {
		fileName = fileName.replace("\\\\", "\\").replace("//", "\\");
		File file = new File(fileName);// fileName
		if (!file.exists()) {
			LOG.error(collectObjInfo + ":  开始省际边界基站调整协调单数据 文件解析，文件未找到："
					+ fileName);
			return false;
		}

		String parentDir = parentFileName;// file.getParentFile().getName();

		String ftpFileName = file.getName();

		LOG.debug(collectObjInfo + ": 省级边界附件上报处理：" + fileName);

		String serverdirectory = messages.getProperty("serverdirectory");
		int serverport = 21;
		String serveraddress = messages.getProperty("serveraddress");
		String username = messages.getProperty("username");
		String password = messages.getProperty("password");
		String encode = messages.getProperty("encode");
		String bPasv = messages.getProperty("bPasv");
		province = messages.getProperty("province");
		if (util.Util.isNull(province))
			throw new Exception(propertyFile + ", 需要配置province省份节点，例如江苏，配置为江苏");

		if (util.Util.isNull(encode))
			encode = "gbk";
		else
			encode = encode.trim();
		boolean pasv = false;
		if (util.Util.isNotNull(bPasv))
			if (bPasv.equals("true") || "bPasv".equals("1"))
				pasv = true;
		try {

			FtpUtils ftpUtil = new FtpUtils(serveraddress, serverport,
					username, password, encode, pasv);
			String tmpDir = serverdirectory + "/" + parentDir + "/";
			if (!fileName.toLowerCase().endsWith(".csv")) {
				ftpUtil.uploadFile(fileName, tmpDir + ftpFileName);
				ftpUtil.closeConnect();
			}
		} catch (Exception e) {
			LOG.error("上传省级边界文件到本地FTP" + serveraddress + "失败 .", e);
		}

		if (!fileName.toLowerCase().endsWith(".csv")) {
			return true;
		}
		collectionDate = new Date();
		int index = ftpFileName.indexOf("_");
		pro_enname = ftpFileName.substring(0, index);

		BufferedReader br = null;
		try {

			String fileEncode = codeString(file.getAbsolutePath());

			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					file), fileEncode));
			// 忽略两行
			br.readLine();
			br.readLine();
			//
			paserMain(br);

			// 忽略两行
			br.readLine();
			br.readLine();
			//
			paserSub(br);
		} catch (Exception e1) {
			LOG.error(collectObjInfo + ": 解析文件 " + file + " 出现异常.原因： ", e1);
			return false;
		} finally {
			// 关闭流
			if (br != null) {
				Util.closeCloseable(br);
			}
		}

		return true;
	}

	/**
	 * 解析主表
	 * 
	 * @param br
	 * @throws Exception
	 */
	void paserSub(BufferedReader br) throws Exception {
		Connection con = null;
		PreparedStatement subPs = null;
		PreparedStatement filePs = null;

		try {
			con = DbPool.getConn();
			con.setAutoCommit(false);
			subPs = con.prepareStatement(subSql);
			filePs = con.prepareStatement(fileSql);

			String line = null;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				// 如果是为空字符串就表示主表结束
				if (line.equals("")) {
					break;
				}

				String[] array = line.split(",", 11);
				String subCode = array[0];
				String mainCode = getMainCode(array[0]);
				// 如果不存在
				if (!mainCodeSet.contains(mainCode)) {
					continue;
				}

				// CODE
				// SUB_CODE 0
				// SUB_TITLE 5
				// NE_TYPE 1
				// NE_ID 2
				// NE_CHNAME 3
				// ADJUST_TYPE 4
				// ADJUST_CONTENT 6
				// ADJUST_RESULT 7

				subPs.setString(1, mainCode);
				subPs.setString(2, subCode);
				subPs.setString(3, array[5]);
				subPs.setString(4, array[1]);
				subPs.setString(5, array[2]);
				subPs.setString(6, array[3]);
				subPs.setString(7, array[4]);
				subPs.setString(8, array[6]);
				subPs.setString(9, array[7]);
				subPs.setString(10, array[8]);
				subPs.setString(11, array[10]);
				// FEEDBACK,SUB_REMARK
				addPsValues(subPs, 12, getCollectObjInfo().getDevInfo()
						.getOmcID(), getCollectObjInfo().getLastCollectTime());

				subPs.addBatch();

				String attFile = array[9];
				if (Util.isNull(attFile)) {
					continue;
				}

				Log.debug(getCollectObjInfo() + "子表附件:" + attFile);
				String[] fileNames = attFile.split("\\|");
				for (int i = 0; i < fileNames.length; i++) {
					filePs.setString(1, fileNames[i]);
					filePs.setString(2, mainCode);
					filePs.setString(3, subCode);

					addPsValues(filePs, 4, getCollectObjInfo().getDevInfo()
							.getOmcID(), getCollectObjInfo()
							.getLastCollectTime());
					filePs.addBatch();
				}

			}

			// 执行subPs
			subPs.executeBatch();
			subPs.clearBatch();
			// 执行filePs
			filePs.executeBatch();
			filePs.clearBatch();
			con.commit();
			con.setAutoCommit(true);
		} catch (Exception e) {
			LOG.error(collectObjInfo + ": 解析子表出现异常.原因： ", e);
		} finally {
			CommonDB.close(null, subPs, null);
			CommonDB.close(null, filePs, con);
		}

	}

	/**
	 * @param ps
	 * @param omcid
	 * @param collectionDate
	 * @param stamptime
	 * @throws SQLException
	 */
	void addPsValues(PreparedStatement ps, int beginIndex, int omcid,
			Date stamptime) throws SQLException {
		ps.setInt(beginIndex, omcid);
		ps.setTimestamp(beginIndex + 1, new Timestamp(collectionDate.getTime()));
		ps.setTimestamp(beginIndex + 2, new Timestamp(stamptime.getTime()));
	}

	/**
	 * @param subCode
	 * @return
	 */
	public String getMainCode(String subCode) {
		if (subCode == null || subCode.equals("")) {
			return null;
		}

		int index = subCode.indexOf("-");
		if (index == -1) {
			return null;
		}

		return subCode.substring(0, index);
	}

	/**
	 * 解析主表
	 * 
	 * @param br
	 * @throws Exception
	 */
	void paserMain(BufferedReader br) throws Exception {
		Connection con = null;
		PreparedStatement ps = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = DbPool.getConn();
			con.setAutoCommit(false);

			// String proSql =
			// "select distinct pro_sign,pro_name  from  MOD_VSITE_CITY t ,  cfg_city t1 "
			// + " where t.city_sign=t1.enname  and  pro_sign='"
			// + pro_enname + "'  order by t.pro_name   ";
			// st = con.createStatement();
			// rs = st.executeQuery(proSql);
			//
			// while (rs.next()) {
			// province = rs.getString("pro_name");
			// break;
			// }
			DBUtil.close(rs, st, null);
			// province

			ps = con.prepareStatement(mainSql);

			String line = null;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				// 如果是为空字符串就表示主表结束
				if (line.equals("") || line.startsWith(",")) {
					break;
				}

				String[] array = line.split(",", 19);

				// 添加mainCode
				mainCodeSet.add(array[1]);

				for (int i = 0; i < array.length; i++) {
					// 时间戳
					// //协调单发起时间 2012-6-1 22:10
					// //协调单完成时间
					//
					if (i == 10 || i == 9 || i == 0 || i == 18) {
						if (array[i].equals("")) {
							ps.setTimestamp(i + 1, null);
						} else {
							java.util.Date d = null;
							String f = array[i].replace("/", "-");
							if (i == 0) {
								try {

									d = Util.getDate(f, "yyyy-MM-dd");
								} catch (Exception e) {

								}
							} else {
								// 关于协调单发起时间，协调单完成赶时间，匹配两种时间类型的解析方式
								try {
									d = Util.getDate(f, "yyyy-MM-dd HH:mm:ss");
								} catch (Exception e) {
									d = Util.getDate(f, "yyyy-MM-dd HH:mm");
								}
							}

							Timestamp t = new Timestamp(d.getTime());
							ps.setTimestamp(i + 1, t);
						}
					} else {
						ps.setString(i + 1, array[i]);
					}
				}

				//

				String orderAttributeValue = "0";// 默认发起

				// 如果相等表示本省
				if (!array[LOCAL_PRO_INDEX].trim().equals(province)) {
					orderAttributeValue = "1";
				}
				ps.setString(MAIN_FIELDS_SIZE + 1, orderAttributeValue);
				// ps.setInt(MAIN_FIELDS_SIZE+1, 1);
				String ISPROVINCE_value = getIsProvinceValue(
						array[STEP_TYPE_INDEX], array[ACTION_TYPE_INDEX],
						orderAttributeValue);
				ps.setString(MAIN_FIELDS_SIZE + 2, ISPROVINCE_value);
				// ps.setInt(MAIN_FIELDS_SIZE+2, 1);

				addPsValues(ps, MAIN_FIELDS_SIZE + 3, getCollectObjInfo()
						.getDevInfo().getOmcID(), getCollectObjInfo()
						.getLastCollectTime());

				ps.addBatch();
			}

			ps.executeBatch();
			ps.clearBatch();
			con.commit();
		} catch (Exception e) {
			LOG.error(collectObjInfo + ": 解析主表出现异常.原因： ", e);
		} finally {
			CommonDB.close(null, ps, con);
		}
	}

	/**
	 * 获取isProviceValue的值
	 * 
	 * @param stepType
	 * @param actionType
	 * @param orderAttributeValue
	 * @return
	 */
	public static String getIsProvinceValue(String stepType, String actionType,
			String orderAttributeValue) {
		// TODO 这里可以用map,用stepType, actionType, orderAttributeValue组合成一个key
		// stepType actionType orderAttributeValue
		// -------------------------
		// 本端发起
		// 新建 0 0 新建的草稿单，不需上传
		// 发起 0 1 本地地市－目标地市，采集需上传
		// 执行 同意 0 2 目标地市同意请求，采集需下载
		// 执行 驳回 0 3 目标地市驳回请求，采集需下载
		// 执行 驳回 0 4 本地地市向本地省网提交
		// 执行 驳回 0 5 本地省网向目标省网提交，采集需上传
		// 执行 驳回 0 6 目标省网向目标地市提交
		// 完成 驳回/同意 0 7 目标省网优中心/本地省网/本地地市，取消/完成，采集需下载/上传
		// 接单 同意 0 8 目标地市同意接单，采集需下载
		//

		// 对端发起
		// 新建 1 0 新建的草稿单
		// 发起 1 1 本地地市－目标地市，采集需下载
		// 执行 同意 1 2 目标地市同意请求，采集需上传
		// 执行 驳回 1 3 目标地市驳回请求，采集需上传
		// 执行 驳回 1 4 本地地市向本省省网提交
		// 执行 驳回 1 5 本省省网向目标省网提交，采集需下载
		// 执行 驳回 1 6 目标省网向目标地市提交
		// 完成 驳回/同意 1 7 目标省网优中心/本省省网/本地地市，取消/完成，采集需上传/下载
		// 接单 同意 1 8 目标地市同意接单，采集需上传

		// 如果是发送
		if (SEND.equals(orderAttributeValue)) {
			if (stepType.equals("执行完毕") && actionType.equals("同意")) {
				return "2";
			} else if ((stepType.equals("不予执行") && actionType.equals("驳回"))) {
				return "3";
			} else if ((stepType.equals("完成") && actionType.equals("驳回"))
					|| stepType.equals("完成") && actionType.equals("同意")) {
				return "7";
			} else if (stepType.equals("接单")) {
				return "8";
			}

		}
		// 如果是接收
		else if (RECV.equals(orderAttributeValue)) {
			if (stepType.equals("发起") && util.Util.isNull(actionType)) {
				return "1";
			} else if (stepType.equals("不予执行") && actionType.equals("驳回")) {
				return "5";
			} else if ((stepType.equals("完成") && actionType.equals("驳回"))
					|| stepType.equals("完成") && actionType.equals("同意")) {
				return "7";
			} else if (stepType.equals("接单")) {
				return "8";
			}
		}

		return "-1";
	}

	public static void main(String[] args) {
		// String line =
		// "2012-8-1,BJ201206010001,边界优化协调会,北京,北京,河北,保定,会议协调单－北京北京－河北保定－灵璧大王庄基站1扇区方位角调整,调整方位角45度->48度,2012-6-1 22:10:00,,张三,15312344321,李四,15312344322,发起,,,1";
		// String[] array = line.split(",", 19);
		// System.out.println(array[18]);

		VcarrnbrSheetCsvParser p = new VcarrnbrSheetCsvParser();
		String s = "d:/TJ_VERGEORDER_20140321_BJ_TJ201403200002.csv";
		p.fileName = s;


		CollectObjInfo collectObjInfo = new CollectObjInfo(1);
		DevInfo devInfo = new DevInfo();
		collectObjInfo.setLastCollectTime(new Timestamp(new Date().getTime()));
		devInfo.setOmcID(1);
		collectObjInfo.setDevInfo(devInfo);
		p.collectObjInfo = collectObjInfo;
		try {
			p.parseData();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// String s="123";
		// String sss[] =s.split("\\|");
		// for(String ss: sss){
		// System.out.println(ss);
		// }
	}

	/**
	 * 判断文件的编码格式
	 * 
	 * @param fileName
	 *            :file
	 * @return 文件编码格式
	 * @throws Exception
	 */
	public  String codeString(String fileName) throws Exception {
		String code = null;
		BufferedInputStream bin = null;
		try {
			bin = new BufferedInputStream(new FileInputStream(fileName));
			int p = (bin.read() << 8) + bin.read();

			switch (p) {
			case 0xefbb:
				code = "UTF-8";
				break;
			case 0xfffe:
				code = "Unicode";
				break;
			case 0xfeff:
				code = "UTF-16BE";
				break;
			default:
				code = "GBK";
			}
		} catch (Exception e) {
			LOG.error( "获取文件编码出现异常.原因： ", e);

		} finally {
           IOUtils.closeQuietly(bin);
		}

		return code;
	}

}
