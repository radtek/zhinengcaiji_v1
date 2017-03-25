package parser.others.bsa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import parser.Parser;
import util.DbPool;
import util.LogMgr;
import cn.uway.alarmbox.db.pool.DBUtil;

/**
 * 
 * BsaLogParser
 * 
 * @author liuwx 2012-9-1
 */
public class BsaLogParser extends Parser {

	public Logger LOG = LogMgr.getInstance().getSystemLogger();

	// private String filename = "F:\\bsalog.txt";

	private String split = ",";

	private Connection con = null;

	private long seqlogvalue = 0;

	private final String left = "(";

	private final String right = ")";

	private final String flag = ",";

	private String seqName = "SEQ_LOG_BSA_RECEIPT_FILE";

	private String recInsert = "insert into log_bsa_receipt_file "
			+ " (logid, syncmode, synctype, syncfile, syncer, syncdate, recordcount, succcount, errcount,filelength)" + " values "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?,?)";

	private String log_bsa_fail_detail_sql = "insert into log_bsa_fail_detail  (logdetailid, logid, errcode, pilot_sector_name, sid, nid, extend_bid, errmessage)  values (SEQ_LOG_BSA_FAIL_DETAIL.Nextval,?,?,?,?,?,?,?)  ";

	private final int comitper = 500;
	



	/** 把 yyyy-MM-dd HH:mm:ss形式的字符串 转换成 时间 */
	public static Date getDate1(String str) throws ParseException {
		String pattern = "yyyy-MM-dd HH:mm";
		SimpleDateFormat f = new SimpleDateFormat(pattern);
		str = str.replace("/", "-");
		Date d = f.parse(str);
		return d;
	}

	public boolean processBSAReceipt(String[] fs, String[] senfs, String tmpFile, long length) {
		// ADD,RSP,2012/04/11 06:50, 100
		// RIGHT,200,ERR,0
		boolean bflag = false;

		Statement st = null;
		PreparedStatement ps = null;
		try {
			seqlogvalue = getSeq(seqName);
		} catch (Exception e1) {
			LOG.error("获取序列 失败：", e1);
		}

		try {
			con = DbPool.getConn();

			con.setAutoCommit(false);
			ps = con.prepareStatement(recInsert);
			int index = 1;
			ps.setLong(index++, seqlogvalue);
			ps.setString(index++, fs[1]);
			ps.setString(index++, fs[0]);
			ps.setString(index++, tmpFile);
			ps.setString(index++, "System");

			java.util.Date d = getDate1(fs[2]);
			Timestamp t = new Timestamp(d.getTime());
			ps.setTimestamp(index++, t);

			ps.setInt(index++, Integer.valueOf(fs[3].trim()));
			ps.setInt(index++, Integer.valueOf(senfs[1].trim()));
			ps.setInt(index++, Integer.valueOf(senfs[3].trim()));
			ps.setLong(index++, length);

			bflag = ps.execute();

			if (con != null) {
				con.commit();
			}
			return bflag;
		} catch (Exception e) {
			LOG.error("插入LOG_BSA_receipt_FILE 失败：" + recInsert, e);
			try {
				if (con != null) {
					con.rollback();
				}
			} catch (Exception ex) {
			}
			return bflag;
		} finally {
			try {
				if (st != null) {
					st.close();
				}
				// if ( con != null )
				// {
				// con.close();
				// }
			} catch (Exception e) {
			}
		}
	}

	/**
	 * 执行select数据
	 * 
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public long getSeq(String seqName) throws Exception {
		String sql = "select " + seqName + ".nextval as SEQ from dual ";
		ResultSet resultSet = null;
		PreparedStatement preparedStatement = null;

		try {
			con = DbPool.getConn();
			preparedStatement = con.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			long intSeq = 0;
			while (resultSet.next()) {
				intSeq = resultSet.getLong("SEQ");
			}
			return intSeq;
		} finally {
			close(resultSet, preparedStatement, null);
		}

	}

	/**
	 * 关闭所有连接
	 */
	public static void close(ResultSet rs, Statement stm, Connection conn) {

		if (rs != null) {
			try {
				rs.close();
			} catch (Exception e) {

			}
		}

		if (stm != null) {
			try {
				stm.close();
			} catch (Exception e) {

			}
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {

			}
		}
	}

	@Override
	public boolean parseData() throws Exception {

		File file = new File(fileName);// fileName
		if (!file.exists()) {
			LOG.error(collectObjInfo + ":  开始BSA LOG_BSA_receipt_FILE 定位日志文件解析，文件未找到：" + fileName);
			return false;
		}

		String tmpFile = file.getName();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		} catch (FileNotFoundException e1) {
			LOG.error(collectObjInfo + ": 解析文件出现异常.原因： ", e1);
		}

		String lineData = null;
		String firstLine = br.readLine();
		String secondLine = br.readLine();

		String[] fs = firstLine.split(split);
		String[] senfs = secondLine.split(split);

		processBSAReceipt(fs, senfs, tmpFile, file.length());

		// 一行一行读取
		try {
			List<String> sqlList = new ArrayList<String>();
			int count = 0;
			// 001,pn-147-test,15403,15,185576992
			while ((lineData = br.readLine()) != null) {
				lineData = lineData.trim();
				String[] lines = lineData.split(split);
				if ((lines != null && lines[0].matches("[a-z|A-Z]{1,}")) || lines.length < 5)
					continue;
				String failDetailInsert = "insert into log_bsa_fail_detail "
						+ " (logdetailid, logid, errcode, pilot_sector_name, sid, nid, extend_bid, errmessage)" + "values "
						+ "(SEQ_LOG_BSA_FAIL_DETAIL.Nextval, " + seqlogvalue + ", '" + "000" + "', '" + lines[0] + "' , '" + lines[1] + "' , '"
						+ lines[2] + "' , '" + lines[3] + "' , '" + lines[4] + "' )";

				sqlList.add(failDetailInsert);

				count++;

				if (count % comitper == 0) {
					executeBatch(sqlList);
				}
			}
			executeBatch(sqlList);
		} catch (IOException e) {
			LOG.error(collectObjInfo + ":  开始BSA LOG_BSA_receipt_FILE 定位日志文件解析 ：" + fileName + "出现错误.");
			con.rollback();
			return false;
		} finally {
			if (br != null)
				br.close();
			close(null, null, con);
		}

		return true;
	}

	public int[] executeBatch(List<String> sqlList) throws Exception {
		int[] result = null;
		Statement stm = null;
		if (con == null) {
			log.error("批量提交获取数据库连接失败！");
			return result;
		}
		try {
			if (sqlList != null && !sqlList.isEmpty()) {
				con.setAutoCommit(false);
				stm = con.createStatement();
				for (String sql : sqlList) {
					stm.addBatch(sql);
				}
				result = stm.executeBatch();
				con.commit();
			}
		} finally {
			close(null, stm, null);
		}
		return result;
	}

	/**
	 * 暴露给外部方法
	 * 
	 * @param parameters
	 *            作为外部扩展查询使用
	 */
	public void updateNeBsa(List<String> updateList) throws Exception {
		String ids[] = null;
		
		if(updateList==null ||  updateList.size()== 0)
			return ; 
			
		
		Date   currDate= new Date();
		String sqlUpdateNeBsaCarr = " update   ne_bsa_carr t set t.updater ='IGP'  , t.upload_time =sysdate , t.upload_status = 1   ";
		
		String sqlUpdateNeBsaFakeCarr = " update   ne_bsa_fake_carr t set t.updater ='IGP'  , t.upload_time =sysdate , t.upload_status = 1   ";

		long begin = System.currentTimeMillis();
		try {
		
			for (String sid_nid_extid : updateList) {
				ids = sid_nid_extid.split(",");

				StringBuilder whereCondition = new StringBuilder();
				for (int i = 0; i < ids.length; i++) {

					String key = ids[i];
					whereCondition.append(left);
					whereCondition.append(key.replace("_", ","));
					whereCondition.append(right);
					whereCondition.append(flag);
					if ((i + 1) % 500 == 0) {
						whereCondition.deleteCharAt(whereCondition.length() - 1);

						String sql1 = null;
						String sql2 = null;
						if (util.Util.isNotNull(whereCondition.toString())) {
							String where = " where (sid ,nid,extend_bid )  in (" + whereCondition + " ) ";
							sql1 = sqlUpdateNeBsaCarr + where;
							
							sql2 = sqlUpdateNeBsaFakeCarr + where;
						} 
						// 执行sql语句
						executeUpdateO(con, sql1);
						// 清零操作
						if (whereCondition.length() > 0)
							whereCondition.setLength(0);
					}
				}

				// 不能被500整除的条件sql语句
				if (whereCondition.length() > 0) {
					// 去掉最后的一个“，”
					whereCondition.deleteCharAt(whereCondition.length() - 1);

					String sql1 = null;
					String sql2 = null;
					if (util.Util.isNotNull(whereCondition.toString())) {
						String where = " where (sid ,nid,extend_bid )  in (" + whereCondition + " ) ";
						sql1 = sqlUpdateNeBsaCarr + where;
						
						sql2 = sqlUpdateNeBsaFakeCarr + where;
					}
					// 执行sql语句
					executeUpdateO(con, sql1);
					// 清零操作
					if (whereCondition.length() > 0)
						whereCondition.setLength(0);

				}

			}

		} finally {
			long end = System.currentTimeMillis();
		}
	}

	/**
	 * 更新数据库
	 * 
	 * @param sql
	 * @return 受影响的条数
	 */
	public int executeUpdate(String sql) throws Exception {
		if (con == null)
			return -1;
		return executeUpdate(con, sql);
	}

	/**
	 * 更新数据库, 不关闭数据库连接
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            要执行的语句
	 * @return 受影响的条数
	 */
	public static int executeUpdate(Connection conn, String sql) throws Exception {
		int count = -1;
		// add by yanb 2013-12-10 修改findbug，增加为空的判断
		if (null == conn) {
			return count;
		}

		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			count = ps.executeUpdate();
			if (conn != null)
				conn.commit();
		} catch (Exception e) {
			try {
				if (conn != null)
					conn.rollback();
			} catch (Exception ex) {
			}
			throw e;
		} finally {
			close(null, ps, conn);
		}

		return count;
	}

	/**
	 * 更新数据库,不用关闭连接
	 * 
	 * @param conn
	 *            数据库连接
	 * @param sql
	 *            要执行的语句
	 * @return 受影响的条数
	 */
	public static int executeUpdateO(Connection conn, String sql) throws Exception {
		int count = -1;
		// add by yanb 2013-12-10 修改findbug，增加为空的判断
		if (null == conn) {
			return count;
		}
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			count = ps.executeUpdate();
			if (conn != null)
				conn.commit();
		} catch (Exception e) {
			try {
				if (conn != null)
					conn.rollback();
			} catch (Exception ex) {
			}
			throw e;
		} finally {
			DBUtil.close(null, ps, null);
		}

		return count;
	}

	// public static void main(String[] args) {
	//
	// String line =
	// "2012-8-1,BJ201206010001,边界优化协调会,北京,北京,河北,保定,会议协调单－北京北京－河北保定－灵璧大王庄基站1扇区方位角调整,调整方位角45度->48度,2012-6-1 22:10:00,,张三,15312344321,李四,15312344322,发起,,,1";
	// String[] array = line.split(",", 19);
	// System.out.println(array[18]);
	//
	// BsaLogParser p = new BsaLogParser();
	// String s = "e:\\REQ_Type_201210120808_025_1000.csv";
	// p.fileName = s;
	//
	// CollectObjInfo collectObjInfo = new CollectObjInfo(1);
	// DevInfo devInfo = new DevInfo();
	// collectObjInfo.setLastCollectTime(new Timestamp(new Date().getTime()));
	// devInfo.setOmcID(1);
	// collectObjInfo.setDevInfo(devInfo);
	// p.collectObjInfo = collectObjInfo;
	// try {
	// p.parseData();
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	//
	// }

	public static void main(String[] args) {
		String name = "ddd.zip";
		System.out.println(name.matches("[a-z|.]{0,}p"));
	}
}
