package util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import framework.SystemConfig;

/**
 * 接收省侧IGP_SMART发送的日志，并入库。
 * 
 * @author ChenSijiang
 */
public class SpasUploadLoggingRec extends Thread {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final String SQL_INSERT = "insert into ds_log_province_upload "
			+ "(province_code,city_id,omc_id,bsc_id,vendor_code,start_time,end_time," + "file_name,file_dir,file_size,ftp_ip,ftp_user,error_msg)"
			+ " values (?,?,?,?,?,to_date(?,'yyyy-mm-dd hh24:mi:ss'),to_date(?,'yyyy-mm-dd hh24:mi:ss'),?,?,?,?,?,?)";

	private static final String ALARM_SQL_INSERT = "insert into DS_LOG_CDL_CLT_ALARM "
			+ "( task_id , insert_time , data_type , collect_path , data_time , bsc_id  , omc_id  , city_id , vendor  , "
			+ "ftp_ip  , ftp_port , ftp_user , ftp_pwd , succ_count , error_msg,is_retask ,alarm_type,province_id,collector_name)values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?,1,?,?)";

	private static final String CITY_SQL = "select city_id as city_id,max(province_id) as province_id from app_cfg_city where city_id is not null and province_id is not null group by city_id";

	private static final Map<String, String> CITYS = new HashMap<String, String>();

	private ServerSocket lis;

	static {

		try {
			Result result = CommonDB.queryForResult(CITY_SQL);
			SortedMap[] sms = result.getRows();
			for (SortedMap sm : sms) {
				CITYS.put(sm.get("city_id").toString(), sm.get("province_id").toString());
			}
		} catch (Exception e) {
			if (SystemConfig.getInstance().isSPAS())
				log.error("查城市配置表失败（仅龙计划使用，其它平台请忽略此错误）。SQL = " + CITY_SQL, e);
		} finally {
		}
	}

	public SpasUploadLoggingRec() {
		super("SPAS-Logging-Rec");
	}

	@Override
	public synchronized void start() {
		if (SystemConfig.getInstance().isSPAS())
			super.start();
	}

	@Override
	public void run() {
		log.info("SPAS省侧日志接收服务已启动，端口：" + SystemConfig.getInstance().getSpasLoggingPort());
		while (true) {
			if (lis == null) {
				try {
					lis = new ServerSocket(SystemConfig.getInstance().getSpasLoggingPort());
					log.info("SPAS日志侦听，已启动 - " + lis);
				} catch (IOException e) {
					log.error("创建侦听失败。", e);
				}
			}

			if (lis != null) {
				try {
					Socket s = lis.accept();
					SocketHandler sh = new SocketHandler(s);
					sh.start();
				} catch (Exception e) {
					log.error("接收侦听失败。", e);
				}
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				try {
					if (lis != null)
						lis.close();
				} catch (Exception exx) {
				}
				log.warn("SPAS日志接收线程退出。");
				break;
			}
		}
	}

	private static class SocketHandler extends Thread {

		Socket s;

		SocketHandler(Socket s) {
			super();
			this.s = s;
		}

		@Override
		public void run() {
			if (s == null)
				return;
			log.debug("收到了一个客户端连接：" + s);
			InputStream in = null;
			InputStreamReader reader = null;
			OutputStream out = null;
			PrintWriter pw = null;
			StringWriter sw = null;
			try {
				s.setSendBufferSize(64);
				s.setReceiveBufferSize(64);
				s.setTcpNoDelay(true);
				in = s.getInputStream();
				reader = new InputStreamReader(in);
				out = s.getOutputStream();
				pw = new PrintWriter(out, true);
				String msg = "";
				char[] buf = new char[64];
				int ret = -1;
				while ((ret = reader.read(buf)) > -1) {
					msg += (new String(buf, 0, ret));
					if (msg.contains("</log>"))
						break;
				}
				log.debug("接收到了一条省侧日志：" + msg + "，地址信息：" + s);
				if (Util.isNotNull(msg))
					pw.print("#####################################################################");
				else
					pw.print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				pw.flush();
				Thread.sleep(3 * 1000);
				IOUtils.closeQuietly(pw);
				IOUtils.closeQuietly(out);

				insertLogger(msg);
			} catch (Exception e) {
				log.error("处理Socket时发生异常。", e);
			} finally {
				IOUtils.closeQuietly(sw);
				IOUtils.closeQuietly(pw);
				IOUtils.closeQuietly(out);
				IOUtils.closeQuietly(reader);
				IOUtils.closeQuietly(in);
				try {
					s.close();
				} catch (IOException e) {
				}
				s = null;
			}
		}
	}

	private static boolean insertAlarm(String msg) {
		Connection con = null;
		Statement st = null;
		PreparedStatement ps = null;
		int ret = -1;
		try {
			Document doc = DocumentHelper.parseText(msg);
			Element root = doc.getRootElement();
			con = DbPool.getConn();

			boolean isClear = (root.elementText("clear") != null && root.elementText("clear").equals("true"));
			if (isClear) {
				/* 将原有记录的is_retask改update成1. */
				String collectpath = root.elementText("collectPath");
				String updateSql = "update ds_log_cdl_clt_alarm set succ_count=abs(succ_count),is_retask=1,error_msg=error_msg||'" + "【已通过补采解决，时间"
						+ Util.getDateString(new Date()) + "】" + "' where is_retask=0 and task_id=" + Long.parseLong(root.elementText("taskId"))
						+ " and alarm_type=1 and " + "data_time=to_date('" + root.elementText("dataTime") + "','yyyy-mm-dd hh24:mi:ss') and bsc_id="
						+ Integer.parseInt(root.elementText("bscId")) + " and omc_id=" + Integer.parseInt(root.elementText("omcId"))
						+ " and city_id=" + Integer.parseInt(root.elementText("cityId")) + " and collect_path "
						+ (Util.isNotNull(collectpath) ? "='" + collectpath + "'" : " is null ") + " and succ_count="
						+ Integer.parseInt(root.elementText("succCount"));
				st = con.createStatement();
				int count = st.executeUpdate(updateSql);
				if (count > 0) {
					log.debug("修改ds_log_cdl_clt_alarm表的is_retask成功：" + updateSql + "，受影响条数：" + count);
				} else {
					log.debug("修改ds_log_cdl_clt_alarm表的is_retask未成功，记录未找到，SQL：" + updateSql);
				}
			} else {
				ps = con.prepareStatement(ALARM_SQL_INSERT);
				ps.setLong(1, Long.parseLong(root.elementText("taskId")));
				Date date = Util.isNotNull(root.elementText("insertTime")) ? Util.getDate1(root.elementText("insertTime")) : null;
				Timestamp ts = date != null ? new Timestamp(date.getTime()) : null;
				ps.setTimestamp(2, ts);
				ps.setString(3, root.elementText("dataType"));
				ps.setString(4, root.elementText("collectPath"));
				date = Util.isNotNull(root.elementText("dataTime")) ? Util.getDate1(root.elementText("dataTime")) : null;
				ts = date != null ? new Timestamp(date.getTime()) : null;
				ps.setTimestamp(5, ts);
				ps.setInt(6, Integer.parseInt(root.elementText("bscId")));
				ps.setInt(7, Integer.parseInt(root.elementText("omcId")));
				ps.setInt(8, Integer.parseInt(root.elementText("cityId")));
				ps.setString(9, root.elementText("vendor"));
				ps.setString(10, root.elementText("ip"));
				ps.setInt(11, Integer.parseInt(root.elementText("port")));
				ps.setString(12, root.elementText("username"));
				ps.setString(13, root.elementText("password"));
				ps.setInt(14, Integer.parseInt(root.elementText("succCount")));
				ps.setString(15, handleErrorMsg(root));
				ps.setInt(16, 0);
				ps.setString(17, CITYS.get(root.elementText("cityId")));
				ps.setString(18, root.elementText("collectorName"));
				ret = ps.executeUpdate();
				log.debug("成功接收到了一条省侧告警，并入库成功：" + msg);

			}

		} catch (Exception e) {
			log.error("记录SPAS省侧日志时发生异常（" + msg + "）。", e);
		} finally {
			CommonDB.close(null, ps, null);
			CommonDB.close(null, st, con);
		}

		return ret > 0;
	}

	private static boolean insertLogger(String msg) {

		Connection con = null;
		PreparedStatement ps = null;
		int ret = -1;
		try {
			Document doc = DocumentHelper.parseText(msg);
			Element root = doc.getRootElement();
			con = DbPool.getConn();
			if (root.attributeValue("type") != null && root.attributeValue("type").equals("download")) {
				insertAlarm(msg);
			} else {
				ps = con.prepareStatement(SQL_INSERT);
				ps.setString(1, root.elementText("provinceCode"));
				ps.setInt(2, Integer.parseInt(root.elementText("cityId")));
				ps.setInt(3, Integer.parseInt(root.elementText("omcId")));
				ps.setInt(4, Integer.parseInt(root.elementText("bscId")));
				ps.setString(5, root.elementText("vendorCode"));
				ps.setString(6, root.elementText("startTime"));
				ps.setString(7, root.elementText("endTime"));
				ps.setString(8, root.elementText("fileName"));
				ps.setString(9, root.elementText("fileDir"));
				ps.setLong(10, Long.parseLong(root.elementText("fileSize")));
				ps.setString(11, root.elementText("ftpIP"));
				ps.setString(12, root.elementText("ftpUser"));
				ps.setString(13, handleErrorMsg(root));
				ret = ps.executeUpdate();
				log.debug("成功接收到了一条省侧日志，并入库成功：" + msg);
			}
		} catch (Exception e) {
			log.error("记录SPAS省侧日志时发生异常（" + msg + "）。", e);
		} finally {
			CommonDB.close(null, ps, con);
		}

		return ret > 0;
	}

	private static final int ERROR_MSG_MAX_LENGTH = 500;

	/* 对error_msg进行URLDecode操作，并对长度进行处理，限制长度。 */
	private static String handleErrorMsg(Element root) {
		if (root == null)
			return "";
		String msg = root.elementText("errorMsg");
		if (msg == null)
			return "";
		try {
			msg = URLDecoder.decode(msg, "utf-8");
		} catch (UnsupportedEncodingException e) {
		}
		if (msg.length() > ERROR_MSG_MAX_LENGTH)
			return msg.substring(0, ERROR_MSG_MAX_LENGTH) + "...";
		return msg;
	}

	public static void main(String[] args) {
		SpasUploadLoggingRec r = new SpasUploadLoggingRec();
		r.start();
	}
}
