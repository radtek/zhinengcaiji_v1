package util;

import java.util.Date;

import org.apache.log4j.Logger;

import task.CollectObjInfo;

public class SPASLogger {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	private static final String LOG_HEAD = "SPAS:";

	private static final String INSERT_SQL = "insert into log_clt_tac\n" + "            (start_time,\n" + "             app_type,\n"
			+ "             task_id,\n" + "             omc_id,\n" + "             bsc_id,\n" + "             city_id,\n"
			+ "             data_time,\n" + "             clt_state,\n" + "             detail_info)\n" + "values      (?,\n" + "             ?,\n"
			+ "             ?,\n" + "             ?,\n" + "             ?,\n" + "             ?,\n" + "             ?,\n" + "             ?,\n"
			+ "             ?)";

	/** 集团IGP */
	public static final int APP_TYPE_IGP = 1;

	/** 省侧IGP_SMART */
	public static final int APP_TYPE_IGP_SMART = 2;

	/** 正在连接 */
	public static final int CLT_STATE_CONNECTING = 1;

	/** 连接成功 */
	public static final int CLT_STATE_CONNECT_SUCCESS = 2;

	/** 正在采集 */
	public static final int CLT_STATE_GATHER = 3;

	/** 采集完毕 */
	public static final int CLT_STATE_GATHER_COMPLET = 4;

	/** 连接失败 */
	public static final int CLT_STATE_CONNECT_FAIL = 5;

	private static SPASLogger instance;

	public static synchronized SPASLogger getInstance() {
		if (instance != null)
			return instance;
		instance = new SPASLogger();
		return instance;
	}

	public boolean log(int appType, long taskId, int omcId, int bscId, int cityId, Date dataTime, int cltState, String detailInfo,
			CollectObjInfo taskInfo) {
		// if ( !SystemConfig.getInstance().isSPAS() )
		// return false;
		//
		// Connection con = null;
		// PreparedStatement ps = null;
		// try
		// {
		// con = DbPool.getConn();
		// if ( con == null )
		// {
		// log.error(LOG_HEAD + "获取数据库连接失败，记录SPAS日志失败。");
		// return false;
		// }
		// ps = con.prepareStatement(INSERT_SQL);
		// int index = 1;
		// ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
		// ps.setInt(index++, appType);
		// ps.setLong(index++, taskId);
		// ps.setInt(index++, omcId);
		// ps.setInt(index++, bscId);
		// ps.setInt(index++, cityId);
		// ps.setTimestamp(index++, new Timestamp(dataTime.getTime()));
		// ps.setInt(index++, cltState);
		//
		// if ( taskInfo != null )
		// {
		// StringBuilder devInfo = new StringBuilder();
		// devInfo.append("\n设备信息：db_url=");
		// devInfo.append(taskInfo.getDBUrl()).append(",");
		// devInfo.append("db_driver=").append(taskInfo.getDBDriver()).append(",");
		// devInfo.append("host=").append(taskInfo.getDevInfo().getIP()).append(",");
		// devInfo.append("port=").append(taskInfo.getDevPort()).append(",");
		// devInfo.append("user_name=").append(taskInfo.getDevInfo().getHostUser());
		// if ( taskInfo.getParserID() == 9006 )
		// {
		// devInfo.append(",password=").append(taskInfo.getDevInfo().getHostPwd());
		// }
		// ps.setString(index++, detailInfo + devInfo.toString());
		// devInfo.setLength(0);
		// }
		// else
		// {
		// ps.setString(index++, detailInfo);
		// }
		//
		// int ret = ps.executeUpdate();
		// if ( ret != 1 )
		// {
		// log.warn(LOG_HEAD + "insert时，受影响行数不是1，可能未成功，受影响行数=" + ret);
		// return false;
		// }
		//
		// }
		// catch (Exception e)
		// {
		// log.error(LOG_HEAD + "记录SPAS日志中发生例外。", e);
		// return false;
		// }
		// finally
		// {
		// CommonDB.close(null, ps, con);
		// }

		return true;
	}

	private SPASLogger() {
		super();
	}

}
