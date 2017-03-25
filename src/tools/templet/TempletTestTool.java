package tools.templet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import task.CollectObjInfo;
import util.CommonDB;
import access.AbstractAccessor;
import framework.Factory;
import framework.SystemConfig;

/**
 * 模板测试工具
 * 
 * @author litp Jul 26, 2010
 * @since igp1.0
 */
public class TempletTestTool {

	/**
	 * 根据任务号从数据库中获取此task
	 * 
	 * @param taskId
	 * @return
	 */
	private CollectObjInfo buildTask(int taskId) {
		CollectObjInfo obj = null;
		SystemConfig config = SystemConfig.getInstance();
		String driver = config.getDbDriver();
		String url = config.getDbUrl();
		String name = config.getDbUserName();
		String pwd = config.getDbPassword();

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		StringBuffer sb = new StringBuffer();
		sb.append("select a.city_id,a.DEV_ID,a.DEV_NAME,a.HOST_IP,a.HOST_USER,a.HOST_PWD,a.HOST_SIGN,a.OMCID,a.vendor,b.DBDRIVER,b.DBURL,");// BY
		sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
		sb.append("b.COLLECTTIMEOUT,b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.COLLECT_TIME,b.COLLECT_TIMEPOS,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
		sb.append("b.DISTRBUTE_TMPID,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,"); // By
		sb.append("c.DEV_ID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME ");
		sb.append("from IGP_CONF_DEVICE a,IGP_CONF_TASK b left join IGP_CONF_DEVICE c on(b.PROXY_DEV_ID = c.DEV_ID) left join  IGP_CONF_TEMPLET d on(b.PARSE_TMPID = d.TMPID) left join  IGP_CONF_TEMPLET f on(b.distrbute_tmpid = f.TMPID)");
		sb.append("where a.DEV_ID = b.DEV_ID and b.TASK_ID=" + taskId);
		String sql = sb.toString();
		conn = CommonDB.getConnection(taskId, driver, url, name, pwd);
		if (conn == null) {
			return obj;
		}
		try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				int taskID = rs.getInt("TASK_ID");
				obj = new CollectObjInfo(taskID);
				obj.buildObj(rs);
				// String localHostName = Util.getHostName();
				// obj.setHostName(localHostName);
			}
		} catch (Exception e) {
			System.out.println("执行查询异常:" + e.getMessage());
		} finally {
			CommonDB.close(rs, ps, conn);
		}

		return obj;
	}

	/**
	 * 根据taskId获取数据接入器
	 */
	private AbstractAccessor getAccessor(int taskId) {
		AbstractAccessor accessor = null;
		CollectObjInfo obj = buildTask(taskId);
		if (obj == null) {
			System.out.println("没有获取到数据，采集任务为空");
			return accessor;
		}
		accessor = Factory.createAccessor(obj);
		obj.setCollectThread(accessor);
		return accessor;
	}

	/**
	 * 测试模板
	 * 
	 * @param taskId
	 *            任务号
	 * @return
	 * @throws Exception
	 */
	public boolean testTemplet(int taskId) throws Exception {

		boolean flag = false;
		AbstractAccessor accessor = getAccessor(taskId);
		if (accessor == null) {
			System.out.println("数据接入器为空");
			return flag;
		}
		accessor.doReady();
		flag = accessor.doBeforeAccess();
		if (!flag)
			return flag;
		flag = accessor.access();
		flag = accessor.doAfterAccess();
		accessor.doSqlLoad();
		return flag;
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("参数错误!请检查参数。");
			return;
		}

		TempletTestTool ttt = new TempletTestTool();

		// 要测试的任务号
		String idStr = args[0];
		int taskId = Integer.parseInt(idStr);
		boolean flag = false;
		try {
			flag = ttt.testTemplet(taskId);
		} catch (Exception e) {
			System.out.println("测试出错：" + e.getMessage());
		}
		if (flag) {
			System.out.println("任务:" + taskId + ",测试成功!");
		} else {
			System.out.println("任务:" + taskId + ",测试失败!");
		}
	}

}
