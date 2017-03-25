package task;

import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import oracle.sql.CLOB;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.Util;
import delayprobe.DelayProbeMgr;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 任务管理器
 * 
 * @author YangJian
 * @since 3.0
 */
public class TaskMgr {

	private Map<Long, CollectObjInfo> activeTasks; // 保存所有正在运行的“正常任务”任务信息

	private Map<Long, RegatherObjInfo> activeTasksForRegather; // 保存所有正在运行的“补采任务”的信息

	// <keyID,CollectObjInfo>
	private boolean checkFlag = true; // 模块校验是否正常

	private Map<Long, HashMap<Long, RegatherStruct>> regatherMap; // 补采表

	// <keyID,List<数据时间,RegatherStruct>>

	public List<CollectObjInfo> tempTasks = new ArrayList<CollectObjInfo>();

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public Map<String, Integer> sessionPool = new HashMap<String, Integer>();// 会话连接池，控制某个ip的连接数量

	protected ReentrantLock lock = new ReentrantLock();

	public int maxSessionCount = SystemConfig.getInstance().getMaxSessionCount();// 最大会话数

	private TaskMgr() {
		super();
		activeTasks = new HashMap<Long, CollectObjInfo>();
		activeTasksForRegather = new HashMap<Long, RegatherObjInfo>();
		regatherMap = new HashMap<Long, HashMap<Long, RegatherStruct>>();

		checkFlag = checkSelf();
	}

	private static class TaskMgrContainer {

		private static TaskMgr instance = new TaskMgr();
	}

	public static TaskMgr getInstance() {
		return TaskMgrContainer.instance;
	}

	/**
	 * 判断是否满足创建会话线程池的条件
	 * 
	 * @param taskInfo
	 * @return
	 */
	public boolean isReady(CollectObjInfo taskInfo) {
		// 1.此时没有配置，无法创建会话
		if (TaskMgr.getInstance().maxSessionCount <= 0) {
			return false;
		}
		// 2.没有设备信息，也创建不了会话
		DevInfo devInfo = taskInfo.getDevInfo();
		if (devInfo == null)
			return false;
		// 3.没有设备ip，也创建不了会话
		String ip = devInfo.getIP();
		if (ip == null || "".equals(ip.trim()))
			return false;
		return true;
	}

	/**
	 * 会话线程池 +1
	 * 
	 * @param ip
	 */
	public void add(String ip) {
		lock.lock();
		try {
			Object o = sessionPool.get(ip);
			sessionPool.put(ip, o == null ? 1 : (Integer) o + 1);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 会话线程池 -1
	 * 
	 * @param ip
	 */
	public void minus(String ip) {
		lock.lock();
		try {
			Object o = sessionPool.get(ip);
			sessionPool.put(ip, o == null ? -1 : (Integer) o - 1);
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 线程池是否已满
	 * 
	 * @param ip
	 * @return
	 */
	public boolean isSessionFull(String ip) {
		boolean flag = false;
		lock.lock();
		try {
			Object o = sessionPool.get(ip);
			flag = (o == null ? 0 : (Integer) o) >= maxSessionCount;
		} finally {
			lock.unlock();
		}
		return flag;
	}

	/**
	 * 返回线程池的数量
	 * 
	 * @param ip
	 * @return
	 */
	public int getSessionCount(String ip) {
		int count = 0;
		lock.lock();
		try {
			Object o = sessionPool.get(ip);
			count = (o == null ? 0 : (Integer) o);
		} finally {
			lock.unlock();
		}
		return count;
	}

	/**
	 * 任务完成后，销毁会话
	 * 
	 * @param ip
	 * @return
	 */
	public void destroySession(String ip) {
		this.minus(ip);
	}

	/**
	 * 列出所有任务,包括正常和补采的
	 * 
	 * @return
	 */
	public List<CollectObjInfo> list() {
		List<CollectObjInfo> lst = new ArrayList<CollectObjInfo>();
		// 正常任务
		Collection<CollectObjInfo> cObjs = activeTasks.values();
		for (CollectObjInfo obj : cObjs) {
			lst.add(obj);
		}
		// 补采任务
		Collection<RegatherObjInfo> cRObjs = activeTasksForRegather.values();
		for (RegatherObjInfo obj : cRObjs) {
			lst.add(obj);
		}

		return lst;
	}

	private boolean checkSelf() {
		String localHostName = Util.getHostName(); // 本地计算机名
		log.debug("check localHostName - " + localHostName);
		boolean b = true;
		if (Util.isNull(localHostName))
			b = false;

		return b;
	}

	/** 向采集队列添加采集任务 */
	public synchronized boolean addTask(CollectObjInfo obj) {
		if (obj == null)
			return false;

		// 判断是不是补采任务
		boolean isReclt = (obj instanceof RegatherObjInfo);
		// 如果任务为非活动状态并且此时系统没有达到预配置的最大线程数则添加到任务表中
		long keyID = obj.getKeyID();
		if (!isActive(keyID, isReclt) && !isMaxThreadCount(isReclt)) {
			if (isReclt) {
				activeTasksForRegather.put(keyID, (RegatherObjInfo) obj);
			} else {
				activeTasks.put(keyID, obj);
			}
			return true;
		}

		return false;
	}

	/**
	 * 线程池处理
	 * 
	 * @param obj
	 * @return flag:1/创建失败，任务可执行；2/创建成功，任务可执行；3/创建失败，因会话满，任务不可执行
	 */
	public int sessionPoolHandler(CollectObjInfo obj) {
		// 判断是否满足创建会话条件
		if (!this.isReady(obj)) {
			log.debug("任务id：" + obj.getTaskID() + ",达不到创建会话条件，可能没有配置，或者设备信息/设备ip为空");
			return 1;
		}
		String ip = obj.getDevInfo().getIP().trim();
		log.debug("任务id：" + obj.getTaskID() + ",设备ip：" + ip + ",满足会话创建条件,可申请最大会话数量:" + maxSessionCount + ",当前会话数量:" + this.getSessionCount(ip));
		if (!this.isSessionFull(ip)) {
			this.add(ip);
			log.debug("任务id：" + obj.getTaskID() + ",设备ip：" + ip + ",会话创建成功,当前会话数量:" + this.getSessionCount(ip));
			return 2;
		} else {
			log.debug("任务id：" + obj.getTaskID() + ",设备ip：" + ip + ",会话数已满，此次不执行任务:" + obj.getDescribe());
			return 3;
		}
	}

	/**
	 * 判断当前任务是否是活动的
	 */
	public synchronized boolean isActive(long taskID, boolean isReclt) {
		Map<Long, ? extends CollectObjInfo> map = isReclt ? activeTasksForRegather : activeTasks;
		boolean bExist = map.containsKey(taskID);

		// 处于活动的任务线程是否处于死锁状态
		if (bExist) {
			CollectObjInfo cltobj = map.get(taskID);

			int iBlockedTime = cltobj.getBlockedTime();
			long currTime = System.currentTimeMillis();
			// chensj 2010-08-13
			// 如果一个任务运行时间超过BLOCKEDTIME分钟，则强制终止此任务，前提是BLOCKEDTIME不为0
			if (iBlockedTime != 0 && (currTime - cltobj.startTime.getTime()) / 1000 / 60 >= iBlockedTime) {
				Thread hClt = cltobj.getCollectThread();
				hClt.interrupt();
				hClt = null;
				bExist = false;
				delActiveTask(taskID, isReclt);
				log.warn("任务-" + taskID + "[" + Util.getDateString(cltobj.getLastCollectTime()) + "]:运行时间已经超过" + iBlockedTime + "分钟，被强制终止");
			}
		}

		return bExist;
	}

	/**
	 * 判断当前任务是否是活动的
	 */
	@SuppressWarnings("unchecked")
	public synchronized boolean isActive(long keyID, long taskID, String filePath, Timestamp ts, boolean isReclt) {
		Map<Long, ? extends CollectObjInfo> map = isReclt ? activeTasksForRegather : activeTasks;

		boolean bExist = map.containsKey(keyID);

		// 处于活动的任务线程是否处于死锁状态
		if (bExist) {
			CollectObjInfo cltobj = map.get(keyID);

			int iBlockedTime = cltobj.getBlockedTime();
			long currTime = System.currentTimeMillis();
			// chensj 2010-08-13
			// 如果一个任务运行时间超过BLOCKEDTIME分钟，则强制终止此任务，前提是BLOCKEDTIME不为0
			if (iBlockedTime != 0 && (currTime - cltobj.startTime.getTime()) / 1000 / 60 >= iBlockedTime) {
				Thread hClt = cltobj.getCollectThread();
				hClt.interrupt();
				hClt = null;
				bExist = false;
				delActiveTask(keyID, isReclt);
				log.warn("任务-" + taskID + "[" + Util.getDateString(cltobj.getLastCollectTime()) + "]:运行时间已经超过" + iBlockedTime + "分钟，被强制终止");
			}
		} else {
			Collection<CollectObjInfo> values = (Collection<CollectObjInfo>) map.values();
			for (CollectObjInfo obj : values) {
				long tTaskID = obj.getTaskID();
				String tCollectPath = obj.getCollectPath();
				Timestamp tTS = obj.getLastCollectTime();
				if (taskID == tTaskID && ts.getTime() == tTS.getTime()) {
					if (tCollectPath.contains(filePath)) {
						log.info("Task-" + taskID + "-" + keyID + ": 业务意义上出现重复的任务. 数据源=" + filePath + " 数据时间=" + Util.getDateString(tTS));
						return true;
					}
				}
			}

		}

		return bExist;
	}

	/**
	 * 删除指定任务
	 * 
	 * @param taskID
	 */
	public synchronized void delActiveTask(long taskID, boolean isReclt) {
		Map<Long, ? extends CollectObjInfo> map = isReclt ? activeTasksForRegather : activeTasks;
		if (map.containsKey(taskID))
			map.remove(taskID);
	}

	/**
	 * 是否达到最大线程数量
	 */
	public synchronized boolean isMaxThreadCount(boolean isReclt) {
		Map<Long, ? extends CollectObjInfo> map = isReclt ? activeTasksForRegather : activeTasks;

		int size = map.size();
		SystemConfig sc = SystemConfig.getInstance();
		int maxThreadCount = isReclt ? sc.getMaxRecltCount() : sc.getMaxCltCount();

		if ((size < maxThreadCount) || (maxThreadCount <= 0))
			return false;
		else {
			log.warn("[" + (isReclt ? "补采任务" : "正常任务") + "]负荷过大,原因:已达到本机最大运行线程数(" + maxThreadCount + ")");
			return true;
		}
	}

	/**
	 * 获取任务表映射关系
	 */
	public Map<Long, CollectObjInfo> getTasksMap() {
		return this.activeTasks;
	}

	public Map<Long, RegatherObjInfo> getActiveTasksForRegather() {
		return activeTasksForRegather;
	}

	public synchronized CollectObjInfo getTask(long taskID) {
		return activeTasks.get(taskID);
	}

	public synchronized int size() {
		return activeTasks.size() + activeTasksForRegather.size();
	}

	/**
	 * 从表igp_conf_task中加载任务信息
	 */
	public boolean loadNormalTasksFromDB(Date scanDate) {
		if (!checkFlag)
			return false;

		log.debug("开始加载任务信息...");

		boolean bReturn = getCollectInfo(scanDate);

		log.info("load tasks from DB. --Done(" + bReturn + ")");

		return bReturn;
	}

	/**
	 * 从表IGP_CONF_RTASK中加载补采任务信息
	 */
	public void loadReGatherTasksFromDB() {
		if (!checkFlag)
			return;

		log.debug("开始加载补采表任务信息...");

		boolean bReturn = getRegatherInfo();

		log.info("load r-tasks from DB. --Done(" + bReturn + ")");
	}

	/**
	 * 设置最后成功导入的时间和位置
	 * 
	 * @param taskID
	 * @param ts
	 * @param pos
	 */
	public void setLastImportTimePos(long taskID, Timestamp ts, int pos) {
		String strTime = Util.getDateString(ts);
		StringBuffer sb = new StringBuffer();
		if (Util.isOracle()) // oracle 数据库
		{
			sb.append("update IGP_CONF_TASK set suc_data_time=to_date(?, 'YYYY-MM-DD HH24:MI:SS'),suc_data_pos= ? where TASK_ID = ?");
		} else if (Util.isSybase()) // sybase 数据库
		{
			sb.append("update IGP_CONF_TASK set suc_data_time=convert(datetime, ?),suc_data_pos= ? where TASK_ID = ?");
		} else if (Util.isMysql()) // sybase 数据库
		{
			sb.append("update IGP_CONF_TASK set suc_data_time= ? ,suc_data_pos= ? where TASK_ID = ?");
		}

		PreparedStatement pstmt = null;
		Connection conn = null;
		try {
			conn = DbPool.getConn();
			pstmt = conn.prepareStatement(sb.toString());
			pstmt.setString(1, strTime);
			pstmt.setInt(2, pos);
			pstmt.setLong(3, taskID);
			pstmt.execute();
		} catch (SQLException e) {
			log.error("Task-" + taskID + ": 更新最后导入时间、位置时出错.原因:", e);
		} finally {
			// DBUtil.close(null, pstmt, conn);
			CommonDB.close(null, pstmt, conn);
		}

	}

	/**
	 * 添加一条补采任务到igp_conf_rtask表中
	 * 
	 * @param taskInfo
	 *            任务信息
	 * @param filePath
	 *            采集路径
	 * @param cause
	 *            添加此条补采的原因
	 */
	public synchronized void newRegather(CollectObjInfo taskInfo, String filePath, String cause) {
		if (taskInfo == null || filePath == null)
			return;

		// 最大补采次数配置为-1的时候为不需要补采
		if (taskInfo.getMaxReCollectTime() == -1)
			return;

		long keyID = taskInfo.getKeyID();
		long taskID = taskInfo.getTaskID();
		Timestamp dataTime = taskInfo.getLastCollectTime();

		if (regatherMap.containsKey(keyID)) {
			HashMap<Long, RegatherStruct> map = regatherMap.get(keyID);
			if (map == null) {
				regatherMap.remove(keyID);
				return;
			}

			long time = dataTime.getTime();
			if (map.containsKey(time)) {
				RegatherStruct struct = map.get(time);
				if (struct == null) {
					map.remove(time);
					return;
				}

				struct.addDS(filePath);
			} else {
				List<String> ds = new ArrayList<String>();
				ds.add(filePath);
				RegatherStruct struct = new RegatherStruct(keyID, taskID, ds, dataTime, cause);

				map.put(time, struct);
			}
		} else {
			List<String> ds = new ArrayList<String>();
			ds.add(filePath);

			RegatherStruct struct = new RegatherStruct(keyID, taskID, ds, dataTime, cause);
			HashMap<Long, RegatherStruct> map = new HashMap<Long, RegatherStruct>();
			map.put(dataTime.getTime(), struct);

			regatherMap.put(keyID, map);
		}
	}

	/**
	 * 创建新的补采任务
	 */
	public synchronized void newRegather(CollectObjInfo taskInfo, String filePath) {
		newRegather(taskInfo, filePath, "");
	}

	/**
	 * 提交补采任务
	 * 
	 * @param obj
	 * @time 这里必须要传入一个时间，因为在commit之前CollectObjInfo的数据时间已经被修改为下一个时间点了
	 */
	public synchronized void commitRegather(CollectObjInfo obj, long time) {
		if (obj == null)
			return;

		long keyID = obj.getKeyID();
		long taskID = obj.getTaskID();

		// 最大补采次数为-1时意义为不需要补采
		if (obj.getMaxReCollectTime() == -1) {
			log.debug("Task-" + taskID + "-" + keyID + ": 最大补采次数为-1,即不需要补采.");
			return;
		}

		if (!regatherMap.containsKey(keyID))
			return;

		HashMap<Long, RegatherStruct> map = regatherMap.get(keyID);
		if (map == null || !map.containsKey(time))
			return;

		RegatherStruct struct = map.get(time);
		if (struct == null)
			return;

		if (struct.taskID != taskID)
			return;

		// 判断第几次补采，如果补采次数已够，不加入补采
		int maxCollectTime = obj.getMaxReCollectTime();
		int recltTimes = RegatherStatistics.getInstance().getRecltTimes(obj);
		if (maxCollectTime < recltTimes) {
			log.debug("Task-" + taskID + "-" + keyID + "(采集时间:" + obj.getLastCollectTime() + "): 已经达到最大补采次数" + maxCollectTime + "次，不加入补采表.");
			// if(this.updateRegatherState(-1, keyID - 10000000)){
			// log.debug("Task-" + taskID + "-" + keyID + "(采集时间:"+ obj.getLastCollectTime()
			// + "): 修改补采状态为-1成功（放弃采集）.");
			// }else{
			// log.debug("Task-" + taskID + "-" + keyID + "(采集时间:"+ obj.getLastCollectTime()
			// + "): 修改补采状态为-1失败（下次扫描继续采集）.");
			// }
			return;
		}

		// 插入到补采表中
		boolean bOK = putRegatherToDB(struct);
		if (bOK) {
			map.remove(time);
			if (map.size() == 0)
				regatherMap.remove(keyID);
		}
	}

	/** 把补采结构信息添加到补采表中 */
	private boolean putRegatherToDB(RegatherStruct struct) {
		String localHostName = Util.getHostName(); // 本地计算机名
		boolean bReturn = true;

		long taskID = struct.getTaskID();
		Long keyID = struct.getKey();
		List<String> ds = struct.getDs();
		String cause = struct.getCause();
		if (ds == null || ds.size() == 0)
			return false;

		Long id = keyID - 10000000;
		String collectTime = Util.getDateString(struct.getDataTime());

		PreparedStatement pstmt = null;
		Connection conn = null;
		StringBuffer sb = null;
		try {

			conn = CommonDB.getConnection();
			if (conn == null) {
				log.error("Task-" + taskID + "-" + keyID + ": 添加补采任务失败,原因:无法获取数据库连接.");
				return false;
			}

			if (SystemConfig.getInstance().getMRProcessId() != 0)
				localHostName += "@" + SystemConfig.getInstance().getMRProcessId();

			for (String filePath : ds) {
				if (filePath == null)
					continue;
				sb = new StringBuffer();
				String stampTime = Util.getDateString(new Date());
				if (Util.isOracle()) // oracle 数据库
				{
					// 去重复比较:
					// 对于状态为0的表示需要补采，所以我们在比较重复添加的时候只能在这个状态(比较的时候一定要先排除掉自身，否则永远都存在一条记录和自己一样)
					String sql = "select (ID + 10000000) as key,FILEPATH from IGP_CONF_RTASK where TASKID= ? and collectstatus=0 and id<> ? and COLLECTTIME=to_date(?,'YYYY-MM-DD HH24:MI:SS')";
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, taskID);
					pstmt.setLong(2, id);
					pstmt.setString(3, collectTime);
					ResultSet rs = pstmt.executeQuery();
					List<String> temp = new ArrayList<String>();
					while (rs.next()) {
						if (rs.getClob("FILEPATH") != null) {
							temp.add(ConstDef.ClobParse(rs.getClob("FILEPATH")));
						} else {
							temp.add("");
						}
					}
					boolean bExist = false;
					if (temp.size() > 0) {
						for (String s : temp) {
							if (s.contains(filePath)) {
								bExist = !Util.isNull(filePath);
								break;
							}
						}
					}

					if (!bExist) {
						Connection con = DbPool.getConn();
						PreparedStatement stmt = null;
						ResultSet res = null;
						try {
							con.setAutoCommit(false);
							// 获取序列
							stmt = con.prepareStatement("select SEQ_IGP_CONF_RTASK.Nextval from dual");
							int seq = 0;
							res = stmt.executeQuery();
							res.next();
							seq = res.getInt(1);
							stmt.close();
							stmt = null;

							String insertsql = " insert into IGP_CONF_RTASK(ID,TASKID,COLLECTOR_NAME,FILEPATH,COLLECTTIME,STAMPTIME,COLLECTSTATUS,READOPTTYPE,CAUSE) values (?,?,?,empty_clob(),to_date(?,'YYYY-MM-DD HH24:MI:SS'),to_date(?,'YYYY-MM-DD HH24:MI:SS'),0,0,empty_clob())";
							stmt = con.prepareStatement(insertsql);
							stmt.setInt(1, seq);
							stmt.setLong(2, taskID);
							stmt.setString(3, localHostName);
							stmt.setString(4, collectTime);
							stmt.setString(5, stampTime);
							stmt.execute();
							stmt.close();
							stmt = null;

							String selectsql = "select FILEPATH,CAUSE from IGP_CONF_RTASK where id= ? for update";
							stmt = con.prepareStatement(selectsql);
							stmt.setInt(1, seq);
							res = stmt.executeQuery();
							res.next();
							CLOB clob = (CLOB) res.getClob("FILEPATH");
							Writer out = clob.getCharacterOutputStream();
							out.write(filePath);
							out.flush();
							out.close();
							clob = (CLOB) res.getClob("CAUSE");
							out = clob.getCharacterOutputStream();
							out.write(cause);
							out.flush();
							out.close();
							if (con != null) {
								con.commit();
							}
							log.debug("id:" + seq + ",taskid:" + taskID + "  被加入IGP_CONF_RTASK表");
						} catch (Exception e) {
							log.error("加入补采时异常，taskid:" + taskID + "  filepath:" + filePath, e);
							if (con != null) {
								con.rollback();
							}
						} finally {
							if (res != null) {
								res.close();
							}
							if (stmt != null) {
								stmt.close();
							}
							if (con != null) {
								con.close();
							}
						}
					}

				} else if (Util.isSybase()) // sybase 数据库
				{
					sb.append(" insert into IGP_CONF_RTASK(TASKID,COLLECTOR_NAME,FILEPATH,COLLECTTIME) values (" + taskID + ",'" + localHostName
							+ "'," + "'" + filePath + "',convert(datetime,'" + collectTime + "'))");
				} else if (Util.isMysql()) // sybase 数据库
				{
					sb.append(" insert into IGP_CONF_RTASK(TASKID,COLLECTOR_NAME,FILEPATH,COLLECTTIME) values (" + taskID + ",'" + localHostName
							+ "'," + "'" + filePath + "','" + collectTime + "')");
				}

				if (sb != null && Util.isNotNull(sb.toString())) {
					log.debug("Task-" + taskID + "-" + keyID + ": 准备执行插入补采SQL语句= " + sb.toString());

					pstmt = conn.prepareStatement(sb.toString());
					pstmt.executeUpdate();

					log.info("Task-" + taskID + "-" + keyID + " 被加入补采表中. (数据时间=" + collectTime + ",数据源=" + filePath + ")");
				}
			} // for loop end
		} catch (Exception e) {
			log.error("Task-" + taskID + "-" + keyID + ": 添加补采任务时异常sql=" + sb.toString() + " (数据时间=" + collectTime + "),原因:", e);
			bReturn = false;
		} finally {
			try {
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}

		return bReturn;
	}

	/** 查询所有的配置信息 */
	private boolean getCollectInfo(Date scandate) {
		String localHostName = Util.getHostName(); // 本地计算机名
		boolean bReturn = false;

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		if (SystemConfig.getInstance().getMRProcessId() != 0)
			localHostName += "@" + SystemConfig.getInstance().getMRProcessId();

		Connection conn = null;

		try {
			log.debug("Starting getConnection...");
			conn = CommonDB.getConnection();
			log.debug("GetConnection done...");
			if (conn == null) {
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				return false;
			}

			StringBuffer sb = new StringBuffer();
			sb.append("select a.city_id,a.DEV_ID,a.DEV_NAME,a.HOST_IP,a.HOST_USER,a.HOST_PWD,a.ENCODE,a.HOST_SIGN,a.OMCID,a.vendor,b.DBDRIVER,b.DBURL,");// BY
			sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
			sb.append("b.COLLECTTIMEOUT,b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.COLLECT_TIME,b.COLLECT_TIMEPOS,b.PROB_STARTTIME,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
			sb.append("b.DISTRBUTE_TMPID,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,");
			sb.append("c.DEV_ID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME ");
			sb.append("from IGP_CONF_DEVICE a,IGP_CONF_TASK b left join IGP_CONF_DEVICE c on(b.PROXY_DEV_ID = c.DEV_ID) left join  IGP_CONF_TEMPLET d on(b.PARSE_TMPID = d.TMPID) left join  IGP_CONF_TEMPLET f on(b.distrbute_tmpid = f.TMPID)");
			sb.append("where a.DEV_ID = b.DEV_ID and b.ISUSED=1 and b.COLLECTOR_NAME='" + localHostName + "' ");
			addIds(sb, 0);

			sb.append("Order By b.suc_data_time");
			//
			String strSQL = sb.toString();
			log.debug("读取任务表SQL为: " + strSQL);

			pstmt = conn.prepareStatement(strSQL);
			rs = pstmt.executeQuery();

			int i = 0;

			if (tempTasks == null) {
				tempTasks = new ArrayList<CollectObjInfo>();
			} else {
				tempTasks.clear();
			}
			while (rs.next()) {
				try {
					long taskID = rs.getLong("TASK_ID");
					CollectObjInfo info = new CollectObjInfo(taskID);

					info.buildObj(rs, scandate);

					info.setHostName(localHostName);
				} catch (Exception e) {
					log.error("构建任务时异常.原因:", e);
				}

				++i;
			}

			tempTasks = DelayProbeMgr.probe(tempTasks);
			if (tempTasks != null) {
				for (CollectObjInfo c : tempTasks) {
					c.startTask();
					DelayProbeMgr.getTaskEntrys().remove(c.getTaskID());
				}
				tempTasks.clear();
				for (CollectObjInfo task : tempTasks) {
					if (task != null) {
						addTask(task);
					}
				}
			}

			log.debug("从任务表中select出的任务数为: " + i);

			bReturn = true;
		} catch (Exception e) {
			log.error("从任务表中读取任务信息时异常,原因:", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
			}
		}

		return bReturn;
	}

	/**
	 * @param sql
	 * @param taskType
	 *            0为采集任务，1为补采任务
	 * @return
	 */
	private synchronized void addIds(StringBuffer sql, int taskType) {
		if (taskType == 0) {
			if (activeTasks.isEmpty())
				return;
		}
		if (taskType == 1) {
			if (activeTasksForRegather.isEmpty())
				return;
		}

		List<Long> ids = toIDs(taskType);
		if (ids == null || ids.size() <= 0)
			return;
		if (taskType == 0)
			sql.append(" and b.TASK_ID NOT IN(");
		else if (taskType == 1)
			sql.append(" and e.ID NOT IN(");

		int size_1 = ids.size() - 1;
		for (int i = 0; i < size_1; i++)
			sql.append(ids.get(i) + ",");
		sql.append(ids.get(size_1));

		sql.append(") ");
	}

	private synchronized List<Long> toIDs(int type) {
		List<Long> idList = null;

		Set<Long> idSet = new HashSet<Long>();
		idSet.addAll(activeTasks.keySet());
		idSet.addAll(activeTasksForRegather.keySet());

		if (idSet.size() > 0)
			idList = new ArrayList<Long>();

		for (long id : idSet) {
			CollectObjInfo obj = getObjByID(id);
			isActive(id, obj instanceof RegatherObjInfo);
			if (obj == null)
				continue;
			// 补采
			if (type == 1 && obj instanceof RegatherObjInfo) {
				idList.add(id - 10000000);
			}
			// 正常采集
			else if (type == 0 && obj.getKeyID() == obj.getTaskID()) {
				idList.add(id);
			}
		}

		return idList;
	}

	public synchronized CollectObjInfo getObjByID(long id) {
		CollectObjInfo obj = activeTasks.get(id);
		if (obj == null)
			obj = activeTasksForRegather.get(id);

		return obj;
	}

	private boolean getRegatherInfo() {
		String localHostName = Util.getHostName(); // 本地计算机名
		boolean bReturn = false;

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			log.debug("Starting getConnection for regatherInfo...");
			conn = CommonDB.getConnection();
			log.debug("GetConnection for regatherInfo done...");
			if (conn == null) {
				log.error("从补采表中读取信息失败,原因:无法获取数据库连接.");
				return false;
			}

			if (SystemConfig.getInstance().getMRProcessId() != 0)
				localHostName += "@" + SystemConfig.getInstance().getMRProcessId();

			int maxCountPerRegather = SystemConfig.getInstance().getMaxCountPerRegather(); // 最大补采个数

			StringBuffer sb = new StringBuffer();
			sb.append("select * from (select topflag (e.ID + 10000000) as ID,e.taskid,e.filepath,e.collecttime,e.readopttype,e.collectdegress,e.collectstatus,e.collector_name,e.stamptime,c.city_id,c.DEV_ID,c.DEV_NAME,c.ENCODE,c.HOST_IP,c.HOST_USER,c.HOST_PWD,c.HOST_SIGN,c.OMCID,c.vendor,b.DBDRIVER,b.DBURL,");
			sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
			sb.append("b.COLLECTTIMEOUT,b.COLLECT_TIME,b.PROB_STARTTIME,b.COLLECT_TIMEPOS,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
			sb.append("b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,");
			sb.append("c.DEV_ID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME ");
			sb.append("from IGP_CONF_RTASK e left join IGP_CONF_TASK b on e.taskid = b.task_id left join IGP_CONF_DEVICE c  on(b.DEV_ID = c.DEV_ID) left join IGP_CONF_TEMPLET d on (d.tmpid=b.parse_tmpid) left join IGP_CONF_TEMPLET f on (f.tmpid=b.distrbute_tmpid) ");
			sb.append("where b.ISUSED=1 ");

			String strCondition = "".equals(localHostName) ? " 1=2 " : " e.COLLECTOR_NAME='" + localHostName + "' ";

			String strTemp = "";
			if (Util.isOracle()) // oracle 数据库
			{
				sb.append(" and " + strCondition);
				sb.append(" and e.COLLECTSTATUS=0 ");
				addIds(sb, 1);

				sb.append(" order by e.READOPTTYPE desc,e.COLLECTTIME desc) ");
				sb.append("where rownum <=" + maxCountPerRegather);
			} else if (Util.isSybase()) // sybase 数据库
			{
				strTemp = "top " + maxCountPerRegather;
				sb.append(" and " + strCondition + " and e.COLLECTSTATUS=0 ");
				addIds(sb, 1);
				sb.append("order by e.READOPTTYPE desc ,e.COLLECTTIME desc) ");
				sb.append("where rownum <=" + maxCountPerRegather);
			} else if (Util.isMysql()) // mysql 数据库
			{
				sb.append(" and " + strCondition + " and e.COLLECTSTATUS=0 ");
				addIds(sb, 1);
				sb.append("order by e.READOPTTYPE desc ,e.COLLECTTIME desc) ");
				sb.append(" as f limit " + maxCountPerRegather);
			}

			String strSQL = sb.toString();
			strSQL = strSQL.replaceFirst("topflag", strTemp);

			log.debug("补采任务SQL为: " + strSQL);

			pstmt = conn.prepareStatement(strSQL);
			rs = pstmt.executeQuery();

			int i = 0;
			while (rs.next()) {
				try {
					long ID = rs.getLong("ID");
					RegatherObjInfo info = new RegatherObjInfo(ID, rs.getLong("taskid"));

					info.buildObj(rs, new Date());

					info.setHostName(localHostName);
					i++;
				} catch (Exception e) {
					log.error("构建补采任务时异常,原因:", e);
				}
			}

			log.debug("从补采表中select出的补采任务数为: " + i);

			bReturn = true;
		} catch (Exception e) {
			log.error("从补采表中读取补采信息时异常,原因:", e);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (pstmt != null)
					pstmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
			}
		}

		return bReturn;
	}

	/**
	 * 更新补采表中补采任务对应的状态
	 * 
	 * @param state
	 *            -1表示此任务补采达到最大次数，下次选取补采任务的时候将忽略此任务; 0为正常状态; 3为已处理状态; 只有当状态为0时才会被触发补采
	 * @param id
	 *            表示补采任务的id(对应补采表ID字段,注意此ID非TASKID)
	 */
	public boolean updateRegatherState(int state, long id) {
		int ret = -1;
		String strSQL = "update IGP_CONF_RTASK  set collectstatus=" + state + " where ID = " + id;
		try {
			ret = CommonDB.executeUpdate(strSQL);
		} catch (SQLException e) {
			log.error("R-Task-" + id + ": 更新collectstatus字段为" + state + "时异常,原因:", e);
		}

		return ret >= 0;
	}

	/** 补采信息结构 */
	class RegatherStruct {

		private long key; // 对应任务对象中的keyID

		private long taskID; // 任务编号

		private List<String> ds; // 数据源

		private Timestamp dataTime; // 采集的数据时间

		private String cause; // 添加此条补采任务的原因

		public RegatherStruct() {
			super();
		}

		public RegatherStruct(long key, long taskID, List<String> ds, Timestamp dataTime, String cause) {
			super();
			this.key = key;
			this.taskID = taskID;
			this.ds = ds;
			this.dataTime = dataTime;
			this.cause = cause;
		}

		/**
		 * 判断数据源是否存在
		 * 
		 * @param strDS
		 *            数据源
		 * @return
		 */
		public boolean dsExists(String strDS) {
			if (ds == null)
				return false;

			return ds.contains(strDS);
		}

		/**
		 * 添加一个数据源
		 * 
		 * @param strDS
		 *            数据源
		 */
		public void addDS(String strDS) {
			if (ds == null)
				return;

			if (ds.contains(strDS))
				return;

			ds.add(strDS);
		}

		public long getKey() {
			return key;
		}

		public void setKey(long key) {
			this.key = key;
		}

		public long getTaskID() {
			return taskID;
		}

		public void setTaskID(long taskID) {
			this.taskID = taskID;
		}

		public List<String> getDs() {
			return ds;
		}

		public void setDs(List<String> ds) {
			this.ds = ds;
		}

		public Timestamp getDataTime() {
			return dataTime;
		}

		public void setDataTime(Timestamp dataTime) {
			this.dataTime = dataTime;
		}

		public String getCause() {
			return cause;
		}

		public void setCause(String cause) {
			this.cause = cause;
		}

	} // class RegatherStruct end

	public static class RedoSQL {

		public String sql;

		public String cause;

		public RedoSQL(String sql, String cause) {
			super();
			this.sql = sql;
			this.cause = cause;
		}

	}

	public static void main(String[] args) {
		// Map<String, Integer> sessionPool = new HashMap<String, Integer>();//会话连接池，控制某个ip的连接数量
		// sessionPool.put("123", sessionPool.get("123")+1);
		// System.out.print(sessionPool.get("123"));
	}
}
