package alarm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.LogMgr;
import util.MsgQueue;
import util.Util;
import framework.SystemConfig;

/**
 * 告警管理器(IGP_DATA_ALARM)
 * 
 * @author ltp Apr 20, 2010
 * @since 3.0
 */
public class AlarmMgr {

	private AlarmSender alarmSender; // 告警发送器

	private static AlarmMgr instance = null;

	private MsgQueue<Alarm> alarmQ; // 告警队列

	private Scanner scanner; // 告警表扫描器

	private ExecutorService executorService;

	private static final int DEFAULT_MAX_SENDER_THRD_COUNT = 2; // 默认最大告警发送线程为2

	private static final Map<Byte, AlarmReSendRule> rules; // 告警发送失败后的重发规则<alarmlevel,AlarmReSendRule>

	private static String ALARM_SQL = "INSERT INTO IGP_DATA_ALARM(ID,ALARMLEVEL,TITLE,SRC,DESCRIPTION,OCCUREDTIME,TS,ERRORCODE,TASKID) VALUES(SEQ_IGP_DATA_ALARM.nextval,%s,'%s','%s','%s',to_date('%s','YYYY-MM-DD HH24:MI:SS'),sysdate,%s,%s)";

	private boolean enable = true; // 模块是否启用标识，默认为true

	private SystemConfig config = SystemConfig.getInstance();

	private List<RuleFilter> filters = null;// 规则过滤器集合

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	static {
		// 初始化重发规则
		rules = new HashMap<Byte, AlarmReSendRule>();
		rules.put((byte) 0, new AlarmReSendRule(1, 5));
		rules.put((byte) 1, new AlarmReSendRule(1, 5));
		rules.put((byte) 2, new AlarmReSendRule(2, 5));
		rules.put((byte) 3, new AlarmReSendRule(3, 5));
		rules.put((byte) 4, new AlarmReSendRule(4, 5));
		rules.put((byte) 5, new AlarmReSendRule(5, 10));
	}

	private AlarmMgr() {
		super();
		enable = config.isEnableAlarm();
		if (!enable)
			return;

		// 初始化
		init();

		alarmQ = new MsgQueue<Alarm>();
		// 启动告警扫描线程
		scanner = new Scanner("Alarm-Scanner-Thrd");
		scanner.start();
		// 启动告警发送线程
		executorService = Executors.newFixedThreadPool(DEFAULT_MAX_SENDER_THRD_COUNT);
		for (int i = 0; i < DEFAULT_MAX_SENDER_THRD_COUNT; i++)
			executorService.execute(new Sender());
	}

	/**
	 * 使用DCL模式创建类的实例
	 * 
	 * @return
	 */
	public static AlarmMgr getInstance() {
		if (instance == null) {
			synchronized (AlarmMgr.class) {
				if (instance == null) {
					instance = new AlarmMgr();
				}
			}
		}
		return instance;
	}

	public void shutdown() {
		if (alarmQ != null)
			alarmQ.clear();

		// 停止扫描线程
		if (scanner != null) {
			scanner.shutdown();
			scanner = null;
		}

		// 停止发送线程
		if (executorService != null)
			executorService.shutdown();
	}

	private void init() {
		// 加载告警发送器
		loadSender();
		// 加载告警过滤器
		loadFilters();
		// 创建表
		createTable();
		// 创建序列
		createSeq();
	}

	/**
	 * 装载告警发送器
	 */
	@SuppressWarnings("unchecked")
	private void loadSender() {
		// 告警发送类
		String sender = config.getSender();
		if (Util.isNotNull(sender)) {
			try {
				alarmSender = ((Class<? extends AlarmSender>) Class.forName(sender)).newInstance();
			} catch (Exception e) {
				log.error("加载告警发送类时发生异常！" + sender, e);
			}
		}

	}

	/**
	 * 装载过滤器
	 */
	@SuppressWarnings("unchecked")
	private void loadFilters() {
		// 告警过滤类
		List<String> filList = config.getFilters();
		if (filList != null && !filList.isEmpty()) {
			filters = new ArrayList<RuleFilter>();
			for (String fi : filList) {
				try {
					filters.add(((Class<? extends RuleFilter>) Class.forName(fi)).newInstance());

				} catch (Exception e) {
					log.error("加载过滤类时发生异常！" + fi, e);
				}
			}
		}
	}

	/**
	 * 创建告警消息表 IGP_DATA_ALARM
	 */
	private void createTable() {
		StringBuilder sb = new StringBuilder("CREATE TABLE IGP_DATA_ALARM(");
		sb.append("ID NUMBER NOT NULL ENABLE,");
		sb.append("ALARMLEVEL NUMBER DEFAULT 1 NOT NULL ENABLE,");
		sb.append("TITLE VARCHAR2(255) NOT NULL ENABLE,");
		sb.append("SRC VARCHAR2(255) NOT NULL ENABLE,");
		sb.append("STATUS NUMBER DEFAULT 0 NOT NULL ENABLE,");
		sb.append("DESCRIPTION VARCHAR2(1000),");
		sb.append("OCCUREDTIME DATE NOT NULL ENABLE,");
		sb.append("PROCESSEDTIME DATE,TS DATE,");
		sb.append("ERRORCODE NUMBER,TASKID NUMBER,");
		sb.append("SENTTIMES NUMBER DEFAULT 0 NOT NULL ENABLE)");
		try {
			CommonDB.executeUpdate(sb.toString());
		} catch (SQLException e) {
			if (e.getErrorCode() == 955)
				// log.error("此表已存在!" + sb.toString());
				return;
			else
				log.error("创建表IGP_DATA_ALARM异常！" + sb, e);
		}
	}

	/**
	 * 创建序列 SEQ_IGP_DATA_ALARM
	 */
	private void createSeq() {
		String seq = "create sequence SEQ_IGP_DATA_ALARM start with 1 increment by 1 nocycle";
		try {
			CommonDB.executeUpdate(seq);
		} catch (SQLException e) {
			if (e.getErrorCode() == 955)
				// log.error("此序列已存在!" + seq);
				return;
			else
				log.error("创建序列sequence异常！" + seq, e);
		}
	}

	/**
	 * 插入告警记录
	 * 
	 * @param taskID
	 *            任务号
	 * @param level
	 *            告警级别
	 * @param title
	 *            标题
	 * @param source
	 *            告警源
	 * @param description
	 *            描述
	 * @param errorCode
	 *            错误码
	 * @param occuredTime
	 *            发生时间
	 */
	public void insert(long taskID, byte level, String title, String source, String description, int errorCode, Date occuredTime) {
		if (!enable)
			return;
		// 如果过滤后为true就添加，false就不会添加
		Alarm a = toAlarm(taskID, level, title, source, description, errorCode, occuredTime);
		if (!filter(a)) {
			return;
		}
		String sql = String.format(ALARM_SQL, level, a.getTitle(), a.getSource(), a.getDescription(), Util.getDateString(occuredTime), errorCode,
				taskID);
		int result = -1;
		try {
			result = CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			log.error("告警添加失败！" + sql, e);
		}
		if (result != -1) {
			log.debug("告警添加成功！");
		}
	}

	/**
	 * 插入告警记录
	 * 
	 * @param taskID
	 *            任务号
	 * @param title
	 *            标题
	 * @param source
	 *            告警源
	 * @param description
	 *            描述
	 * @param errorCode
	 *            错误码
	 * @param occuredTime
	 *            发生时间
	 */
	public void insert(long taskID, String title, String source, String description, int errorCode, Date occuredTime) {
		this.insert(taskID, (byte) 0, title, source, description, errorCode, occuredTime);
	}

	/**
	 * 插入告警记录
	 * 
	 * @param taskID
	 *            任务号
	 * @param title
	 *            标题
	 * @param source
	 *            告警源
	 * @param description
	 *            描述
	 * @param errorCode
	 *            错误码
	 */
	public void insert(long taskID, String title, String source, String description, int errorCode) {
		this.insert(taskID, title, source, description, errorCode, new Date());
	}

	/**
	 * 插入告警记录
	 * 
	 * @param level
	 *            告警级别
	 * @param taskID
	 *            任务号
	 * @param level
	 * @param title
	 *            标题
	 * @param source
	 *            告警源
	 * @param description
	 *            描述
	 * @param errorCode
	 *            错误码
	 */
	public void insert(int taskID, byte level, String title, String source, String description, int errorCode) {
		this.insert(taskID, level, title, source, description, errorCode, new Date());
	}

	/**
	 * 插入告警记录
	 * 
	 * @param alarm
	 *            告警实体类
	 */
	public void insert(Alarm alarm) {
		this.insert(alarm.getTaskID(), alarm.getAlarmLevel(), alarm.getTitle(), alarm.getSource(), alarm.getDescription(), alarm.getErrorCode(),
				alarm.getOccuredTime());
	}

	/**
	 * @param alarm
	 * @return boolean true:可以添加这条告警, fasle:不能添加这条告警
	 */
	private boolean filter(Alarm alarm) {
		boolean flag = true;
		if (filters != null) {
			for (RuleFilter rule : filters) {
				// 如果过滤失败就不添加这条数据
				if (!rule.doFilter(alarm)) {
					flag = false;
					break;
				}
			}
		}
		return flag;
	}

	private Alarm toAlarm(long taskID, byte level, String title, String source, String description, int errorCode, Date occuredTime) {
		Alarm alarm = new Alarm();
		alarm.setTaskID(taskID);
		alarm.setAlarmLevel(level);
		alarm.setTitle(title);
		alarm.setSource(source);
		alarm.setDescription(description);
		alarm.setErrorCode(errorCode);
		alarm.setOccuredTime(occuredTime);
		return alarm;
	}

	/**
	 * 从表IGP_DATA_ALARM扫描需要发送在告警记录 扫描还未发送过和发送失败的告警,即status=0,-1的记录
	 */
	class Scanner extends Thread {

		private static final String SQL = "select * from IGP_DATA_ALARM where status=0 or status=-1 order by alarmlevel desc,occuredtime asc";

		private static final long INTERVAL = 5000L; // 扫描间隔 默认为5秒钟

		private boolean runFlag = true;

		public Scanner() {
			super();
		}

		public Scanner(String name) {
			super(name);
		}

		public synchronized boolean isRunning() {
			return this.runFlag;
		}

		public synchronized void shutdown() {
			this.runFlag = false;
		}

		private void builderAlarm(ResultSet rs) throws Exception {
			if (rs == null) {
				return;
			}
			Date now = new Date();

			while (rs.next()) {
				// 业务判断,用于查看告警是否还应该被发送
				long id = rs.getLong("ID");
				byte alarmLevel = rs.getByte("ALARMLEVEL");
				int sentTimes = rs.getInt("SENTTIMES");
				byte status = rs.getByte("STATUS");
				Date ts = rs.getDate("TS");
				AlarmReSendRule reSendRule = rules.get(alarmLevel);
				if (reSendRule == null) {
					log.error("重发规则中不含有此级别：" + alarmLevel);
					continue;
				}
				Alarm alarm = null;
				// 如果已发送次数还未达到最大发送次数并且发送还没有超时，或者为一条新消息时就发送此告警
				if ((reSendRule.getMaxReSendTimes() > sentTimes && (now.getTime() - ts.getTime()) < reSendRule.getTimeout() * 60 * 1000)
						|| status == 0) {
					// 构建一条告警
					alarm = new Alarm();
					alarm.setId(id);
					alarm.setAlarmLevel(alarmLevel);
					alarm.setTitle(rs.getString("TITLE"));
					alarm.setSource(rs.getString("SRC"));
					alarm.setStatus(status);
					alarm.setDescription(rs.getString("DESCRIPTION"));
					alarm.setOccuredTime(rs.getDate("OCCUREDTIME"));
					Date processedTime = rs.getDate("PROCESSEDTIME");
					if (processedTime != null) {
						alarm.setProcessedTime(processedTime);
					}
					alarm.setErrorCode(rs.getInt("ERRORCODE"));
					alarm.setTaskID(rs.getInt("TASKID"));
					alarm.setSentTimes(sentTimes);
				} else {
					// 将status改为-2,即弃用此条告警
					String update = "UPDATE IGP_DATA_ALARM SET STATUS=-2 WHERE ID=" + id;
					try {
						CommonDB.executeUpdate(update);
					} catch (SQLException e) {
						log.error("更改告警状态为-2错误！" + update, e);
					}
				}
				if (alarm != null) {
					alarmQ.put(alarm);
					log.debug("构建1条告警！");
				}
			}

		}// end of method builderAlarm.

		@Override
		public void run() {
			log.info("扫描器开始扫描！");
			while (isRunning()) {
				Connection conn = null;
				PreparedStatement stm = null;
				ResultSet rs = null;
				try {
					conn = CommonDB.getConnection();
					stm = conn.prepareStatement(SQL);
					rs = stm.executeQuery();
					builderAlarm(rs);
				} catch (Exception e) {
					log.error("扫描器发生错误！", e);
				} finally {
					CommonDB.close(rs, stm, conn);
				}
				try {
					Thread.sleep(INTERVAL);
				} catch (InterruptedException e) {
					log.error("邮件扫描器睡眠被打断！", e);
				}

			}
		}
	} // class Scanner end.

	/**
	 * 告警发送线程,从告警队列中读取告警并发送
	 */
	class Sender implements Runnable {

		private String updateSql = "UPDATE IGP_DATA_ALARM SET STATUS=%s,SENTTIMES=SENTTIMES+1,PROCESSEDTIME=to_date('%s','YYYY-MM-DD HH24:MI:SS') WHERE ID =%s";

		private boolean runFlag = true;

		public synchronized boolean isRunning() {
			return this.runFlag;
		}

		public synchronized void shutdown() {
			this.runFlag = false;
		}

		@Override
		public void run() {
			log.info("告警发送器开始运行！");
			while (isRunning()) {
				String sql = null;
				try {
					while (alarmSender == null)
						Thread.sleep(1000L);

					Alarm alarm = alarmQ.get();
					if (alarm == null)
						continue;
					// 发送告警
					byte result = alarmSender.send(alarm);
					// 用于记录处理时间
					Date now = new Date();
					// 如果告警发送成功
					if (result == 0) {
						log.debug("消息发送成功！");
						sql = String.format(updateSql, 1, Util.getDateString(now), alarm.getId());
					} else {
						// 发送失败时,将status改为-1，senttimes加1
						log.debug("消息发送失败！");
						sql = String.format(updateSql, -1, Util.getDateString(now), alarm.getId());
					}
					CommonDB.executeUpdate(sql);
				} catch (Exception e) {
					log.error("更改告警状态错误！" + sql, e);
				}
			}
		}
	} // class Sender end.

	// 单元测试
	public static void main(String[] args) {

	}
}
