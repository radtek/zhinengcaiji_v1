package access;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import parser.Parser;
import task.CollectObjInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import templet.TempletBase;
import util.CommonDB;
import util.LogMgr;
import util.Util;
import datalog.DataLogInfo;
import distributor.Distribute;
import distributor.DistributeSqlLdr;
import distributor.DistributeTemplet;
import distributor.TableItem;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 抽象接入器类
 * 
 * @author YangJian
 * @since 3.0
 */
public abstract class AbstractAccessor extends Thread implements Accessor {

	protected Logger log = LogMgr.getInstance().getSystemLogger();

	protected CollectObjInfo taskInfo;

	protected Parser parser;

	protected Distribute distributor;

	protected boolean runFlag = true;

	private boolean accessSucc = false; // 采集是否成功,默认为false

	/* 以下数据为方便每个方法使用而特意提取出来的 */
	private long taskID;

	protected String strLastGatherTime;

	private GenericDataConfig dataSourceConfig;

	protected String name;

	public AbstractAccessor() {
		super();
	}

	/** 配置接入器参数 */
	public abstract void configure() throws Exception;

	/** 接入数据 */
	public abstract boolean access() throws Exception;

	/** 关闭 */
	public void shutdown() {

	}

	/** 销毁资源 */
	public void dispose(long lastCollectTime) {
		closeFiles();

		runFlag = false;
		taskInfo.setUsed(false);

		// 删除该线程任务

		String logStr = name + ": remove from active-task-map. " + strLastGatherTime;
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_END, logStr);
		TaskMgr.getInstance().delActiveTask(taskInfo.getKeyID(), taskInfo instanceof RegatherObjInfo);

		//销毁会话
		destroySession();
		
		TaskMgr.getInstance().commitRegather(taskInfo, lastCollectTime);
	}

	/**
	 * 任务完成后，销毁会话
	 */
	public void destroySession() {
		//判断是否创建过会话，如果没创建，返回
		if(!TaskMgr.getInstance().isReady(taskInfo)){
			return;
		}
		String ip = taskInfo.getDevInfo().getIP().trim();
		//销毁，减1
		TaskMgr.getInstance().destroySession(ip);
		log.debug("任务id：" + taskInfo.getTaskID() + ",设备ip："+ ip + ",当前任务的会话被销毁,会话剩余数量:"
				+ TaskMgr.getInstance().getSessionCount(ip));
	}
	
	@Override
	public boolean validate() {
		boolean b = true;

		if (taskInfo == null)
			return false;

		// 检查是否设置了数据源
		if ((dataSourceConfig == null || dataSourceConfig.getDatas() == null) && taskInfo.getCollectType() != ConstDef.COLLECT_TYPE_FTP_DOWNLOADER
				&& taskInfo.getParserID() != ConstDef.WCDMA_HW_M2000_ALARM_STREAM_PARSER && taskInfo.getParserID() != 101
				&& taskInfo.getParserID() != 102) {
			log.error("taskId-" + taskInfo.getTaskID() + ":不是有效任务，原因，collect_path为空");
			return false;
		}

		// 检查最后一次采集时间是否正确
		try {
			strLastGatherTime = Util.getDateString(taskInfo.getLastCollectTime());
		} catch (Exception e) {
			log.error(name + "> 时间格式错误,原因:", e);
			taskInfo.log(DataLogInfo.STATUS_START, name + "> 时间格式错误,原因:", e);
			b = false;
		}

		return b;
	}

	@Override
	public void doReady() throws Exception {
		// 设置为使用状态
		taskInfo.setUsed(true);

		// 设置任务开始时间
		taskInfo.startTime = new Timestamp((new Date()).getTime());

		// 判断是否需要休息暂停几秒
		int sleepTime = taskInfo.getSleepTime();
		if (sleepTime > 0) {
			log.debug(name + " sleep " + sleepTime + " (s)");
			taskInfo.log(DataLogInfo.STATUS_START, name + " sleep " + sleepTime + " (s)");
			Thread.sleep(sleepTime * 1000);
		}
	}

	@Override
	public void doStart() throws Exception {
		log.info(name + ": 开始采集时间点为 " + strLastGatherTime + " 的数据.");
		taskInfo.log(DataLogInfo.STATUS_START, name + ": 开始采集时间点为 " + strLastGatherTime + " 的数据.");
	}

	@Override
	public boolean doBeforeAccess() throws Exception {
		return true;
	}

	@Override
	public void parse(char[] chData, int iLen) throws Exception {
		// do nothing
	}

	@Override
	public boolean doAfterAccess() throws Exception {
		return true;
	}

	@Override
	public void doFinishedAccess() throws Exception {
		// do nothing
	}

	/**
	 * 执行SQLLOAD操作
	 */
	@Override
	public void doSqlLoad() throws Exception {
		String logStr = null;

		if (accessSucc) {
			// 如果解析类型不为MR和空模板解析的，则进行sqlldr操作
			int parseType = taskInfo.getParseTmpType();
			if (parseType != ConstDef.COLLECT_TEMPLATE_THRD && parseType != ConstDef.COLLECT_TEMPLATE_NULL
					&& taskInfo.getParserID() != ConstDef.COLLECT_TEMPLATE_HW_DBF) {
				// 判断是否存在ldrlog目录，如果没有的话，就创建。否则SQLLDR无法正常运行。
				File ldrlogDirectory = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog");
				if (!ldrlogDirectory.exists()) {
					ldrlogDirectory.mkdir();
				}

				logStr = name + ": " + strLastGatherTime + " SQLLoad start.";
				log.info(logStr);
				taskInfo.log(DataLogInfo.STATUS_DIST, logStr);
				// 运行sqlldr
				runSqlldr(true);
			}

			logStr = name + ": " + strLastGatherTime + " SQLLoad end.";
			log.info(logStr);
			taskInfo.log(DataLogInfo.STATUS_END, logStr);

			logStr = name + ": " + taskInfo.getDescribe() + " import " + strLastGatherTime + " finish 任务开始时间:" + taskInfo.startTime + " "
					+ taskInfo.m_nAllRecordCount;
			log.info(logStr);
			taskInfo.log(DataLogInfo.STATUS_END, logStr);
		}
	}

	@Override
	public void doFinished() throws Exception {
		if (taskInfo.getPeriod() == ConstDef.COLLECT_PERIOD_FOREVER) {
			runFlag = true;
			while (true) {
				Thread.sleep(5 * 1000);
			}
		} else
			runFlag = false;

		// 如果是正常采集，则不管采集操作成功与否都更新到下一个采集时间点
		// 如果是补采，则不管操作成功与否都删除掉补采表中对应的记录
		taskInfo.doAfterCollect();
	}

	@Override
	public void run() {
		String logStr = null;
		long lastCollectTime = -1;
		try {
			configure();

			boolean b = validate();
			if (!b)
				return;

			doReady();
			doStart();

			b = doBeforeAccess();
			if (!b)
				return;
			lastCollectTime = taskInfo.getLastCollectTime().getTime();
			accessSucc = b = access();
			logStr = name + ": 数据(" + strLastGatherTime + ")接入完成. 接入结果=" + accessSucc;
			log.info(logStr);
			taskInfo.log(DataLogInfo.STATUS_START, logStr);

			b = doAfterAccess();

			doFinishedAccess();

			doSqlLoad();

			doFinished();

			accessSucc = true;

			// 防止更新数据库表时网络延时造成下次读取不到更新状态.
			Thread.sleep(1000 * 3);
		} catch (InterruptedException ie) {
			log.error(name + ": 任务被外界终止");
			taskInfo.log(DataLogInfo.STATUS_END, name + ": 任务被外界终止", ie);
		} catch (Exception e) {
			logStr = name + ": 数据(" + strLastGatherTime + ")接入异常,原因：";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_END, logStr, e);
		} finally {
			dispose(lastCollectTime);
		}
	}

	// 当nTableIndex =-1 时，导所有的文件，如果i=nTableIndex，则单独导这个表
	private void runSqlldr(boolean isAll) {
		boolean isRedoFlag = false;
		isRedoFlag = CommonDB.isReAdoptObj(taskInfo);

		// 非永久线程执行完成批量导入sqlldr批量导入
		DistributeSqlLdr sqlldr = new DistributeSqlLdr(taskInfo);
		DistributeTemplet distmp = (DistributeTemplet) taskInfo.getDistributeTemplet();

		if (distmp == null || distmp.tableTemplets == null)
			return;

		for (int i = 0; i < distmp.tableTemplets.size(); i++) {
			if (!isAll) {
				if (taskInfo.getActiveTableIndex() != i)
					continue;
			}
			TableItem tableItem = distmp.tableItems.get(i);

			DistributeTemplet.TableTemplet table = distmp.tableTemplets.get(i);

			String strOldFileName = tableItem.fileName;
			FileWriter fw = tableItem.fileWriter;
			try {
				// fw防止空指针异常
				if (fw != null)
					fw.close();
				else
					continue;
			} catch (IOException e) {
				log.error(this + ": runSqlldr", e);
				taskInfo.log(DataLogInfo.STATUS_DIST, this + ": runSqlldr", e);
			}
			// 关闭旧文件,创建新文件,不然临时文件不能删除

			// log.debug(name + ": getActiveTableIndex="
			// + taskInfo.getActiveTableIndex() + " table.m_nTableIndex="
			// + table.tableIndex + " strOldFileName=" + strOldFileName
			// + " i=" + i + " isAll=" + isAll);

			if (isAll) {
				// 判断是否是补采
				if (isRedoFlag) {
					RegatherObjInfo rTask = (RegatherObjInfo) taskInfo;
					if (!rTask.isEmptyTableIndexes()) {
						if (!((RegatherObjInfo) taskInfo).existsInTableIndexes(i))
							// if ( taskInfo.getActiveTableIndex() != i )
							continue;
					} else {
						// 如果是空在，最坏在处理，全部SQLLDR
					}
				}
			}

			sqlldr.buildSqlLdr(table.tableIndex, strOldFileName);

			tableItem.recordCounts = 0;

			// 当导入所有数据的时候，不需要创建新的文件
			if (!isAll) {
				String strOracleCurrentPath = SystemConfig.getInstance().getCurrentPath();

				Date now = new Date(taskInfo.getLastCollectTime().getTime());
				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
				String strTime = formatter.format(now);
				String strNewFileName = taskInfo.getGroupId() + "_" + taskInfo.getTaskID() + "_" + strTime + "_" + String.valueOf(i);
				tableItem.fileName = strNewFileName;
				try {
					fw = new FileWriter(strOracleCurrentPath + File.separatorChar + strNewFileName + ".txt");
					tableItem.fileWriter = fw;
					// 给文件首行写上字段名
					if (table.isFillTitle) {
						try {
							for (int k1 = 0; k1 < table.fields.size(); k1++) {
								DistributeTemplet.FieldTemplet field = table.fields.get(k1);
								// 添加多余的行
								if (k1 < table.fields.size() - 1)
									fw.write(field.m_strFieldName + ";");
								else
									fw.write(field.m_strFieldName);
							}
							fw.write("\n");
							fw.flush();
						} catch (IOException e) {
							log.error(name + ": runSqlldr", e);
							taskInfo.log(DataLogInfo.STATUS_DIST, name + ": runSqlldr", e);
						}
					}
				} catch (Exception e) {
					log.error(name + ": runSqlldr", e);
					taskInfo.log(DataLogInfo.STATUS_DIST, name + ": runSqlldr", e);
				}
			}
		}// for
	}

	// 关闭所有打开的句柄
	private void closeFiles() {
		TempletBase distmp = taskInfo.getDistributeTemplet();

		try {
			if (distmp instanceof DistributeTemplet) {
				for (int i = 0; i < ((DistributeTemplet) distmp).tableTemplets.size(); i++) {
					TableItem tableItem = ((DistributeTemplet) distmp).tableItems.get(i);
					if (tableItem == null)
						continue;

					FileWriter fw = tableItem.fileWriter;
					if (fw != null) {
						try {
							fw.close();
						} catch (IOException e1) {
						}// 关闭旧文件,创建新文件,不然临时文件不能删除
					}
				}
			}
		} catch (Exception e) {
			log.error("关闭所有打开的分发临时文件句柄时发生异常", e);
		}
	}

	public CollectObjInfo getTaskInfo() {
		return taskInfo;
	}

	public void setTaskInfo(CollectObjInfo obj) {
		this.taskInfo = obj;
		this.taskID = obj.getTaskID();
		this.dataSourceConfig = GenericDataConfig.wrap(obj.getCollectPath());

		long id = obj.getTaskID();
		if (obj instanceof RegatherObjInfo)
			id = obj.getKeyID() - 10000000;
		this.name = obj.getTaskID() + "-" + id;
	}

	public Parser getParser() {
		return parser;
	}

	public void setParser(Parser parser) {
		this.parser = parser;
	}

	public Distribute getDistributor() {
		return distributor;
	}

	public void setDistributor(Distribute distributor) {
		this.distributor = distributor;
	}

	public long getTaskID() {
		return taskID;
	}

	public void setTaskID(long taskID) {
		this.taskID = taskID;
	}

	/**
	 * 获得采集数据源描述
	 * 
	 * @return
	 */
	public GenericDataConfig getDataSourceConfig() {
		return dataSourceConfig;
	}

	public void setDataSourceConfig(GenericDataConfig dataSourceConfig) {
		this.dataSourceConfig = dataSourceConfig;
	}

	public String getStrLastGatherTime() {
		return strLastGatherTime;
	}

	public void setStrLastGatherTime(String strLastGatherTime) {
		this.strLastGatherTime = strLastGatherTime;
	}

	/**
	 * 采集是否成功
	 * 
	 * @return
	 */
	public boolean isAccessSucc() {
		return accessSucc;
	}

	public void setAccessSucc(boolean accessSucc) {
		this.accessSucc = accessSucc;
	}

	public String getMyName() {
		return name;
	}

}
