package task;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import util.Util;
import access.AbstractAccessor;
import framework.ConstDef;
import framework.Factory;

/**
 * 补采任务 类
 * 
 * @author IGP TDT
 * @version 1.0
 */
public class RegatherObjInfo extends CollectObjInfo {

	private static final long serialVersionUID = 1L;

	private String filePath = "";

	private int reAdoptType = 1; // 采集类型 默认1是自动生成 2 手动生成

	private int collectTimes = 0; // 采集次数

	public String strFileName = "";

	public int recondCount = 0;

	public FileWriter m_hFile = null;

	public int tableIndex = 0;

	private List<Integer> tableIndexes = new ArrayList<Integer>(); // 需要补采的所有表在模板中在索引

	private Timestamp stamptime;

	public RegatherObjInfo(long ID, long taskID) {
		super(taskID);
		keyID = ID;
		sysName = taskID + "-" + (ID - 10000000);
	}

	public int getReAdoptType() {
		return reAdoptType;
	}

	public void setReAdoptType(int type) {
		reAdoptType = type;
	}

	public int getCollectTimes() {
		return collectTimes;
	}

	public void setCollectTimes(int times) {
		collectTimes = times;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String path) {
		filePath = path;
	}

	public void buildObj(ResultSet rs) throws Exception {
		super.buildObj(rs);

		keyID = rs.getLong("ID");
		reAdoptType = rs.getInt("READOPTTYPE");
		collectTimes = rs.getInt("COLLECTDEGRESS");

		// ------by chensj 20100517
		// 1.5的补采表，无stamptime字段，以下修正可兼容为null的stamptime字段，避免发生NullPointerException
		// --/
		// -------以下代码，之前是 stamptime = rs.getTimestamp("STAMPTIME"); 仅一行 --/
		Timestamp ts = rs.getTimestamp("STAMPTIME");
		if (ts == null) {
			stamptime = new Timestamp(new Date().getTime());
		} else {
			stamptime = ts;
		}
		// -------------------------------------------------------------------------------------------------------------------------/

		filePath = "";
		if (rs.getClob("FILEPATH") != null) {
			filePath = ConstDef.ClobParse(rs.getClob("FILEPATH"));
		}

		super.setLastCollectTime(rs.getTimestamp("COLLECTTIME"));

		if (filePath != null && filePath.length() != 0) {
			collectPath = filePath;
		}
	}

	@Override
	public void buildObj(ResultSet rs, Date scantime) throws Exception {
		TaskMgr tmgr = TaskMgr.getInstance();

		if (tmgr.isActive(rs.getLong("ID"), rs.getLong("TASKID"), ConstDef.ClobParse(rs.getClob("FILEPATH")), rs.getTimestamp("COLLECTTIME"), true)) {
			log.debug(sysName + " is active");
			return;
		}

		buildObj(rs);

		// 第几次被补采，补采时延为getRedoTimeOffset() * recltTimes，即每次翻一倍，增量式补采
		int recltTimes = RegatherStatistics.getInstance().getRecltTimes(this);

		long time = stamptime.getTime() + getRedoTimeOffset() * recltTimes * 60 * 1000;
		if (time > scantime.getTime()) {
			return;
		}
		
		//如果会话数已满，则退出，暂不执行此任务
		int result = TaskMgr.getInstance().sessionPoolHandler(this);
		if(result == 3)
			return;

		// 检查指定的补采任务是否已经合法
		boolean b = RegatherStatistics.getInstance().check(this);
		if (!b) {
			long id = keyID - 10000000;
			tmgr.updateRegatherState(-1, id);
			//已经创建了会话，但是要销毁
			if(result == 2){
				TaskMgr.getInstance().destroySession(this.getDevInfo().getIP());
			}
			return;
		}
		
		log.debug(sysName + " 第" + recltTimes + "次补采,设置的补采时延:" + getRedoTimeOffset() + "分钟,实际时延:" + (getRedoTimeOffset() * recltTimes)
				+ "分钟(设置的补采时延 * 补采次数),预计补采开始时间:" + (Util.getDateString(new Timestamp(time))));
		addTaskItem(scantime);
	}

	protected void addTaskItem(Date scantime) {
		if (TaskMgr.getInstance().addTask(this)) {
			AbstractAccessor accessor = Factory.createAccessor(this);
			this.setCollectThread(accessor);
			accessor.start();
			log.debug(sysName + " 已被加入采集队列中.");
		}
	}

	/**
	 * 针对补采任务，无论成功与否都将COLLECTSTATUS字段值修改为3（正常情况下为0）
	 */
	public boolean doAfterCollect() {
		long id = keyID - 10000000;

		boolean b = TaskMgr.getInstance().updateRegatherState(3, id);

		return b;
	}

	public Timestamp getStamptime() {
		return stamptime;
	}

	public void setStamptime(Timestamp stamptime) {
		this.stamptime = stamptime;
	}

	@Override
	public String toString() {
		return sysName;
	}

	/**
	 * 添加补采的数据源在模板中索引
	 * 
	 * @param index
	 */
	public void addTableIndex(int index) {
		if (!tableIndexes.contains(index)) {
			tableIndexes.add(index);
		}
	}

	/**
	 * 补采的数据源索引是否存在
	 * 
	 * @param index
	 * @return
	 */
	public boolean existsInTableIndexes(int index) {
		return tableIndexes.contains(index);
	}

	public boolean isEmptyTableIndexes() {
		return tableIndexes.isEmpty();
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		long id = getKeyID() - 10000000;
		return getTaskID() + "-" + id;
	}

}
