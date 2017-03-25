package store;

import java.sql.Timestamp;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.DBLogger;
import util.LogMgr;
import exception.StoreException;

/**
 * 存储抽象类
 * 
 * @author YangJian
 * @since 3.1
 */
public class AbstractStore<T extends StoreParam> implements Store {

	protected static DBLogger dbLogger = LogMgr.getInstance().getDBLogger();

	protected static Logger log = LogMgr.getInstance().getSystemLogger();

	protected T param;

	// 系统级别字段
	private long taskID; // 系统任务号

	private Timestamp dataTime; // 系统数据时间

	private int omcID;

	private String flag; // 标识，加在生成的文件名最后（扩展名之前），以应对多文件/多表入到同一个CLT表的问题

	private CollectObjInfo collectInfo;
	
	public AbstractStore() {
		super();
	}

	public AbstractStore(T param) {
		super();
		this.param = param;
	}
	
	@Override
	public void open() throws StoreException {
		// do nothing
	}

	@Override
	public void write(String data) throws StoreException {
		// do nothing
	}

	@Override
	public void flush() throws StoreException {
		// do nothing
	}

	@Override
	public void commit() throws StoreException {
		// do nothing
	}

	@Override
	public void close() {
		// do nothing
	}

	public T getParam() {
		return param;
	}

	public void setParam(T param) {
		this.param = param;
	}

	public long getTaskID() {
		return taskID;
	}

	public void setTaskID(long taskID) {
		this.taskID = taskID;
	}

	public Timestamp getDataTime() {
		return dataTime;
	}

	public void setDataTime(Timestamp dataTime) {
		this.dataTime = dataTime;
	}

	public int getOmcID() {
		return omcID;
	}

	public void setOmcID(int omcID) {
		this.omcID = omcID;
	}

	public CollectObjInfo getCollectInfo() {
		return collectInfo;
	}

	public void setCollectInfo(CollectObjInfo collectInfo) {
		this.collectInfo = collectInfo;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

}
