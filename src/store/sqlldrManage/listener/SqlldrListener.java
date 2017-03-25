package store.sqlldrManage.listener;

import java.util.Date;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import store.sqlldrManage.bean.SqlldrPro;
import util.LogMgr;

/**
 * @author yuy
 * 监听线程 负责监听维护sqlldr进程池模块
 */
public class SqlldrListener extends TimerTask{
	/**
	 * log日志
	 */
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 对象List
	 */
	public Vector<Object> list;
	
	/**
	 * 重入锁(同步锁)
	 */
	private ReentrantLock lock = null;
	
	/**
	 * 单个sqlldr进程的超时时间 1h
	 */
	public int timeOut = 1 * 60 * 60;
	
	public SqlldrListener(Vector<Object> list, ReentrantLock lock){
		this.list = list;
		this.lock = lock;
	}
	
	public void run(){
		//循环遍历list
		lock.lock();
		try {
			if(list == null || list.size() == 0)
				return;
			for(Object obj : list){
				SqlldrPro pro = (SqlldrPro)obj;
				//超时
				if(new Date().getTime() - pro.getStartTime().getTime() > timeOut * 1000){
					if(pro.getPro() == null)
						continue;
					
					//结束sqlldr进程
					pro.getPro().destroy();
					
					log.debug("任务id：" + pro.getTaskID() + ",sqlldr进程超时(超时设置" + timeOut + "秒)，已结束进程.运行该进程的命令是：" + pro.getCmd());
				}
			}
		} finally {
			lock.unlock();
		}
	}
}
