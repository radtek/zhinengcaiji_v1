package store.sqlldrManage.impl;

import java.util.Timer;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import store.sqlldrManage.AbstractPool;
import store.sqlldrManage.SqlldrCmd;
import store.sqlldrManage.listener.SqlldrListener;
import util.LogMgr;
import framework.SystemConfig;

/**
 * @author yuy
 * sqlldr进程池 处理调用sqlldr进程的集成模块
 */
public class SqlldrPool extends AbstractPool {

	/**
	 * 重入锁(同步锁)
	 */
	private ReentrantLock lock = new ReentrantLock();
	
	/**
	 * sqlldr进程池实例
	 */
	private static SqlldrPool sqlldrPool = new SqlldrPool();
	
	/**
	 * sqlldr进程池监听器
	 */
	public Timer sqlldrListener = null;
	
	/**
	 * 监听器执行周期 2min
	 */
	public int period = 2 * 60;
	
	/**
	 * 配置的最大sqlldr进程数 默认是10
	 */
	public static int maxCount = SystemConfig.getInstance().getMaxSqlldrProCount();
	
	/**
	 * log日志
	 */
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 构造函数(隐藏)
	 */
	private SqlldrPool(){
		super();
		if(list == null){
			//初始化 限定大小
			list = new Vector<Object>(maxCount);
		}
	}
	
	/**
	 * 获取sqlldr进程池实例(单例)
	 * @return
	 */
	public synchronized static SqlldrPool getInstance(){
		if(sqlldrPool == null)
			sqlldrPool = new SqlldrPool();
		return sqlldrPool;
	}
	
	@Override
	public boolean applyObj(Object obj) {
		if(obj == null)
			return false;
		lock.lock();
		try {
			if(!isFull()){
				list.add(obj);
				return true;
			}
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public void returnObj(Object obj) {
		if(obj == null)
			return;
		lock.lock();
		try {
			list.remove(obj);
		} finally {
			lock.unlock();
		}

	}

	@Override
	public int getCurrCount() {
		lock.lock();
		try {
			 return list.size();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isFull() {
		lock.lock();
		try {
			return list.size() >= maxCount;
		} finally {
			lock.unlock();
		}
	}
	
	@Override
	public int getMaxCount() {
		return maxCount;
	}
	
	/**
	 * 启动监听
	 */
	public void startListening() {
		lock.lock();
		try{
			if(sqlldrListener != null)
				return;
			log.debug("sqlldr监听线程启动.执行周期" + period + "秒");
			sqlldrListener = new Timer("sqlldr监听线程");
			sqlldrListener.schedule(new SqlldrListener(list, lock), 10, period * 1000);
		} finally {
			lock.unlock();
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 for (int i = 0; i < 100; i++)
		 {
			 new Thread(new Runnable() { 
				 public void run(){
					 try{
						SqlldrCmd externalCmd = new SqlldrCmd();
						externalCmd.taskID = (long) (Math.random() * 1000000);
						int ret = externalCmd.execute("ftp");
						System.out.println("ret = " + ret);
					 }catch (Exception e){
						e.printStackTrace();
					 }
				 }
			}).start();
		 }
	}
}
