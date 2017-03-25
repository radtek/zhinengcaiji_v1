package parser.dt;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import util.LogMgr;

import db.dao.RegionDAO;

/**
 * Region外部关联数据<br>
 * 从数据库加载
 * 
 * @author chenrongqiang @ 2013-10-17
 */
public class RegionCache {

	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 场景和REGION对应关系缓存MAP
	 */
	private static Map<Long, Region> PIECE_REGION = new HashMap<Long, Region>();

	/**
	 * 定时调度器
	 */
	private static Timer TIMER = null;

	/**
	 * 并发读写访问锁
	 */
	private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	/**
	 * 并发读锁
	 */
	private static ReadLock readLock = lock.readLock();

	/**
	 * 并发写锁
	 */
	private static WriteLock writeLock = lock.writeLock();
	
	/**
	 * 在类加载的时候调用一次加载
	 */
	static{
		load();
	}
	
	/**
	 * 数据加载方法
	 */
	private static void load() {
		log.debug("开始加载网格信息");
		Map<Long, Region> currentData = RegionDAO.getInstance().execute();
		// 加载完成数据后在替换内存的瞬间加载写锁
		writeLock.lock();
		try {
			if (currentData == null){
				log.debug("未加载到网格信息");
				return;
			}
			// 如果本次加载的数据不为空 则直接替换缓存中的信息
			PIECE_REGION = currentData;
			log.debug("加载到网格信息的数量为：" + currentData.size());
		} finally {
			writeLock.unlock();
		}
	}
	
	public static void reLoad(){
		if(TIMER != null)
			return;
		//如果TIMER没有启动 则是第一次重新加载 那么启动定时调度
		TIMER =  new Timer();
		//TODO 时间处理可以优化 目前是程序启动后一天就开始加载
		TIMER.schedule(new TimerLoader(), 24 * 60 * 60 * 1000);
	}

	/**
	 * 通过场景ID获取REGION信息
	 * @param pieceId
	 * @return
	 */
	public static Region getRegion(long pieceId) {
		readLock.lock();
		try {
			if(PIECE_REGION == null)
				return null;
			return PIECE_REGION.get(pieceId);
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 定时加载任务
	 * 
	 * @author chenrongqiang @ 2013-10-17
	 */
	static class TimerLoader extends TimerTask {

		@Override
		public void run() {
			load();
		}
	}
}
