package store.sqlldrManage;

import java.util.Vector;

/**
 * @author yuy
 * 抽象池类 
 */
public abstract class AbstractPool {
	
	/**
	 * 对象List
	 */
	public Vector<Object> list;
	
	/**
	 * 配置的最大容量限制，即object的总数
	 */
	private int maxCount = 5;
	
	/**
	 * 申请一个对象
	 * @return 是否成功 true&&false
	 */
	abstract public boolean applyObj(Object obj);
	
	/**
	 * 归还一个对象
	 */
	abstract public void returnObj(Object obj);
	
	/**
	 * 返回所有对象的总数量
	 * @return Num of all Objects
	 */
	abstract public int getCurrCount();
	
	/**
	 * 判断是否已满
	 * @return 是否已满 true&&false
	 */
	abstract public boolean isFull();
	
	/**
	 * 返回池的容量
	 * @return maxCount
	 */
	public int getMaxCount() {
		return maxCount;
	}
	
	/**
	 * 开启监听线程池
	 */
	abstract public void startListening();

}
