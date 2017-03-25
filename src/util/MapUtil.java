package util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * JDK中map util类
 * @author chenrongqiang
 * @ 2013-8-9
 */
public class MapUtil{
	
	/**
	 *默认的Map自增长系数 因为size方法使用double运算 所以设置为0.74f，而不是jdk提供实现中的0.75f
	 */
	private static final float DEFAULT_LOAD_FACTOR = 0.74f;
	
	/**
	 *默认的Map大小
	 */
	private static final int DEFAULT_INITIAL_CAPACITY = 16;
	
	/**
	 * 处理java提供的map数量大小初始值<br>
	 * double运算不精确,如果相除刚好等于0.75时，获取size会有问题,所以比例调整为0.74.在int类型最大值内都不会有问题
	 * 
	 * @param actureNum
	 * @return
	 */
	public static int size(int actureNum) {
		if (actureNum <= 12)
			return DEFAULT_INITIAL_CAPACITY;
		double power = Math.log(actureNum) / Math.log(2);
		double round = Math.round(power);
		if (power < round)
			return Math.pow(2, power) / Math.pow(2, round) > DEFAULT_LOAD_FACTOR ? (int) Math.pow(2, round + 1) : (int) Math.pow(2, round);
		return Math.pow(2, power) / Math.pow(2, round + 1) >DEFAULT_LOAD_FACTOR ? (int) Math.pow(2, round + 2) : (int) Math.pow(2, round + 1);
	}
	
	/**
	 * 通过指定的数据量创建适合的HashMap
	 * @param dataNum 指定的数据量
	 * @return  最佳大小的HashMap
	 */
	public static final <K,V> Map<K,V> create(int dataNum){
		return new HashMap<K,V>(size(dataNum));
	}
	
	/**
	 * 通过指定的数据量创建适合的HashMap
	 * @param dataNum 指定的数据量
	 * @return  最佳大小的ConcurrentHashMap
	 */
	public static final <K,V> Map<K,V> concurrentMap(int dataNum){
		return new ConcurrentHashMap<K,V>(size(dataNum));
	}
	
}
