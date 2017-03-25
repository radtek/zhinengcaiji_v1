package parser.hw.am;

import java.io.Serializable;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.log4j.Logger;

import util.LogMgr;

/**
 * 汇总的cache Summarizing
 * 
 * @author liangww 2012-6-19
 * @version 1.0.0 1.0.1 liangww 2012-06-27 增加getHW2GM2000M1，getHW2GM2000M2方法，并删除清除线程<br>
 *          1.0.2 liangww 2012-07-26 增加中兴wcdma的汇总算法<br>
 */
public abstract class Summarizing {

	private final static String HW_WCDMA_M2000_CACHE_NAME = "hw_wcdna_m2000_m1";

	private final static String HW_GSM_M2000_CACHE_NAME = "hw_gsm_m2000_m1";

	private final static String ZTE_WCDMA_CACHE_NAME = "zte_wcdna";

	private final static String ZTE_GSM_CACHE_NAME = "zte_gsm";

	private final static Logger log = LogMgr.getInstance().getSystemLogger();

	private final static String MISS_CACHE_NAME = "miss";		//

	private final static String PREFIX_3G = "w";		// 3g的前缀标识

	private final static String PREFIX_2G = "g";		// 2g的前缀标识

	private final static String ABBREVIATORY_HW = "hw";		// 华为缩写

	private final static String ABBREVIATORY_ZTE = "zte";	// 中兴缩写

	public final static String HW_VENDOR = "ZY0808";

	public final static String ZTE_VENDOR = "ZY0804";

	private final static Serializable NULL_OBJECT = new Serializable() {

		private static final long serialVersionUID = 1L;
	};

	private static CacheManager CACHE_MANAGER = null;
	static {
		CACHE_MANAGER = CacheManager.create("./conf/ehcache.xml");
		init();
		log.info("summarizing init successful");
	}

	private static void init() {
		CACHE_MANAGER.addCache(MISS_CACHE_NAME);
		CACHE_MANAGER.addCache(HW_WCDMA_M2000_CACHE_NAME);
		// liangww add 2012-06-21 增加2g的cache
		CACHE_MANAGER.addCache(HW_GSM_M2000_CACHE_NAME);

		// liangww add 2012-07-25 增加w网的中只cache
		CACHE_MANAGER.addCache(ZTE_WCDMA_CACHE_NAME);
	}

	/**
	 * 获取查询结果
	 * 
	 * @param omcId
	 * @param neLevel
	 * @param objFdn
	 * @return
	 */
	public static QueriedEntry getHW3GM2000M1(int omcId, int neLevel, String objFdn) {
		QueriedEntry qe = null;
		String key = ABBREVIATORY_HW + PREFIX_3G + '_' + omcId + neLevel + objFdn;

		Element element = getMissElement(key);
		// 如果miss 中存在
		if (element != null) {
			return null;
		}

		// 从cache中获取出来
		Cache cache = CACHE_MANAGER.getCache(HW_WCDMA_M2000_CACHE_NAME);
		element = cache.get(key);
		if (element != null) {
			return (QueriedEntry) element.getValue();
		}

		qe = MappingTables.findHw3G(omcId, neLevel, objFdn);
		if (qe == null) {
			setMissElement(key);
		} else {
			cache.put(new Element(key, qe));
		}

		return qe;
	}

	public static QueriedEntry getHW3GM2000M2(int omcId, String bscName, String bstId) {
		QueriedEntry qe = null;
		String key = ABBREVIATORY_HW + PREFIX_3G + '_' + omcId + bscName + bstId;

		Element element = getMissElement(key);
		// 如果miss 中存在
		if (element != null) {
			return null;
		}

		// 从cache中获取出来
		Cache cache = CACHE_MANAGER.getCache(HW_WCDMA_M2000_CACHE_NAME);
		element = cache.get(key);
		if (element != null) {
			return (QueriedEntry) element.getValue();
		}

		// 没有就查询数据库
		qe = MappingTables.find3GHwBtsLeven(omcId, bscName, bstId);
		if (qe == null) {
			setMissElement(key);
		} else {
			cache.put(new Element(key, qe));
		}

		return qe;
	}

	/**
	 * 获取查询结果
	 * 
	 * @param omcId
	 * @param neLevel
	 * @param objFdn
	 * @return
	 */
	public static QueriedEntry getHW2GM2000M1(int omcId, int neLevel, String objFdn) {
		QueriedEntry qe = null;
		String key = ABBREVIATORY_HW + PREFIX_2G + '_' + omcId + neLevel + objFdn;

		Element element = getMissElement(key);
		// 如果miss 中存在
		if (element != null) {
			return null;
		}

		// 从cache中获取出来
		Cache cache = CACHE_MANAGER.getCache(HW_WCDMA_M2000_CACHE_NAME);
		element = cache.get(key);
		if (element != null) {
			return (QueriedEntry) element.getValue();
		}

		qe = MappingTables.findHw3G(omcId, neLevel, objFdn);
		if (qe == null) {
			setMissElement(key);
		} else {
			cache.put(new Element(key, qe));
		}

		return qe;
	}

	public static QueriedEntry getHW2GM2000M2(int omcId, String bscName, String bstId) {
		QueriedEntry qe = null;
		String key = ABBREVIATORY_HW + PREFIX_2G + '_' + omcId + bscName + bstId;

		Element element = getMissElement(key);
		// 如果miss 中存在
		if (element != null) {
			return null;
		}

		// 从cache中获取出来
		Cache cache = CACHE_MANAGER.getCache(HW_WCDMA_M2000_CACHE_NAME);
		element = cache.get(key);
		if (element != null) {
			return (QueriedEntry) element.getValue();
		}

		// 没有就查询数据库
		qe = MappingTables.find3GHwBtsLeven(omcId, bscName, bstId);
		if (qe == null) {
			setMissElement(key);
		} else {
			cache.put(new Element(key, qe));
		}

		return qe;
	}

	/**
	 * 获取中兴的wcdna网的
	 * 
	 * @param omcId
	 * @param bscId
	 * @param bstId
	 * @param cellId
	 * @return
	 */
	public static QueriedEntry getZTEWcdma(int omcId, String bscId, String bstId, String cellId) {
		QueriedEntry qe = null;
		String key = ABBREVIATORY_ZTE + PREFIX_3G + '_' + omcId + bscId + bstId + cellId;

		Element element = getMissElement(key);
		// 如果miss 中存在
		if (element != null) {
			return null;
		}

		// 从cache中获取出来
		Cache cache = CACHE_MANAGER.getCache(ZTE_WCDMA_CACHE_NAME);
		element = cache.get(key);
		if (element != null) {
			return (QueriedEntry) element.getValue();
		}

		// 没有就查询数据库
		qe = MappingTables.findDevTONe3G(omcId, "ZY0804", bscId, bstId, cellId);
		if (qe == null) {
			setMissElement(key);
		} else {
			cache.put(new Element(key, qe));
		}

		return qe;
	}

	/**
	 * 获取miss element
	 * 
	 * @param key
	 * @return
	 */
	static Element getMissElement(String key) {
		Cache missCache = CACHE_MANAGER.getCache(MISS_CACHE_NAME);
		return missCache.get(key);
	}

	/**
	 * 设置miss element
	 * 
	 * @param key
	 */
	static void setMissElement(String key) {
		Cache missCache = CACHE_MANAGER.getCache(MISS_CACHE_NAME);
		missCache.put(new Element(key, NULL_OBJECT));
	}

}
