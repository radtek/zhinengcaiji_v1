package parser.lucent.evdo;

import java.util.List;

/**
 * Evdo话单解析回调接口。
 * 
 * @author ChenSijiang 2009.01.25
 * @since 1.0
 * @see EvdoParser
 */
public interface EvdoParserCallback {

	/**
	 * 处理一条话单记录。
	 * 
	 * @param records
	 *            保存一条记录的键值对
	 * @param owner
	 *            当前记录的宿主名
	 * @param id
	 *            当前记录的标识
	 * @param omcId
	 *            OMC_ID
	 * @param stampTime
	 *            STAMPTIME
	 * @param taskID
	 *            任务编号
	 * @see EvdoRecordPair
	 */
	void handleData(List<EvdoRecordPair> records, String owner, String id, String omcId, String stampTime, long taskID);
}
