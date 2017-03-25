package parser.eric.cm;

import java.sql.Timestamp;

/**
 * 爱立信参数解析
 * 
 * @author chensj 2010-3-5
 */
public interface EricssonCmParser {

	/**
	 * 解析并入库一个文件。
	 * 
	 * @param file
	 *            要解析的文件的绝对或相对路径
	 * @param omcId
	 *            OMC_ID
	 * @param stampTime
	 *            数据时间
	 * @param taskID
	 *            任务编号
	 */
	void parse(String file, int omcId, Timestamp stampTime, long taskID) throws Exception;
}
