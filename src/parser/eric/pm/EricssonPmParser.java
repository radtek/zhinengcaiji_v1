package parser.eric.pm;

import java.sql.Timestamp;

/**
 * 爱立信原始COUNTER文件解析接口。
 * 
 * @author 陈思江 2010-3-1
 */
public interface EricssonPmParser {

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
	void parse(String file, int omcId, Timestamp stampTime, int taskID) throws EricssonPmParserException;
}
