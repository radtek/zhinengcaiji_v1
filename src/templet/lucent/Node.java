package templet.lucent;

import util.LineReader;
import distributor.Distribute;

/**
 * 结点，其中它有两个实现{@link FieldGroup}， {@link SmartTemplet}
 * 
 * @author liangww
 * @version 1.0.0 1.0.1 liangww 2012-07-17 增加setLogKey方法<br>
 * 
 * @create 2012-7-17 下午12:04:35
 * @see FieldGroup
 * @see SmartTemplet
 */
public interface Node {

	/**
	 * 设置父模板
	 * 
	 * @param templet
	 */
	public void setPTemlple(SmartTemplet templet);

	/**
	 * 是否公共的
	 * 
	 * @return
	 */
	public boolean isPublic();

	/**
	 * 获取域对应的值
	 * 
	 * @param recordIndex
	 *            记录索引
	 * @param fieldIndex
	 *            域索引
	 * @return
	 */
	public String getFieldValue(int recordIndex, int fieldIndex);

	/**
	 * 
	 * @param lineReader
	 * @param distribute
	 * @throws Exception
	 */
	public void parse(LineReader lineReader, Distribute distribute) throws Exception;

	/**
	 * 能否解析
	 * 
	 * @param line
	 * @return
	 */
	public boolean canParse(String line);

	/**
	 * 获取记录个数
	 * 
	 * @return
	 */
	public int getRecordNum();

	/**
	 * 清除记录
	 */
	public void clearRecords();

	/**
	 * 设置日志关键字
	 * 
	 * @param logKey
	 */
	public void setLogKey(String logKey);
}
