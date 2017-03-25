package parser.lucent.evdo;

import java.io.InputStream;
import java.sql.Timestamp;

/**
 * <p>
 * EVDO话单解析接口。
 * </p>
 * <p>
 * 考虑到大的话单文件，这里使用回调的方式解析话单。<br />
 * 调用者实现EvdoParserCallback接口，此从中获取到当前解析到的数据。
 * </p>
 * 
 * @author ChenSijiang 2009.01.25
 * @since 1.0
 * @see EvdoParserCallback
 */
public interface EvdoParser {

	/**
	 * 设定被解析话单的输入流。
	 * 
	 * @param in
	 *            被解析话单的输入流
	 */
	void setInputStream(InputStream in);

	/**
	 * 获取被解析话单的输入流。
	 * 
	 * @return 被解析话单的输入流
	 */
	InputStream getInputStream();

	/**
	 * 开始解析话单，每解析到一条记录，就调用一次EvdoParserCallback的handleData方法，将数据信息传入。
	 * 
	 * @param callback
	 *            话单解析器的回调接口
	 * @param omcId
	 *            OMC_ID
	 * @param fileName
	 *            话单原始文件的文件名
	 * @param autoDispose
	 *            是否自动释放资源
	 * @param taskID
	 *            任务编号
	 * @throws EvdoParseException
	 *             抛出所有解析中产生的异常
	 * @see EvdoParserCallback
	 * @see #dispose()
	 */
	void parse(EvdoParserCallback callback, String omcId, Timestamp time, boolean autoDispose, long taskID) throws EvdoParseException;

	/**
	 * 释放资源。将话单文件的输入流关闭。
	 */
	void dispose();
}
