package util.loganalyzer;

import java.io.InputStream;

import util.SqlldrResult;

/**
 * LogAnalyzer 用于分析入库的结果
 * 
 * @author Liuwx
 * @since 1.0
 */
public interface LogAnalyzer {

	/**
	 * 初始化
	 * 
	 * @throws LogAnalyzerException
	 */
	public void init() throws LogAnalyzerException;

	/**
	 * 分析日志文件
	 * 
	 * @param fileName
	 * @return
	 * @throws LogAnalyzerException
	 */
	public SqlldrResult analysis(String fileName) throws LogAnalyzerException;

	/**
	 * 分析日志文件
	 * 
	 * @param in
	 * @return
	 * @throws LogAnalyzerException
	 */
	public SqlldrResult analysis(InputStream in) throws LogAnalyzerException;

	/**
	 * 销毁
	 */
	public void destory();

}
