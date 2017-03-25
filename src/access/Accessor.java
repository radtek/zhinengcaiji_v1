package access;

/**
 * 接入器 接口
 * 
 * @author YangJian
 * @since 3.0
 */
public interface Accessor {

	/**
	 * 验证方法
	 * 
	 * @return
	 */
	public boolean validate();

	/**
	 * 接入数据前的准备
	 * 
	 * @throws Exception
	 */
	public void doReady() throws Exception;

	/**
	 * 数据接入开始时
	 * 
	 * @throws Exception
	 */
	public void doStart() throws Exception;

	/**
	 * 数据接入前触发执行
	 * 
	 * @throws Exception
	 */
	public boolean doBeforeAccess() throws Exception;

	/**
	 * 数据接入
	 * 
	 * @throws Exception
	 */
	public boolean access() throws Exception;

	/**
	 * 解析数据
	 * 
	 * @throws Exception
	 */
	public void parse(char[] chData, int iLen) throws Exception;

	/**
	 * 数据接入后触发此事件
	 * 
	 * @throws Exception
	 */
	public boolean doAfterAccess() throws Exception;

	/**
	 * 数据接入完成后触发此事件
	 * 
	 * @throws Exception
	 */
	public void doFinishedAccess() throws Exception;

	/**
	 * 执行SQLLOAD操作
	 * 
	 * @throws Exception
	 */
	public void doSqlLoad() throws Exception;

	/**
	 * 接入任务完成
	 * 
	 * @throws Exception
	 */
	public void doFinished() throws Exception;
}
