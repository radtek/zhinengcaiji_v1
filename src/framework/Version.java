package framework;

import util.Util;

/**
 * 版本控制 类
 * 
 * @author YangJian
 * @version 1.0
 */
public class Version {

	/** 期望的版本号，每次发布版本都必须对此版本号进行修改 */
	/**
	 * ver:1.2.2.5 路测CSC表网元关联删除SID，NID两个字段
	 * ver:1.2.2.6 bsa日志采集调整
	 * ver:1.2.3.0 禅道IGP_V1.2.3.0
	 * ver:1.2.4.0 爱立信参数节点无数据问题
	 * ver:1.2.5.0 华为文件接口采集，偶尔出现个别任务停止采集的情况
	 * ver:1.2.6.0 爱立信性能文件解析报错,主要是文件中解析出来的数据在数据库中匹配不到；
	 * 		更改文件采集卡死的设置，如果没有<ftpSingleFileTimeOut>节点或者值小于等于0则表示不启动超时处理
	 * ver:1.2.6.1针对ManyFilesAccessor，有两处修改。限制了文件名最长为255个字符、修改了重试下载的跳出逻辑。
	 * ver:1.2.6.2 DBAutoParser2 的parseMeta方法增加了源字段跟目的字段的比较
	 * ver:1.2.6.3 DBAutoParser2 添加了日志，SqlldrStore添加调用sqlldr返回128的处理；
	 * ver:1.2.6.4 爱立信性能的求平均值与最大值逻辑修改,sqlldr入库时返回128的处理
	 * ver:1.2.6.5 放开sqlldr返回128的重试次数,解决LoadConfMapDevToNe网元重复加载导致内存溢出的问题
	 * 	SqlLdrLogAnalyzer.loadTemplet方法configMap创建次序;
	 */
	private static final String expectedVersion = "1.2.6.4";

	private static Version instance;

	private Version() {
		super();
	}

	public static synchronized Version getInstance() {
		if (instance == null) {
			instance = new Version();
		}

		return instance;
	}

	/**
	 * 检查程序版本号是否正确,检查方法:与 config.xml 文件中的config.system.version.edition项 匹配，如果不相等，则程序不允许运行
	 */
	public boolean isRightVersion() {
		boolean bReturn = true;

		String version = SystemConfig.getInstance().getEdition();
		if (Util.isNotNull(version)) {
			bReturn = expectedVersion.equals(version);
		}

		return bReturn;
	}

	/** 返回程序内部构建的版本号 */
	public String getExpectedVersion() {
		return expectedVersion;
	}

	// 单元测试
	public static void main(String[] args) {
		System.out.println(Version.getInstance().isRightVersion());
	}

}
