package task;

/**
 * 采集路径忽略信息。
 * 
 * @author ChenSijiang 2010-10-27
 * @since 1.1
 */
public interface IgnoresInfo {

	/**
	 * 获取关联到的任务ID
	 * 
	 * @return 关联到的任务ID
	 */
	long getTaskId();

	/**
	 * 获取忽略的采集路径
	 * 
	 * @return 忽略的采集路径
	 */
	String getPath();

	/**
	 * 判断此条忽略信息是否生效
	 * 
	 * @return 此条忽略信息是否生效
	 */
	boolean isUsed();

	/**
	 * 使此条忽略信息变为不生效，并立即持久化。
	 */
	void setNotUsed();
}
