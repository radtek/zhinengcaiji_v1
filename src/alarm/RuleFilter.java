package alarm;

/**
 * 对数据进行的某些规则过滤(主要是对比数据库是否已存在类似的数据)
 * 
 * @author ltp Apr 21, 2010
 * @since 3.0
 */
public interface RuleFilter {

	/**
	 * 如果过滤成功就添加
	 * 
	 * @return boolean true表示可以添加, false表示不能添加
	 */
	public boolean doFilter(Alarm alarm);
}
