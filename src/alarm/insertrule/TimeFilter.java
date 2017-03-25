package alarm.insertrule;

import java.text.SimpleDateFormat;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.log4j.Logger;

import util.CommonDB;
import util.LogMgr;
import alarm.Alarm;
import alarm.RuleFilter;

/**
 * 时间过滤器,对在一定时间间隔内的数据进行过滤
 * 
 * @author ltp Apr 22, 2010
 * @since 3.0
 */
public class TimeFilter implements RuleFilter {

	private String timeSql = "SELECT * FROM IGP_DATA_ALARM WHERE OCCUREDTIME>to_date('%s','YYYY-MM-DD HH24:MI:SS') AND TASKID=%s";

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public TimeFilter() {
	}

	@Override
	public boolean doFilter(Alarm alarm) {
		boolean flag = true;
		// 查看数据库在十分钟内是否已经存在类似记录
		String sql = String.format(timeSql, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(alarm.getOccuredTime().getTime() - 10 * 60 * 1000),
				alarm.getTaskID());
		Result result = null;
		try {
			result = CommonDB.queryForResult(sql);
		} catch (Exception e) {
			log.error("时间过滤时发生错误！", e);
		}
		// 如果有类似的数据就不用加，所以就返回false
		if (result != null && result.getRowCount() > 0) {
			flag = false;
		}
		return flag;
	}

}
