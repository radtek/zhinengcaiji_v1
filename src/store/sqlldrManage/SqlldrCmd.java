package store.sqlldrManage;

import java.util.Date;

import org.apache.log4j.Logger;

import store.sqlldrManage.bean.SqlldrPro;
import store.sqlldrManage.impl.SqlldrPool;
import util.ExternalCmd;
import util.LogMgr;
import util.Util;

/**
 * @author yuy sqlldr命令执行 继承自ExternalCmd
 */
public class SqlldrCmd extends ExternalCmd {

	public AbstractPool sqlldrPool = SqlldrPool.getInstance();

	public Process pro = null;

	public SqlldrPro sqlldrPro = null;

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	protected static int sleepTime = 2000;// 如果竞争不到sqlldr进程，休眠时间，单位毫秒

	/** 执行命令 */
	public int execute() throws Exception {
		if (Util.isNull(_cmd))
			return 0;

		int retCode = -1;

		try {
			// 1.执行之前，要判断是否可以竞争到本地sqlldr进程
			beforeExec();
//			ProcessBuilder pb = new ProcessBuilder(_cmd);
//			pb.redirectErrorStream(true);
//			pro = pb.start();
			pro = Runtime.getRuntime().exec(_cmd);
			// 2.开启守护线程监听
			afterExec();
			new StreamGobbler(pro.getErrorStream()).start();
			new StreamGobbler(pro.getInputStream()).start();
			pro.waitFor();
			// 3.扫尾（归还）
			finish();
			retCode = pro.exitValue();
		} catch (Exception e) {
			throw e;
		} finally {
			if (pro != null)
				pro.destroy();
		}

		return retCode;
	}

	/**
	 * 竞争本地sqlldr进程
	 * 
	 * @param proc
	 */
	public void beforeExec() {
		sqlldrPro = new SqlldrPro();
		sqlldrPro.setStartTime(new Date());
		sqlldrPro.setCmd(_cmd);
		sqlldrPro.setTaskID(taskID);

		while (!sqlldrPool.applyObj(sqlldrPro)) {
			log.debug("任务id：" + taskID + ",当前调用sqlldr进程数已满（限额" + sqlldrPool.getMaxCount() + "个），" + sleepTime / 1000 + "秒后重试");
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				log.error("任务id：" + taskID + ",休眠线程被打断", e);
				break;
			}
		}
		log.debug("任务id：" + taskID + ",竞争到sqlldr进程，当前调用sqlldr进程总数为" + sqlldrPool.getCurrCount());
	}

	/**
	 * 开启监听
	 */
	public void afterExec() {
		sqlldrPro.setPro(pro);
		sqlldrPool.startListening();
	}

	/**
	 * 扫尾 回收
	 */
	public void finish() {
		sqlldrPool.returnObj(sqlldrPro);
		long time = new Date().getTime()-sqlldrPro.getStartTime().getTime();
		log.debug("任务:("+sqlldrPro.getTaskID()+") sqlldr完毕,运行时间为:"+time+"ms,状态:"+sqlldrPro.getStatus());
	}
}