package access;

import parser.hw.am.w.StreamAlarmParser;
import util.Parsecmd;
import util.Util;
import collect.SocketCollect;
import framework.ConstDef;

/**
 * 联通一期华为M2000北向告警字符流
 * 
 * @author liuwx 2010-4-29
 * @since 2.0
 */
public class SocketAccessor extends AbstractAccessor {

	@Override
	public boolean access() throws Exception {
		log.debug(name + ": 开始通过Socket连接方式处理M2000北向告警字符流.");
		if (taskInfo.getParserID() == ConstDef.WCDMA_HW_M2000_ALARM_STREAM_PARSER) {
			/* W网华为M2000告警采集，实时socket方式采集。 */
			Thread th = new Thread(new StreamAlarmParser(taskInfo, name), "wcdma_m2000_stream_collector");
			th.start();
		} else {
			SocketCollect socket = new SocketCollect(taskInfo);
			socket.start();
		}
		return true;
	}

	@Override
	public void configure() throws Exception {

	}

	@Override
	public boolean doAfterAccess() throws Exception {
		// 采集之后执行的Shell命令
		String strShellCmdFinish = taskInfo.getShellCmdFinish();
		if (Util.isNotNull(strShellCmdFinish)) {
			Parsecmd.ExecShellCmdByFtp(strShellCmdFinish, taskInfo.getLastCollectTime());
		}

		return true;
	}

}
