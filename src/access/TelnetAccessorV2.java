package access;

import java.net.SocketTimeoutException;
import java.util.List;

import parser.AbstractStreamParser;
import parser.Parser;
import util.Util;
import access.special.luc.ShellCmdPrepare;
import collect.ProtocolTelnet;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.Factory;

/**
 * telnet连接方式，其是利用telnet远程登陆，再利用 tail命令 TelnetAccessorV2
 * 
 * @author liangww 2012-6-6
 */
public class TelnetAccessorV2 extends AbstractAccessor {

	private AbstractStreamParser parser2 = null;

	public TelnetAccessorV2() {
		super();
	}

	protected void disposeParser() {
		parser2.dispose();
	}

	List<ShellCmdPrepare> cmds;

	@Override
	public boolean validate() {
		// TODO Auto-generated method stub
		if (!super.validate()) {
			return false;
		}

		// liangww add 2012-04-27 初始化name
		this.name = this.taskInfo.getFullName();

		// 检测parser
		Parser tmpParser = Factory.createParser(this.taskInfo);
		if (tmpParser == null || !(tmpParser instanceof AbstractStreamParser)) {
			log.error("taskId-" + taskInfo.getTaskID() + ":不是有效任务，原因，parser配置不对");
			return false;
		}

		//
		parser2 = (AbstractStreamParser) tmpParser;

		return true;
	}

	/**
	 * 登陆
	 * 
	 * @param telnet
	 * @return
	 */
	private boolean login(ProtocolTelnet telnet) {
		String logStr = name + ": 准备 Telnet 登陆.";
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_START, logStr);

		String strHostIP = taskInfo.getDevInfo().getIP();
		int nHostPort = taskInfo.getDevPort();
		String strUser = taskInfo.getDevInfo().getHostUser();
		String strPassword = taskInfo.getDevInfo().getHostPwd();
		String strHostSign = taskInfo.getDevInfo().getHostSign();
		int gatherTimeout = taskInfo.getCollectTimeOut();
		String des = taskInfo.getDescribe();

		if (!telnet.Login(strHostIP, nHostPort, strUser, strPassword, strHostSign, "ANSI", gatherTimeout)) {
			String strError = String.format("Telnet 登陆失败. Host=%s Port=%d(%s)", strHostIP, nHostPort, des);
			logStr = name + ": " + strError;
			log.error(logStr);
			taskInfo.log(DataLogInfo.STATUS_START, logStr);
			disposeParser();
			return false;
		}

		logStr = name + "Telnet 登陆成功.";
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_START, logStr);
		return true;
	}

	boolean runShellCmd(ProtocolTelnet telnet) {
		String strHostSign = taskInfo.getDevInfo().getHostSign();

		if (cmds == null) {
			cmds = ShellCmdPrepare.parseAll(taskInfo.getShellCmdPrepare());
		}

		String strShellCmdPrepare = taskInfo.getShellCmdPrepare();
		if (Util.isNotNull(strShellCmdPrepare)) {
			if (!telnet.ExecuteShell(strShellCmdPrepare, strHostSign, taskInfo.getShellTimeout())) {
				// 执行脚本出错
				String strError = "error when execute script. " + strShellCmdPrepare;
				String logStr = name + ": " + strError;
				log.error(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				disposeParser();
				return false;
			}
		}

		return true;
	}

	static String getCmd(int collectPeriod) {
		// 如果为一直采集，则要加f标志,否则不加f标记
		// return collectPeriod == ConstDef.COLLECT_PERIOD_FOREVER ?
		// "tail +%dcf %s" : "tail +%dc %s";
		// 要用一直采集的方式，利用超时认为文件结束了---liangww add 2012-09-27
		return "tail +%dcf %s";
	}

	@Override
	public boolean access() throws Exception {
		String logStr = null;
		boolean result = false;
		ProtocolTelnet telnet = new ProtocolTelnet();
		int collectPeriod = taskInfo.getPeriod();
		int lastCollectPos = taskInfo.get_LastCollectPos();

		try {
			// 如果登陆失败
			if (!login(telnet) || !runShellCmd(telnet)) {
				return false;
			}

			String[] strNeedGatherFileNames = getDataSourceConfig().getDatas();
			if (strNeedGatherFileNames.length == 0) {
				log.warn("数据源路径，路径条数为0");
			}

			for (String strTimeFile : strNeedGatherFileNames) {
				strTimeFile = ConstDef.ParseFilePath(strTimeFile, this.taskInfo.getLastCollectTime());
				logStr = name + ": 采集的文件:" + strTimeFile;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				// 获取telnet命令
				String strCmd = String.format(getCmd(collectPeriod), lastCollectPos, strTimeFile);
				logStr = name + ": 开始发送命令:" + strCmd;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				// 开始发送采集命令
				if (!telnet.sendCmd(strCmd)) {
					logStr = name + ": 命令发送失败,任务将结束. " + strCmd;
					log.error(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					disposeParser();
					return result;
				}

				logStr = name + ": 命令发送成功:" + strCmd;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				try {
					// 发送数据后，暂停二秒，等待数据完全返回
					Thread.sleep(2000);
				} catch (InterruptedException e) {
				}

				/**
				 * TODO 如果判何采集完！ 1.telnet.readCharData == -1 2.已经生成了下个采集文件 必须1 & 2 is true 另外还有个问题就是，补采问题（针对不是永久采集的） 目前只是简单 判断 1，另外补采问题未做
				 */
				// char[] tmpRecv = new char[1024];
				// int nRet = 0;
				// while (true)
				// {
				// nRet = telnet.readCharData(tmpRecv);
				// if(nRet == -1)
				// {
				// disposeParser();
				// break;
				// }
				//
				// parser2.parse(new
				// ByteArrayInputStream(String.valueOf(tmpRecv, 0,
				// nRet).getBytes()), null);
				// }
				// liangww modify 2012-09-26 修改为直接获取telnet的输入流操作
				try {
					parser2.parse(telnet.getInpustream(), null);
				} catch (SocketTimeoutException e) {
					log.debug(name + "-tail 文件：" + strTimeFile + " SocketTimeoutException");
					logStr = name + ": Telnet: read data ok.";
					log.debug(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					result = true;
				} catch (Exception e) {
					log.warn(name + "-tail 文件：" + strTimeFile + "异常", e);
				}
			}
		} catch (Exception e) {
			result = false;
			logStr = name + ": Telnet采集异常. Cause:";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
		} finally {
			telnet.Close();
			disposeParser();
		}

		return result;
	}

	@Override
	public void configure() throws Exception {

	}

}
