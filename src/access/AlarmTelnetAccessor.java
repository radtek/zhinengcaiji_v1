package access;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import parser.LineParser;
import parser.lucent.CV1ASCII;
import util.CommonDB;
import util.Util;
import collect.ProtocolTelnet;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * HW alarm Telnet方式数据接入器
 */
public class AlarmTelnetAccessor extends AbstractAccessor {

	private boolean bRunFlag = true;

	public AlarmTelnetAccessor() {
		super();
	}

	@Override
	public boolean access() throws Exception {
		String logStr = null;

		boolean result = false;

		String strError;

		ProtocolTelnet telnet = new ProtocolTelnet();

		byte[] bufRecv = new byte[ConstDef.COLLECT_DATA_BUFF_SIZE];

		long taskID = this.getTaskID();
		String des = taskInfo.getDescribe();
		int gatherTimeout = taskInfo.getCollectTimeOut();
		int collectPeriod = taskInfo.getPeriod();
		Timestamp lastGatherTime = taskInfo.getLastCollectTime();
		int lastCollectPos = taskInfo.get_LastCollectPos();

		logStr = name + ": 准备 Telnet 登陆.";
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_START, logStr);

		String strHostIP = taskInfo.getDevInfo().getIP();
		int nHostPort = taskInfo.getDevPort();
		String strUser = taskInfo.getDevInfo().getHostUser();
		String strPassword = taskInfo.getDevInfo().getHostPwd();
		String strHostSign = taskInfo.getDevInfo().getHostSign();

		try {
			if (!telnet.Login(strHostIP, nHostPort, strUser, strPassword, strHostSign, "ANSI", gatherTimeout)) {
				strError = String.format("Telnet 登陆失败. Host=%s Port=%d(%s)", strHostIP, nHostPort, des);
				logStr = name + ": " + strError;
				log.error(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				return result;
			}
			logStr = name + "Telnet 登陆成功.";
			log.debug(logStr);
			taskInfo.log(DataLogInfo.STATUS_START, logStr);
			// 判断是否需要进行中转连接,判断依据为代理设置为空
			String strProxyHostIP = taskInfo.getProxyDevInfo().getIP();
			if (Util.isNotNull(strProxyHostIP)) {
				strHostSign = taskInfo.getProxyDevInfo().getHostSign();

				int nProxyPort = taskInfo.getProxyDevPort();
				String strProxyUser = taskInfo.getProxyDevInfo().getHostUser();
				String strProxyPassword = taskInfo.getProxyDevInfo().getHostPwd();

				if (!telnet.ProxyLogin(strProxyHostIP, nProxyPort, strProxyUser, strProxyPassword, strHostSign, "ANSI")) {
					strError = String.format("Proxy Telnet 登陆失败: Host=%s Port=%d(%s)", strProxyHostIP, nProxyPort, des);
					logStr = name + ": " + strError;
					log.error(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					return result;
				}
				logStr = name + ": Proxy Telnet 登陆成功.";
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
			}

			// 采集之前是否需要执行SHELL脚本
			String strShellCmdPrepare = taskInfo.getShellCmdPrepare();
			if (Util.isNotNull(strShellCmdPrepare)) {
				if (!telnet.ExecuteShell(strShellCmdPrepare, strHostSign, taskInfo.getShellTimeout())) {
					// 执行脚本出错
					strError = "error when execute script. " + strShellCmdPrepare;
					logStr = name + ": " + strError;
					log.error(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					return result;
				}
			}

			String strTimeFile1 = ConstDef.ParseFilePath(taskInfo.getCollectPath(), lastGatherTime);
			// 需要采集的文件,多个文件以;间隔
			String[] strNeedGatherFileNames = null;// this.getDataSourceConfig().getDatas();

			Set<String> list = new HashSet<String>();// add
			String[] filepaths = strTimeFile1.split(";");
			if (filepaths != null) {
				for (String f : filepaths) {
					if (Util.isNull(f))
						continue;
					list.add(f);
				}
			}
			if (list.size() == 0) {
				log.warn("数据源路径，路径条数为0");
			}

			strNeedGatherFileNames = (String[]) list.toArray(new String[0]);

			if (strNeedGatherFileNames == null) {
				log.debug(name + "请检查数据源");
			}
			for (String strTimeFile : strNeedGatherFileNames) {

				logStr = name + ": 采集的文件:" + strTimeFile;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				// 查询文件是否存在
				boolean bFound = telnet.findFile(strTimeFile, taskInfo.getDevInfo().getHostSign(), gatherTimeout);

				// 如果文件不存在
				if (!bFound) {
					logStr = name + ": " + strTimeFile + " 不存在.";
					log.info(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					return result;
				}

				String strCmd = null;
				if (collectPeriod == ConstDef.COLLECT_PERIOD_FOREVER) {
					// 如果为一直采集，则要加f标志,否则不加f标记
					strCmd = String.format("tail +%dcf %s", lastCollectPos, strTimeFile);
				} else {
					strCmd = String.format("tail +%dc %s", lastCollectPos, strTimeFile);
				}

				logStr = name + ": 开始发送命令:" + strCmd;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				boolean bFlag = telnet.sendCmd(strCmd); // 开始发送采集命令

				if (!bFlag) {
					logStr = name + ": 命令发送失败,任务将结束. " + strCmd;
					log.error(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
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

				BufferedWriter bw = null;
				File file = null;

				if (taskInfo.getParserID() == 11) {
					// 创建本地文件路径
					String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
					String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

					String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strTimeFile);

					int index = strTimeFile.lastIndexOf("/");
					String fname = null;
					if (index != -1) {
						fname = strTimeFile.substring(index + 1);
					} else {
						fname = strTimeFile;
					}

					file = new File(strTempPath + File.separator + fname);

					log.debug(name + "本地文件名" + file.getAbsolutePath());

					if (file.exists()) {
						file.delete();
					} else
						file.createNewFile();

					parser.setFileName(file.getAbsolutePath());

					bw = new BufferedWriter(new FileWriter(file, true));
				}

				int nRet = 0;
				int nRunCount = 0;
				// 前面TELNET初始化完成，循环接收文件
				while (bRunFlag) {
					nRunCount++;

					nRet = telnet.readData(bufRecv);

					// 未到达文件的末尾
					if (nRet != -1) {
						if (taskInfo.getParserID() == 1) {
							parse(Util.bytesToChars(bufRecv), nRet);
						} else {
							if (bw != null) {
								((CV1ASCII) parser).buildAlData(Util.bytesToChars(bufRecv), nRet, bw);
							}
						}

						// 更新最后采集位置
						taskInfo.setCollectTimePos(nRet);
						if ((nRunCount % 50) == 0)
							CommonDB.LastImportTimePos(taskID, lastGatherTime, taskInfo.get_LastCollectPos());
					}
					// 到达文件的末尾
					else {
						StringBuffer buf = new StringBuffer();
						buf.append("\n**FILEEND**\n");

						if (taskInfo.getParserID() == 1)// add
						{
							parse(buf.toString().toCharArray(), buf.toString().length());

						} else {
							if (bw != null) {

								((CV1ASCII) parser).buildAlData(buf.toString().toCharArray(), buf.toString().length(), bw);
							}

						}
						logStr = name + ": " + des + ":" + lastGatherTime + " import finish.";
						log.debug(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);

						break;
					}
				}
				// 通过telnet方式将文件些到本地后对文件进行解析 ,解析朗讯性能的数据
				if (bw != null) {
					bw.flush();
					bw.close();
				}
				if (taskInfo.getParserID() == 11) {
					((CV1ASCII) parser).parseData();
				}

				logStr = name + ": Telnet: read data ok.";
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
				result = true;
			}
		} catch (Exception e) {
			result = false;
			logStr = name + ": Telnet采集异常. Cause:";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
		} finally {
			telnet.Close();
		}

		return result;
	}

	@Override
	public void configure() throws Exception {

	}

	/**
	 * 解析数据
	 */
	@Override
	public void parse(char[] chData, int len) throws Exception {
		// 解析数据,这里定死了只能 按行解析
		((LineParser) parser).BuildData(chData, len);
	}

}
