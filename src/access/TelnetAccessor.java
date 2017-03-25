package access;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parser.LineParser;
import parser.SmartLucTelnetParser;
import parser.hw.am.CV1LogTelnetAlarm;
import parser.lucent.CV1ASCII;
import util.CommonDB;
import util.Util;
import access.special.luc.LucTelnetCollect;
import access.special.luc.ShellCmdPrepare;
import collect.ProtocolTelnet;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * Telnet方式数据接入器
 * 
 * @author YangJian
 * @since 3.0
 */
public class TelnetAccessor extends AbstractAccessor {

	private boolean bRunFlag = true;

	public TelnetAccessor() {
		super();
	}

	protected void disposeParser() {
		if (parser != null) {
			if (parser instanceof SmartLucTelnetParser) {
				SmartLucTelnetParser sp = (SmartLucTelnetParser) parser;
				sp.dispose();
			}
		}
	}

	List<ShellCmdPrepare> cmds;

	@Override
	public boolean access() throws Exception {
		String logStr = null;

		boolean result = false;

		String strError;

		ProtocolTelnet telnet = new ProtocolTelnet();

		byte[] bufRecv = new byte[ConstDef.COLLECT_DATA_BUFF_SIZE];
		char[] charRecv = null;
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
				disposeParser();
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
					disposeParser();
					return result;
				}
				logStr = name + ": Proxy Telnet 登陆成功.";
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);
			}

			if (cmds == null) {
				cmds = ShellCmdPrepare.parseAll(taskInfo.getShellCmdPrepare());
			}

			// 采集之前是否需要执行SHELL脚本
			if (this.taskInfo.getCollectType() == 10) {
				boolean ok = false;
				if (cmds != null && !cmds.isEmpty()) {
					log.debug(name + " 开始执行luc脚本。");
					for (ShellCmdPrepare cmd : cmds) {
						log.debug(name + " 开始执行 - " + cmd.getRaw());
						if (!telnet.ExecuteShell(cmd.getRaw(), strHostSign, taskInfo.getShellTimeout())) {
							// 执行脚本出错
							strError = "error when execute script. " + cmd.getRaw();
							logStr = name + ": " + strError;
							log.error(logStr);
							taskInfo.log(DataLogInfo.STATUS_START, logStr);
							log.error(name + " 执行失败 - " + cmd.getRaw());
						} else {
							log.debug(name + " 执行成功 - " + cmd.getRaw());
							ok = true;
						}
						Thread.sleep(2000);
					}
					if (!ok) {
						// disposeParser();
						// return result;
					}
				}

			} else {
				String strShellCmdPrepare = taskInfo.getShellCmdPrepare();
				if (Util.isNotNull(strShellCmdPrepare)) {
					if (!telnet.ExecuteShell(strShellCmdPrepare, strHostSign, taskInfo.getShellTimeout())) {
						// 执行脚本出错
						strError = "error when execute script. " + strShellCmdPrepare;
						logStr = name + ": " + strError;
						log.error(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
						disposeParser();
						return result;
					}
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
				ShellCmdPrepare scp = null;
				if (cmds != null) {
					for (ShellCmdPrepare x : cmds) {
						if (x.getLog().equals(strTimeFile)) {
							scp = x;
							break;
						}
					}
					if (scp == null) {
						log.error(name + " 采集路径存在命令(ShellCmdPrepare)中不存在 - " + strTimeFile);
						continue;
					} else {
						String sctName = scp.getSct();
						String logName = scp.getLog();
						Map<Integer, String> sctMap = new HashMap<Integer, String>();
						boolean contain = false;
						synchronized (SmartLucTelnetParser.SCT_CACHE) {
							contain = false;// SmartLucTelnetParser.SCT_CACHE.containsKey(sctName);
						}
						if (contain) {
							synchronized (SmartLucTelnetParser.SCT_CACHE) {
								sctMap = SmartLucTelnetParser.SCT_CACHE.get(sctName);
							}
						} else {
							ProtocolTelnet pt = null;
							try {
								log.debug(name + " 开始获取sct文件 - " + sctName);
								pt = new ProtocolTelnet();
								boolean b = pt.Login(taskInfo.getDevInfo().getIP(), taskInfo.getDevPort(), taskInfo.getDevInfo().getHostUser(),
										taskInfo.getDevInfo().getHostPwd(), taskInfo.getDevInfo().getHostSign(), "ANSI", 30);
								if (b) {
									pt.sendCmd("cat " + sctName);
									Thread.sleep(2000);
									int ret = -1;
									byte[] buffer = new byte[4096];
									StringBuilder sb = new StringBuilder();
									while ((ret = pt.readData(buffer)) != -1) {
										sb.append(new String(buffer, 0, ret));
									}
									sctMap = LucTelnetCollect.parseSCT(sb.toString());
									synchronized (SmartLucTelnetParser.SCT_CACHE) {
										SmartLucTelnetParser.SCT_CACHE.put(sctName, sctMap);
									}
								} else {
									log.warn(name + " 获取sct文件时，telnet登录失败 - " + sctName);
								}
							} catch (Exception e) {
								log.error(name + " 获取sct内容时出现异常 - " + sctName, e);
							} finally {
								pt.Close();
							}
						}

						if (parser != null && parser instanceof SmartLucTelnetParser) {
							((SmartLucTelnetParser) parser).setSctInfo(sctName, logName, sctMap);
						}
					}
				}

				logStr = name + ": 采集的文件:" + strTimeFile;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				// 查询文件是否存在
				boolean bFound = true;

				// 如果文件不存在
				if (!bFound) {
					logStr = name + ": " + strTimeFile + " 不存在.";
					log.info(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					disposeParser();
					continue;
				}

				telnet.Close();
				Thread.sleep(2000);
				if (!telnet.Login(strHostIP, nHostPort, strUser, strPassword, strHostSign, "ANSI", gatherTimeout)) {
					strError = String.format("Telnet 登陆失败. Host=%s Port=%d(%s)", strHostIP, nHostPort, des);
					logStr = name + ": " + strError;
					log.error(logStr);
					taskInfo.log(DataLogInfo.STATUS_START, logStr);
					disposeParser();
					return result;
				}

				// 判断操作系统，是不是solaris，否则认为是linux
				boolean isSunOs = true;
				log.debug("发送命令“uname”……");
				telnet.sendCmd("uname"); // 开始发送采集命令
				try {
					String recv = telnet.receiveUntilTimeout(strHostSign, 5);
					log.debug("uname返回内容为：" + recv);
					if (recv != null && !recv.toLowerCase().contains("sunos"))
						isSunOs = false;
				} catch (Exception e) {
					log.error("接收uname的响应失败。", e);
				}

				String strCmd = null;
				if (collectPeriod == ConstDef.COLLECT_PERIOD_FOREVER) {
					// 如果为一直采集，则要加f标志,否则不加f标记
					// strCmd = String.format("tail +%dcf %s", lastCollectPos, strTimeFile);
					strCmd = String.format(isSunOs ? "tail +%df %s" : "tail -fn +%d %s", lastCollectPos, strTimeFile);
				} else {
					// strCmd = String.format("tail +%dc %s", lastCollectPos, strTimeFile);
					strCmd = String.format(isSunOs ? "tail +%d %s" : "tail -n +%d %s", lastCollectPos, strTimeFile);
				}

				logStr = name + ": 开始发送命令:" + strCmd;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				boolean bFlag = telnet.sendCmd(strCmd); // 开始发送采集命令

				if (!bFlag) {
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
				char[] tmpRecv = null;
				int nRet = 0;
				int nRunCount = 0;
				// 前面TELNET初始化完成，循环接收文件
				while (bRunFlag) {
					nRunCount++;

					if (taskInfo.getParserID() == 2005) {
						tmpRecv = new char[ConstDef.COLLECT_DATA_BUFF_SIZE];
						nRet = telnet.readCharData(tmpRecv);
						if (nRet != -1) {
							charRecv = new char[nRet];
							System.arraycopy(tmpRecv, 0, charRecv, 0, nRet);
						}
					} else
						nRet = telnet.readData(bufRecv);

					// 未到达文件的末尾
					if (nRet != -1) {
						if (taskInfo.getParserID() == 1 || taskInfo.getParserID() == 10) {
							parse(Util.bytesToChars(bufRecv), nRet);
						} else if (taskInfo.getParserID() == 2005) {// 华为告警日志文件Telnet方式解析
																	// ((CV1LogTelnetAlarm)
																	// parser).process(bufRecv,
																	// nRet);
							((CV1LogTelnetAlarm) parser).process(charRecv, nRet);
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

						if (taskInfo.getParserID() == 1 || taskInfo.getParserID() == 10)// add
						{
							parse(buf.toString().toCharArray(), buf.toString().length());

						} else if (taskInfo.getParserID() == 2005) {// 华为告警日志文件Telnet方式解析
																	// ((CV1LogTelnetAlarm)
																	// parser).process(bufRecv,
																	// nRet);
																	// ((CV1LogTelnetAlarm)
																	// parser).insertLogClt();
																	// ((CV1LogTelnetAlarm)
																	// parser).dispose();
							((CV1LogTelnetAlarm) parser).process(charRecv, nRet);
							((CV1LogTelnetAlarm) parser).insertLogClt();
							((CV1LogTelnetAlarm) parser).dispose();
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
			disposeParser();
		}
		disposeParser();
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
