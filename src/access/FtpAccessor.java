package access;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

import parser.FtpDeleteFileParser;
import parser.hw.dt.CdmaScanParser;
import parser.nbi.check.GroupFilesCheck;
import parser.others.gpslog.CV1BinMgr;
import task.IgnoresInfo;
import task.IgnoresMgr;
import util.DeCompression;
import util.ExcelToCsvUtil;
import util.LogMgr;
import util.Parsecmd;
import util.SPASLogger;
import util.Util;
import access.special.AlcatelLucentWcdmaPerformanceAccessor;
import access.special.CDMDDingliDTParser;
import access.special.EricssonWcdmaPerformanceAccessor;
import access.special.HuaweiWcdmaXMLPerformanceAccessor;
import alarm.AlarmMgr;
import collect.DownStructer;
import collect.FTPConfig;
import collect.FTPTool;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.DataLifecycleMgr;
import framework.SystemConfig;

/**
 * Ftp方式数据接入器
 * 
 * @author YangJian
 * @since 3.0
 */
public class FtpAccessor extends AbstractAccessor {

	/** 登陆FTP失败重试最大次数 */
	private static final byte MAX_TRY_TIMES = 5;

	private IgnoresMgr ignoresMgr = IgnoresMgr.getInstance();

	public FtpAccessor() {
		super();
	}

	@Override
	public boolean access() throws Exception {
		boolean bSucceed = false;

		// W网爱立信性能，特殊处理，并发解析。
		if (taskInfo.getParserID() == 18)
			return new EricssonWcdmaPerformanceAccessor(taskInfo).handle();

		// W网华为XML性能，特殊处理，并发解析。
		if (taskInfo.getParserID() == 2003)
			return new HuaweiWcdmaXMLPerformanceAccessor(taskInfo).handle();

		// W网阿朗XML性能，特殊处理，并发解析。
		if (taskInfo.getParserID() == 4001)
			return new AlcatelLucentWcdmaPerformanceAccessor(taskInfo).handle();

		// CDMA鼎立路测。
		if (taskInfo.getParserID() == 9003)
			return new CDMDDingliDTParser(taskInfo).parse();

		// 定位信息特殊处理
		if (taskInfo.getParserID() == 24 && FTPConfig.getFTPConfig(taskInfo.getTaskID()) != null)
			return new CV1BinMgr(taskInfo).parse();

		// 集团上报文件核查
		if (taskInfo.getParserID() == 25)
			return new GroupFilesCheck(taskInfo).parse();

		// 用户手工导入路测文件扫描解析
		if (taskInfo.getParserID() == 26)
			return new CdmaScanParser(taskInfo).parse();

		if (taskInfo.getParserID() == 9013)
			return new FtpDeleteFileParser(taskInfo).deleteFiles();

		long taskID = this.getTaskID();
		FTPTool ftp = new FTPTool(taskInfo);
		String logStr = name + ": 开始FTP登陆.";
		LogMgr.getInstance()
				.getSPASLogger()
				.log(SPASLogger.APP_TYPE_IGP, taskInfo.getTaskID(), 0, 0, 0, taskInfo.getLastCollectTime(), SPASLogger.CLT_STATE_CONNECTING,
						"正在连接到FTP.", taskInfo);
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_START, logStr);
		try {
			boolean bOK = ftp.login(30000, MAX_TRY_TIMES);
			if (!bOK) {
				LogMgr.getInstance()
						.getSPASLogger()
						.log(SPASLogger.APP_TYPE_IGP, taskInfo.getTaskID(), 0, 0, 0, taskInfo.getLastCollectTime(),
								SPASLogger.CLT_STATE_CONNECT_FAIL, "连接到FTP失败。", taskInfo);
				logStr = name + ": FTP多次尝试登陆失败:" + ftp;
				log.error(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				// 多次登陆失败后则加入补采表
				task.TaskMgr.getInstance().newRegather(taskInfo, "", "多次登陆失败，全部补采");

				// 通知告警
				AlarmMgr.getInstance().insert(taskID, "FTP多次尝试登陆失败", ftp.toString(), name, 1500);
				return false;
			}
			logStr = name + ": FTP登陆成功.";
			log.debug(name + ": FTP登陆成功.");
			taskInfo.log(DataLogInfo.STATUS_START, logStr);

			int parseType = taskInfo.getParseTmpType();

			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

			// 需要采集的文件,多个文件以;间隔
			String[] strNeedGatherFileNames = this.getDataSourceConfig().getDatas();

			// liangww modify 2012-12-18 增加list，用于保证其path的顺序
			Set<String> set = new HashSet<String>();
			List<String> list = new ArrayList<String>();

			for (String s : strNeedGatherFileNames) {
				try {
					String p = ConstDef.ParseFilePath(s, taskInfo.getLastCollectTime());
					List<String> subPaths = Util.listFTPDirs(p, taskInfo.getDevInfo().getIP(), taskInfo.getDevPort(), taskInfo.getDevInfo()
							.getHostUser(), taskInfo.getDevInfo().getHostPwd(), taskInfo.getDevInfo().getEncode(), taskInfo.getParserID());

					for (String path : subPaths) {
						if (!set.contains(path)) {
							set.add(path);
							list.add(path);
						}
					}

				} catch (Exception e) {
					log.error("展开目录通配符时异常");
					throw e;
				}
			}
			if (list.size() == 0) {
				log.warn("展开目录通配符后，路径条数为0");
			}

			strNeedGatherFileNames = (String[]) list.toArray(new String[0]);

			// G网重庆工单采集，必须先下载xls文件，再下载XML文件。
			if (taskInfo.getParserID() == 9002 && taskInfo.getCollectType() != 9 && strNeedGatherFileNames != null
					&& strNeedGatherFileNames.length > 1) {
				log.debug("G网重庆EOMS工单采集路径处理，当前路径 - " + Util.listStringArray(strNeedGatherFileNames, ','));
				String[] tmpArr = new String[strNeedGatherFileNames.length];

				List<String> tmpListXls = new ArrayList<String>();
				List<String> tmpListXml = new ArrayList<String>();
				for (String tmpCollectPath : strNeedGatherFileNames) {
					if (tmpCollectPath.toLowerCase().trim().endsWith(".xls") || tmpCollectPath.toLowerCase().trim().endsWith(".xlsx"))
						tmpListXls.add(tmpCollectPath);
					else
						tmpListXml.add(tmpCollectPath);
				}
				set.clear();
				list.clear();

				int index = 0;
				for (String tmpCollectPath : tmpListXls)
					tmpArr[index++] = tmpCollectPath;
				for (String tmpCollectPath : tmpListXml)
					tmpArr[index++] = tmpCollectPath;

				tmpListXls = null;
				tmpListXml = null;
				strNeedGatherFileNames = tmpArr;
				log.debug("G网重庆EOMS工单采集路径处理，处理后路径 - " + Util.listStringArray(strNeedGatherFileNames, ','));
			}

			// 解压文件并遍历文件夹，返回文件的路径
			Parsecmd parsecmd = new Parsecmd();
			long mrsumcount = 0;
			long localtimecount = 0;

			// 下载并解析文件内容
			for (String gatherFileName : strNeedGatherFileNames) {
				if (Util.isNull(gatherFileName))
					continue;

				String strSubFilePath = ConstDef.ParseFilePath(gatherFileName.trim(), taskInfo.getLastCollectTime());

				// 根据文件名建立目录
				String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strSubFilePath);

				// 以FTP方式下载文件到本地
				// String[] strFileNames = null;
				DownStructer dStruct = null;
				try {
					dStruct = ftp.downFile(strSubFilePath, strTempPath);
				} catch (Exception e) {
					// 加入补采表
					task.TaskMgr.getInstance().newRegather(taskInfo, gatherFileName, "文件下载失败，异常信息为:" + e.getMessage());
					// dbLogger.log(taskInfo.getDevInfo().getOmcID(),
					// t.getDestTableName(), taskInfo.getLastCollectTime(), -1,
					// taskInfo.getTaskID());
					continue;
				}
				// strFileNames=dStruct.getSuc().toArray(new String[0]);
				// 如果文件不存在，则加入补采表并跳过
				if (dStruct.getSuc().size() == 0) {
					IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), gatherFileName, taskInfo.getLastCollectTime());
					if (ignoresInfo == null) {
						// 加入补采表
						task.TaskMgr.getInstance().newRegather(taskInfo, gatherFileName, "文件不存在");
					} else {
						log.warn(name + " " + gatherFileName + "不存在,但igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),不加入补采表.");
					}
					// dbLogger.log(taskInfo.getDevInfo().getOmcID(),
					// t.getDestTableName(), taskInfo.getLastCollectTime(), -1,
					// taskInfo.getTaskID());
					continue;
				}
				if (dStruct.getFail().size() > 0) {
					for (String fFile : dStruct.getFail()) {
						// 加入补采表
						task.TaskMgr.getInstance().newRegather(taskInfo, fFile, "ftp文件长度为0");
						IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), fFile, taskInfo.getLastCollectTime());
						if (ignoresInfo != null) {
							log.warn(name + " " + fFile + ",  igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),但本次发现其存在,以后将不再忽略此路径.");
							ignoresInfo.setNotUsed();
						}
					}
				}

				List<String> arrfileList = new ArrayList<String>();

				for (String strFileName : dStruct.getSuc()) {
					if (Util.isNull(strFileName))
						continue;

					IgnoresInfo ignoresInfo = ignoresMgr.checkIgnore(taskInfo.getTaskID(), strFileName, taskInfo.getLastCollectTime());
					if (ignoresInfo != null) {
						log.warn(name + " " + strFileName + ",  igp_conf_ignores表中设置了忽略此路径(" + ignoresInfo + "),但本次发现其存在,以后将不再忽略此路径.");
						ignoresInfo.setNotUsed();
					}
					// 江苏南京省级边界协调单
					if (taskInfo.getParserID() == 9011) {
						int begin = strFileName.lastIndexOf("_") + 1;
						int end = strFileName.lastIndexOf(".");

						parser.setParentFileName(strFileName.substring(begin, end));
					}

					// end 协调单

					if (Util.isZipFile(strFileName)) {
						try {
							// 解压文件并遍历文件夹，返回文件的路径
							// 解压根据当前的时间，周期类型保存原始数据
							// liangww modify 2012-06-04 修改decompress做出相应调整
							List<String> tmpLst = DeCompression.decompress(taskID, taskInfo.getParseTemplet(), strFileName,
									taskInfo.getLastCollectTime(), taskInfo.getPeriod(), true);
							if (tmpLst != null)
								arrfileList.addAll(tmpLst);
							// log.debug(name + " 解压完成 - ");

							if (SystemConfig.getInstance().isSPAS()) {
								if (tmpLst != null) {
									for (String s : tmpLst) {
										String norFile1 = FilenameUtils.normalize(s);
										String norFile2 = FilenameUtils.normalize(strFileName);
										taskInfo.filenameMap.put(norFile1, norFile2);
										log.debug("putted, key=" + norFile1 + ", value=" + norFile2);
									}
								}
							}
						} catch (Exception e) {
							logStr = name + ": 文件解压失败 " + strFileName + " . 原因:";
							log.error(logStr, e);
							taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
							// 如果出现异常则进行补采
							task.TaskMgr.getInstance().newRegather(taskInfo, gatherFileName, "解压文件时异常,异常信息为:" + e.getMessage());
							continue;
						}
					} else {
						arrfileList.add(strFileName);
					}
				}

				// chensj 2010-12-16
				// 判断文件是不是excel文件，如果是的话，将excel中的每一个sheet转换成一个标准csv文件。
				List<String> xlsList = new ArrayList<String>();// 这里把每个excel文件名存起来，转换完后从arrfileList里删掉
				List<String> totalCsv = new ArrayList<String>();// 存放所有转换出来的csv，最后一起加进arrfileList
				for (String oneFile : arrfileList) {
					if ((oneFile.endsWith(".xls") || oneFile.endsWith(".xlsx")) && taskInfo.getParserID() != 9001 && taskInfo.getParserID() != 9002
							&& taskInfo.getParserID() != 9007 && taskInfo.getParserID() != 9011)// add
																								// on
																								// 2012-10-25
					{
						try {
							List<String> csvFiles = new ExcelToCsvUtil(oneFile, taskInfo).toCsv();
							xlsList.add(oneFile);
							totalCsv.addAll(csvFiles);

						} catch (Exception e) {
							log.error(name + " 转换excel时异常: " + oneFile, e);
						}
					}
				}
				for (String s : xlsList) {
					arrfileList.remove(s);
				}
				arrfileList.addAll(totalCsv);
				xlsList.clear();
				totalCsv.clear();
				xlsList = null;
				totalCsv = null;
				// chensj 2010-12-16 end.

				if (arrfileList == null || arrfileList.size() == 0)
					continue;

				// 给文件打时间戳
				for (int j = 0; j < arrfileList.size(); j++) {
					String strTempFileName = arrfileList.get(j);
					Date dataTime = taskInfo.getLastCollectTime();
					// 给文件打时间戳
					DataLifecycleMgr.getInstance().doFileTimestamp(strTempFileName, dataTime);
				}

				// ftp下载文件之后的Shell命令
				String strCmd = taskInfo.getShellCmdPrepare();
				if (Util.isNotNull(strCmd)) {
					boolean b = Parsecmd.ExecShellCmdByFtp1(strCmd, taskInfo.getLastCollectTime());
					if (!b) {
						logStr = name + ": ftp执行命令失败. " + strCmd;
						log.error(logStr);
						taskInfo.log(DataLogInfo.STATUS_START, logStr);
					}
				}

				logStr = name + ": 解析类型=" + parseType;
				log.debug(logStr);
				taskInfo.log(DataLogInfo.STATUS_START, logStr);

				parser.setDsConfigName(gatherFileName);

				// 读取文件内容
				for (int j = 0; j < arrfileList.size(); j++) {
					String strTempFileName = arrfileList.get(j);

					if (taskInfo.getParseTmpType() == ConstDef.COLLECT_TEMPLATE_GPS_ENSURE_POS && strTempFileName.endsWith(".fix")) {
						continue;
					}

					logStr = name + ": 当前要解析的文件为:" + strTempFileName;
					log.debug(logStr);
					taskInfo.log(DataLogInfo.STATUS_PARSE, logStr);
					parser.setFileName(strTempFileName);
					try {
						parser.parseData();
					} catch (Exception e) {
						// 解析失败不需要补采
						logStr = name + ": 文件解析失败(" + strTempFileName + "),原因:";
						log.error(logStr, e);
						taskInfo.log(DataLogInfo.STATUS_PARSE, logStr, e);
						continue;
					}

					boolean isDel = !DataLifecycleMgr.getInstance().isEnable() && DataLifecycleMgr.getInstance().isDeleteWhenOff();
					FTPConfig cfg = FTPConfig.getFTPConfig(taskInfo.getTaskID());
					if (cfg != null)
						isDel = cfg.isDelete();
					// 删除文件(数据文件存活时间管理模块关闭，默认删除)
					if (isDel) {
						File f = new File(strTempFileName);
						if (taskInfo.getParserID() == 9002
								&& (f.getName().toLowerCase().endsWith(".xls") || f.getName().toLowerCase().endsWith(".xlsx"))) {
							// 由解析类来删除
						} else
							f.delete();
					}
					logStr = name + ": " + strTempFileName + " 解析分发完成.";
					log.debug(logStr);
					taskInfo.log(DataLogInfo.STATUS_PARSE, logStr);
				}
			}

			logStr = name + ": 完成数据解析分发处理,共耗时：" + localtimecount + " MR数:" + mrsumcount + "," + "定位: " + taskInfo.m_nAllRecordCount;
			log.debug(logStr);
			taskInfo.log(DataLogInfo.STATUS_PARSE, logStr);
			// 开始移动文件到指定的目录
			parsecmd.comitmovefiles();

			// 返回成功的标志
			bSucceed = true;
			// 根据标识判断 解析完是否要删掉服务器上的文件 add by lijiayu 2013-10-08
			// String strShellCmdFinish = taskInfo.getShellCmdFinish();
			// if (Util.isNotNull(strShellCmdFinish) && strShellCmdFinish.trim().toUpperCase().startsWith("DELETEFILE")) {
			// deleteFiles(strNeedGatherFileNames, ftp, strShellCmdFinish);
			// }
		} catch (Exception e) {
			logStr = name + ": FTP采集异常.";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);
			// dbLogger.log(taskInfo.getDevInfo().getOmcID(),
			// t.getDestTableName(), taskInfo.getLastCollectTime(), -1,
			// taskInfo.getTaskID());
			// 通知告警
			AlarmMgr.getInstance().insert(taskID, "FTP采集异常", name, e.getMessage(), 1501);
		} finally {
			ftp.disconnect();
		}

		return bSucceed;
	}

	@Override
	public void configure() throws Exception {

	}

	@Override
	public boolean doAfterAccess() throws Exception {
		// 采集之后执行的Shell命令
		String strShellCmdFinish = taskInfo.getShellCmdFinish();
		// 如果是删除文件的标识，则不执行
		if (Util.isNotNull(strShellCmdFinish) && strShellCmdFinish.trim().toUpperCase().startsWith("DELETEFILE")) {
			return true;
		}
		if (Util.isNotNull(strShellCmdFinish)) {
			Parsecmd.ExecShellCmdByFtp(strShellCmdFinish, taskInfo.getLastCollectTime());
		}
		return true;
	}

	/**
	 * 解析完 删掉服务器上的文件 add by lijiayu 2013-10-08 标识配置支持按 年Y，月M，日D，时H，分m，周W 如按日为 'deleteFile,3D'
	 */
	// private void deleteFiles(String[] strNeedGatherFileNames, FTPTool ftp, String strShellCmdFinish) {
	// log.info("begin to delete files");
	// // 路测的解析完要删掉服务器上的文件
	// ftp.deleteFiles(strNeedGatherFileNames, strShellCmdFinish);
	// }
}
