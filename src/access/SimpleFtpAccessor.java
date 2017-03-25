package access;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.parser.NTFTPEntryParser;

import task.RegatherObjInfo;
import task.TaskMgr;
import util.CommonDB;
import util.Parsecmd;
import util.Util;
import util.opencsv.CSVWriter;
import alarm.AlarmMgr;
import collect.DownStructer;
import collect.FTPConfig;
import collect.FTPTool;
import datalog.DataLogInfo;
import framework.ConstDef;
import framework.DataLifecycleMgr;
import framework.SystemConfig;

public class SimpleFtpAccessor extends AbstractAccessor {

	/** 登陆FTP失败重试最大次数 */
	private static final byte MAX_TRY_TIMES = 5;

	private int index = 0;

	public SimpleFtpAccessor() {
		super();
	}

	/* 2011-6-29 by chensj 把数据库表转为.data文件 */
	private boolean dbPack() throws Exception {
		Connection con = CommonDB.getConnection(taskInfo, 3000, (byte) 3);
		Statement st = null;
		ResultSet rs = null;
		try {
			if (con == null) {
				log.error(name + "获取数据库连接失败");
				return false;
			}

			String[] sqls = taskInfo.getCollectPath().split(";");
			if (sqls == null || sqls.length == 0) {
				log.error(name + "collect_path中未找到可用的select语句");
				return false;
			}
			File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + taskInfo.getTaskID() + File.separator + "db_pack"
					+ File.separator + Util.getDateString_yyyyMMddHH(taskInfo.getLastCollectTime()) + File.separator);
			dir.mkdirs();

			for (String sql : sqls) {
				if (Util.isNull(sql))
					continue;
				sql = ConstDef.ParseFilePath(sql, taskInfo.getLastCollectTime());
				try {
					st = con.createStatement();
					rs = st.executeQuery(sql);
				} catch (Exception e) {
					log.error(name + "执行sql出错 - " + sql, e);
					CommonDB.close(rs, st, null);
					continue;
				}

				File dataFile = new File(dir, CommonDB.getTableName(sql) + ".data");
				Writer writer = new PrintWriter(dataFile);
				CSVWriter csvWriter = new CSVWriter(writer, ',', '\0');
				csvWriter.writeAll(rs, true);
				csvWriter.close();
				writer.close();
				CommonDB.close(rs, st, null);
				log.debug(name + "文件已生成 - " + dataFile.getAbsolutePath());
				DataLifecycleMgr.getInstance().doFileTimestamp(dataFile.getAbsolutePath(), taskInfo.getLastCollectTime());
			}
		} finally {
			CommonDB.closeDBConnection(con, st, rs);
		}
		return true;
	}

	public boolean access() throws Exception {
		if (FTPConfig.getFTPConfig(taskInfo.getTaskID()) != null && FTPConfig.getFTPConfig(taskInfo.getTaskID()).isByCreateTime()) {
			return createTimeAccess();
		}
		boolean bSucceed = false;

		long taskID = this.getTaskID();

		if (Util.isNotNull(taskInfo.getDBDriver()) && Util.isNotNull(taskInfo.getDBUrl()))
			return dbPack();

		FTPTool ftp = new FTPTool(taskInfo);
		String logStr = name + ": 开始FTP登陆.";
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_START, logStr);
		try {
			boolean bOK = ftp.login(30000, MAX_TRY_TIMES);
			if (!bOK) {
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

			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

			// 需要采集的文件,多个文件以;间隔
			String[] strNeedGatherFileNames = this.getDataSourceConfig().getDatas();

			List<String> list = new ArrayList<String>();
			for (String s : strNeedGatherFileNames) {
				try {
					list.addAll(Util.listFTPDirs(ConstDef.ParseFilePath(s, taskInfo.getLastCollectTime()), taskInfo.getDevInfo().getIP(), taskInfo
							.getDevPort(), taskInfo.getDevInfo().getHostUser(), taskInfo.getDevInfo().getHostPwd(),
							taskInfo.getDevInfo().getEncode(), taskInfo.getParserID()));
				} catch (Exception e) {
					log.error("展开目录通配符时异常");
					throw e;
				}
			}
			if (list.size() == 0) {
				log.warn("展开目录通配符后，路径条数为0");
			} else {
				log.debug("展开目录 - " + list);
			}

			List<String> tmpList = new ArrayList<String>(list);
			list.clear();
			String str = getDataSourceConfig().getDatas()[0];
			String[] sp = str.split("/");
			int wIndex = -1;
			for (int i = 0; i < sp.length; i++) {
				if (sp[i].equals("*")) {
					wIndex = i;
					break;
				}
			}
			for (String s : tmpList) {
				if (wIndex == -1) {
					list.add(s);
					continue;
				}
				StringBuilder onePath = new StringBuilder();
				sp = s.split("/");
				for (int i = 0; i < sp.length; i++) {
					if (i == wIndex) {
						onePath.append("!").append(sp[i]).append("{").append(taskInfo.getTaskID()).append("}!").append("/");
					} else
						onePath.append(sp[i]).append("/");
				}
				onePath.delete(onePath.length() - 1, onePath.length());
				list.add(onePath.toString());
			}

			strNeedGatherFileNames = (String[]) list.toArray(new String[0]);

			// 解压文件并遍历文件夹，返回文件的路径
			Parsecmd parsecmd = new Parsecmd();

			// 下载并解析文件内容
			for (String gatherFileName : strNeedGatherFileNames) {
				index++;
				if (Util.isNull(gatherFileName))
					continue;

				String strSubFilePath = ConstDef.ParseFilePath(gatherFileName.trim(), taskInfo.getLastCollectTime());

				// 根据文件名建立目录
				String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strSubFilePath);

				// 以FTP方式下载文件到本地
				DownStructer dStruct = null;
				try {
					dStruct = ftp.downFile(strSubFilePath, strTempPath);
				} catch (Exception e) {
					// 加入补采表
					task.TaskMgr.getInstance().newRegather(taskInfo, gatherFileName, "文件下载失败，异常信息为:" + e.getMessage());
					continue;
				}

				// 如果文件不存在，则加入补采表并跳过
				// strFileNames=dStruct.getSuc().toArray(new String[0]);
				if (dStruct.getSuc().size() == 0) {
					// 加入补采表
					task.TaskMgr.getInstance().newRegather(taskInfo, gatherFileName, "文件不存在");
					continue;
				}

				List<String> arrfileList = new ArrayList<String>();

				for (String strFileName : dStruct.getSuc()) {
					if (Util.isNull(strFileName))
						continue;
					arrfileList.add(strFileName);
				}

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

				if (parser != null)
					parser.setDsConfigName(gatherFileName);
				else
					log.warn(name + " - SimpleFtpAccessor~ parser is null");

			}
			// 开始移动文件到指定的目录
			parsecmd.comitmovefiles();

			// 返回成功的标志
			bSucceed = true;
		} catch (Exception e) {
			logStr = name + ": FTP下载异常.";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);

			// 通知告警
			AlarmMgr.getInstance().insert(taskID, "FTP下载异常", name, e.getMessage(), 1501);
		} finally {
			ftp.disconnect();
		}

		return bSucceed;
	}

	private static final Map<Long, Map<String, List<String>>> downedMap = new HashMap<Long, Map<String, List<String>>>();

	public boolean createTimeAccess() throws Exception {

		boolean bSucceed = false;

		long taskID = this.getTaskID();

		if (Util.isNotNull(taskInfo.getDBDriver()) && Util.isNotNull(taskInfo.getDBUrl()))
			return dbPack();

		FTPConfig cfg = FTPConfig.getFTPConfig(taskID);

		FTPTool ftp = new FTPTool(taskInfo);
		String logStr = name + ": 开始FTP登陆.";
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_START, logStr);
		try {
			boolean bOK = ftp.login(30000, MAX_TRY_TIMES);
			if (!bOK) {
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

			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

			// 需要采集的文件,多个文件以;间隔
			String[] strNeedGatherFileNames = this.getDataSourceConfig().getDatas();

			List<String> collectPathlist = new ArrayList<String>();
			for (String s : strNeedGatherFileNames) {
				String apath = s != null ? s.trim() : "";
				apath = ConstDef.ParseFilePath(apath, taskInfo.getLastCollectTime());
				if (Util.isNotNull(apath))
					collectPathlist.add(apath);
			}
			if (collectPathlist.isEmpty()) {
				log.warn(name + " 路径条数为0");
			}

			for (String apath : collectPathlist) {
				List<String> newDownedFiles = new ArrayList<String>();
				FTPFile[] ftpFiles = null;
				try {
					ftpFiles = ftp.getFtpClient().listFiles(apath);
				} catch (Exception e) {
					log.error(name + " listFiles发生异常 - " + apath, e);
				}
				if (ftpFiles == null || ftpFiles.length == 0) {
					log.warn(name + " listFiles未获取到文件。");
					for (int i = 0; i < cfg.getListTryTimes(); i++) {
						log.debug(name + " 第" + (i + 1) + "次重试list - " + apath);
						ftp.login(cfg.getLoginTryDelay(), cfg.getLoginTryTimes());
						try {
							ftpFiles = ftp.getFtpClient().listFiles(apath);
						} catch (Exception e) {
							log.error(name + " listFiles发生异常 - " + apath, e);
						}
						if (ftpFiles != null && ftpFiles.length > 0) {
							log.debug(name + " 第" + (i + 1) + "次重试list成功 - " + apath);
							break;
						}
					}
				}

				if (ftpFiles != null && ftpFiles.length > 0) {
					log.debug(name + " listFiles成功 - " + apath + "，文件个数：" + ftpFiles.length);
					if (ftpFiles[0] != null)
						log.debug("ftpFiles[0] time=" + ftpFiles[0].getTimestamp() + ",toString=" + ftpFiles[0]);
					LinkedList<FTPFile> lstFtpFiles = new LinkedList<FTPFile>();
					for (FTPFile fx : ftpFiles) {
						if (fx != null && fx.getTimestamp() != null)
							lstFtpFiles.add(fx);
					}
					Collections.sort(lstFtpFiles, new DateComp());
					while (lstFtpFiles.size() > cfg.getDownCount()) {
						lstFtpFiles.removeLast();
					}
					Collections.reverse(lstFtpFiles);

					for (int i = 0; i < lstFtpFiles.size(); i++) {
						String remotePath = FilenameUtils.getFullPath(apath) + FilenameUtils.getName(lstFtpFiles.get(i).getName());
						synchronized (downedMap) {
							if (downedMap.containsKey(taskID)) {
								if (downedMap.get(taskID).containsKey(apath)) {
									List<String> downedFiles = downedMap.get(taskID).get(apath);
									if (downedFiles.contains(remotePath)) {
										log.debug(name + " 文件" + remotePath + "已下载过，跳过。");
										continue;
									}
								}
							}
						}
						OutputStream fout = null;
						File localFile = new File(cfg.getLocalPath(), FilenameUtils.getName(remotePath));
						File tmplocalFile = new File(cfg.getLocalPath(), FilenameUtils.getName(remotePath) + ".tmp");
						boolean downOk = false;
						try {
							fout = new FileOutputStream(tmplocalFile);
							downOk = ftp.getFtpClient().retrieveFile(remotePath, fout);
						} catch (Exception e) {
							log.error(name + " 下载发生异常 - " + remotePath, e);
						} finally {
							IOUtils.closeQuietly(fout);
							if (downOk) {
								if (localFile.exists() && localFile.isFile())
									localFile.delete();
								if (!tmplocalFile.renameTo(localFile)) {
									log.warn(name + " 将" + tmplocalFile + "重命名为" + localFile + "时失败，可能文件被占用。");
									downOk = false;
								}
							}
						}

						if (downOk)
							newDownedFiles.add(remotePath);

						if (downOk)
							log.debug(name + " 下载成功 - " + localFile + "，文件大小：" + localFile.length() + "字节");
						else
							log.debug(name + " 下载失败 - " + localFile + "，远程文件：" + remotePath);

					}

				}

				ftp.disconnect();
				synchronized (downedMap) {
					Map<String, List<String>> map0 = null;
					if (downedMap.containsKey(taskID)) {
						map0 = downedMap.get(taskID);
					} else {
						map0 = new HashMap<String, List<String>>();
						downedMap.put(taskID, map0);
					}
					map0.put(apath, newDownedFiles);
				}
			}
		} catch (Exception e) {
			logStr = name + ": FTP下载异常.";
			log.error(logStr, e);
			taskInfo.log(DataLogInfo.STATUS_START, logStr, e);

			// 通知告警
			AlarmMgr.getInstance().insert(taskID, "FTP下载异常", name, e.getMessage(), 1501);
		} finally {
			ftp.disconnect();
		}

		return bSucceed;
	}

	private static class DateComp implements Comparator<FTPFile> {

		@Override
		public int compare(FTPFile o1, FTPFile o2) {
			return o2.getTimestamp().compareTo(o1.getTimestamp());
		}

	}

	public void configure() {

	}

	public void doSqlLoad() {

	}

	/** 销毁资源 */
	public void dispose(long lastCollectTime) {

		runFlag = false;
		taskInfo.setUsed(false);

		// 删除该线程任务

		String logStr = name + ": remove from active-task-map. " + strLastGatherTime;
		log.debug(logStr);
		taskInfo.log(DataLogInfo.STATUS_END, logStr);
		TaskMgr.getInstance().delActiveTask(taskInfo.getKeyID(), taskInfo instanceof RegatherObjInfo);

		TaskMgr.getInstance().commitRegather(taskInfo, lastCollectTime);
	}

	public static void main(String[] args) {
		NTFTPEntryParser p = new NTFTPEntryParser();
		FTPFile f = p.parseFTPEntry("03-13-12  09:16                4163115 BSC3009_[PCHR]00Log20120313084521_20120313091615.log.zip");
		System.out.println(f.getTimestamp());
	}
}
