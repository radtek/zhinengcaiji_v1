package parser.nbi.check;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import parser.Parser;
import parser.nbi.check.bean.FilesMgrServerInfo;
import parser.nbi.check.bean.NbiFileCheckInfo;
import parser.nbi.check.bean.NbiFileCheckResult;
import sqlldr.SqlldrInfo;
import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import util.DbPool;
import util.ExternalCmd;
import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import cn.uway.alarmbox.db.pool.DBUtil;
import collect.FTPTool;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 集团文件上报检查。
 * 
 * @author yuy 2014-7-31
 */
public class GroupFilesCheck extends Parser {

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private CollectObjInfo task;

	private String logKey;

	private boolean regatherFlag = false;

	private static Map<String, FilesMgrServerInfo> FilesMgrServerInfoMap;

	private List<NbiFileCheckInfo> checkInfoList;

	private static Map<Integer, String> checkTypeMap;

	private static String PROVINCENUM_REGEX = "{PROVINCE_EN}";

	private static Integer NORMAL_TYPE = 1;

	private static Integer NOFILE_TYPE = 2;

	private static Integer EMPTYFILE_TYPE = 3;

	private static Integer DOWNFAIL_TYPE = 4;

	private static Integer FILEBASE_SIZE = 2000;

	private static String QUERYFTPINFO_SQL = "select substr(china_name,0,instr(china_name,'|',1,1)-1) as province_num,"
			+ "serveraddress,serverport,username,password from mod_filesmgr_serverinfo";

	private static String QUERYFILESCHECKINFO_SQL = "select file_id,ftp_dir,ftp_dir || '/' || file_rule_name as path from "
			+ "CFG_NBI_FILE_CHECK where is_effect = 1";

	private static String INSERTINTORESULT_SQL = "insert into mod_nbi_file_check_result(START_TIME,PROVINCE_NAME,"
			+ "FILE_ID,FILE_NAME,FILE_CHECK_TYPE,FILE_CHECK_RESULT) values (?,?,?,?,?,?)";

	private Map<String/* 表名 */, SqlldrInfo/* sqlldr入库信息 */> sqlldrs; // 记录入库信息，表名对应sqlldr信息

	private String mydir = "jituan_nbi_check";

	private String tableName = "mod_nbi_file_check_result";

	private String splitSign = "~`";

	private String stampTime;

	public GroupFilesCheck() {

	}

	public GroupFilesCheck(CollectObjInfo task) {
		this.task = task;
		long id = task.getTaskID();
		if (task instanceof RegatherObjInfo)
			id = task.getKeyID() - 10000000;
		this.stampTime = Util.getDateString(task.getLastCollectTime());
		this.logKey = task.getTaskID() + "-" + id;
		this.logKey = String.format("[%s][%s]", this.logKey, stampTime);
		checkInfoList = new ArrayList<NbiFileCheckInfo>();
	}

	static {
		if (FilesMgrServerInfoMap == null) {
			FilesMgrServerInfoMap = new HashMap<String, FilesMgrServerInfo>();
		}
		if (FilesMgrServerInfoMap.size() == 0) {
			loadFilesMgrServerInfo();
		}
		checkTypeMap = new HashMap<Integer, String>();
		checkTypeMap.put(NORMAL_TYPE, "正常");
		checkTypeMap.put(NOFILE_TYPE, "上报文件缺失");
		checkTypeMap.put(EMPTYFILE_TYPE, "上报文件内容为空");
		checkTypeMap.put(DOWNFAIL_TYPE, "下载失败");
	}

	public boolean parse() {
		logger.info(logKey + "表mod_filesmgr_serverinfo加载到" + FilesMgrServerInfoMap.size() + "条ftp信息");
		if (FilesMgrServerInfoMap.size() == 0)
			return false;

		// 加载所要核查的文件
		loadFilesCheckInfo();

		if (checkInfoList.size() == 0)
			return false;

		List<Thread> threadList = new ArrayList<Thread>();
		for (final String provinceNum : FilesMgrServerInfoMap.keySet()) {
			// 重构task
			reBuildTaskInfo(provinceNum);
			final FTPTool ftpTool = new FTPTool(task);
			Thread thread = new Thread(new Runnable() {

				@Override
				public void run() {
					if (!loginForFTP(ftpTool))
						return;
					List<NbiFileCheckResult> resultList = new ArrayList<NbiFileCheckResult>();
					for (NbiFileCheckInfo checkInfo : checkInfoList) {
						String path = ConstDef.ParseFilePath(checkInfo.getFILE_RULE_NAME().trim(), task.getLastCollectTime());
						path = path.replace(PROVINCENUM_REGEX, provinceNum);
						FTPFile[] fs = listFTPFiles(ftpTool, path);
						// 上报文件缺失
						if (fs == null || fs.length == 0) {
							resultList.add(buildCheckResult(provinceNum, checkInfo, path, NOFILE_TYPE));
							continue;
						}
						for (FTPFile file : fs) {
							// 文件大小大于基准值，被认为有数据，正常
							if (file.getSize() > FILEBASE_SIZE) {
								resultList.add(buildCheckResult(provinceNum, checkInfo, checkInfo.getFTP_DIR() + "/" + file.getName(), NORMAL_TYPE));
								continue;
							}
							// 文件大小为0，空文件
							if (file.getSize() == 0) {
								resultList.add(buildCheckResult(provinceNum, checkInfo, checkInfo.getFTP_DIR() + "/" + file.getName(), EMPTYFILE_TYPE));
								continue;
							}
							// 文件偏小，需要验证是否有数据（只针对csv文件）
							if (file.getName().endsWith(".csv") || file.getName().endsWith(".CSV")) {
								String localPath = SystemConfig.getInstance().getCurrentPath() + "/" + mydir + "/" + checkInfo.getFTP_DIR();
								ftpTool.downFile(checkInfo.getFTP_DIR() + "/" + file.getName(), localPath);
								File tmpFile = new File(localPath + "/" + file.getName());
								// 下载失败
								if (!tmpFile.exists()) {
									resultList.add(buildCheckResult(provinceNum, checkInfo, checkInfo.getFTP_DIR() + "/" + file.getName(),
											DOWNFAIL_TYPE));
									continue;
								}
								BufferedReader reader = null;
								@SuppressWarnings("unused")
								String line = null;
								int count = 0;
								try {
									reader = new BufferedReader(new InputStreamReader(new FileInputStream(tmpFile)));
									while ((line = reader.readLine()) != null) {
										count++;
										if (count > 2)
											break;
									}
								} catch (IOException e) {
									logger.error(logKey + "读取文件：" + tmpFile + "时出现异常", e);
								} finally {
									try {
										reader.close();
									} catch (IOException e) {
										logger.error(logKey + "结束流时出现异常", e);
									}
								}
								// 多于2行，正常
								if (count > 2) {
									resultList.add(buildCheckResult(provinceNum, checkInfo, checkInfo.getFTP_DIR() + "/" + file.getName(),
											NORMAL_TYPE));
									continue;
								}
								// 空文件
								resultList.add(buildCheckResult(provinceNum, checkInfo, checkInfo.getFTP_DIR() + "/" + file.getName(), EMPTYFILE_TYPE));
								continue;
							}
							// 其他空文件
							resultList.add(buildCheckResult(provinceNum, checkInfo, checkInfo.getFTP_DIR() + "/" + file.getName(), EMPTYFILE_TYPE));
						}
					}

					// 入库
					logger.debug(logKey + "省份：" + provinceNum + "，上报文件核查完毕，开始准备入库");
					if (sqlldrs == null)
						initSqlldrs();
					writeSqlldrTxt(resultList);
					startSqlldr();
				}

				/**
				 * @param provinceNum
				 * @param checkInfo
				 * @param path
				 * @param checkType
				 * @return
				 */
				private NbiFileCheckResult buildCheckResult(final String provinceNum, NbiFileCheckInfo checkInfo, String path, Integer checkType) {
					NbiFileCheckResult result = new NbiFileCheckResult();
					result.setSTART_TIME(task.getLastCollectTime());
					result.setPROVINCE_NAME(provinceNum);
					result.setFILE_ID(checkInfo.getFILE_ID());
					result.setFILE_NAME(path);
					result.setFILE_CHECK_TYPE(checkType);
					result.setFILE_CHECK_RESULT(checkTypeMap.get(checkType));
					return result;
				}

				/**
				 * @param resultList
				 */
				@SuppressWarnings("unused")
				protected void insertIntoResults(List<NbiFileCheckResult> resultList) {
					Connection conn = null;
					PreparedStatement ps = null;
					try {
						conn = DbPool.getConn();
						conn.setAutoCommit(false);
						ps = conn.prepareStatement(INSERTINTORESULT_SQL);
						for (NbiFileCheckResult result : resultList) {
							ps.setTimestamp(1, new Timestamp(result.getSTART_TIME().getTime()));
							ps.setString(2, result.getPROVINCE_NAME());
							ps.setInt(3, result.getFILE_ID());
							ps.setString(4, result.getFILE_NAME());
							ps.setInt(5, result.getFILE_CHECK_TYPE());
							ps.setString(6, result.getFILE_CHECK_RESULT());
							ps.addBatch();
						}
						// int[] flag = ps.executeBatch();
						ps.executeBatch();
						logger.debug(logKey + "省份：" + provinceNum + " 入库成功，共入库条数：" + resultList.size());
					} catch (Exception e) {
						logger.error(logKey + "入库mod_nbi_file_check_result表出现异常", e);
					} finally {
						DBUtil.close(null, ps, conn);
					}
				}

			});
			thread.start();
			threadList.add(thread);

		}
		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				logger.error(logKey + "线程被打断", e);
			}
		}

		return true;
	}

	/**
	 * 重构task
	 * 
	 * @param fileId
	 */
	private void reBuildTaskInfo(String provinceNum) {
		FilesMgrServerInfo ftpInfo = FilesMgrServerInfoMap.get(provinceNum);
		DevInfo dev = task.getDevInfo();
		task.setDevPort(ftpInfo.getSERVERPORT());
		dev.setIP(ftpInfo.getSERVERADDRESS());
		dev.setHostUser(ftpInfo.getUSERNAME());
		dev.setHostPwd(ftpInfo.getPASSWORD());
	}

	/**
	 * 加载各省市ftp信息
	 */
	protected static void loadFilesMgrServerInfo() {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = DbPool.getConn();
			ps = conn.prepareStatement(QUERYFTPINFO_SQL);
			rs = ps.executeQuery();
			while (rs.next()) {
				FilesMgrServerInfo ftpInfo = new FilesMgrServerInfo();
				ftpInfo.setCHINA_NAME(removeNull(rs.getString("province_num")));
				ftpInfo.setSERVERADDRESS(removeNull(rs.getString("serveraddress")));
				ftpInfo.setSERVERPORT(rs.getInt("serverport"));
				ftpInfo.setUSERNAME(removeNull(rs.getString("username")));
				ftpInfo.setPASSWORD(removeNull(rs.getString("password")));
				FilesMgrServerInfoMap.put(ftpInfo.getCHINA_NAME(), ftpInfo);
			}
			logger.info("表mod_filesmgr_serverinfo加载到" + FilesMgrServerInfoMap.size() + "条ftp信息");
		} catch (Exception e) {
			logger.error("查询mod_filesmgr_serverinfo表出现异常", e);
		} finally {
			DBUtil.close(rs, ps, conn);
		}
	}

	/**
	 * 加载省市文件信息
	 */
	protected void loadFilesCheckInfo() {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = DbPool.getConn();
			ps = conn.prepareStatement(QUERYFILESCHECKINFO_SQL);
			rs = ps.executeQuery();
			while (rs.next()) {
				NbiFileCheckInfo checkInfo = new NbiFileCheckInfo();
				checkInfo.setFILE_ID(rs.getInt("file_id"));
				checkInfo.setFTP_DIR(rs.getString("ftp_dir"));
				checkInfo.setFILE_RULE_NAME(removeNull(rs.getString("path")));
				checkInfoList.add(checkInfo);
			}
			logger.info("表cfg_nbi_file_check加载到" + checkInfoList.size() + "条核查文件信息");
		} catch (Exception e) {
			logger.error("查询cfg_nbi_file_check表出现异常", e);
		} finally {
			DBUtil.close(rs, ps, conn);
		}
	}

	/**
	 * 初始化sqlldr
	 * 
	 * @return
	 */
	protected boolean initSqlldrs() {
		if (sqlldrs != null)
			return true;
		String time = Util.getDateString_yyyyMMddHHmmss(task.getLastCollectTime());
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator + mydir + File.separator
				+ task.getTaskID() + File.separator);
		long flag = System.currentTimeMillis();
		if (!dir.mkdirs() && !dir.exists()) {
			logger.error(logKey + "创建临时目录失败：" + dir);
			return false;
		}
		sqlldrs = new HashMap<String, SqlldrInfo>();
		String baseName = tableName + "_" + time + "_" + flag;
		File txt = new File(dir, baseName + ".txt");
		File ctl = new File(dir, baseName + ".ctl");
		File log = new File(dir, baseName + ".log");
		File bad = new File(dir, baseName + ".bad");
		SqlldrInfo sq = new SqlldrInfo(txt, log, bad, ctl);
		sq.writerForCtl.println("LOAD DATA");
		sq.writerForCtl.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
		sq.writerForCtl.println("INFILE '" + txt.getAbsolutePath() + "' APPEND INTO TABLE " + tableName);
		sq.writerForCtl.println("FIELDS TERMINATED BY \"" + splitSign + "\"");
		sq.writerForCtl.println("TRAILING NULLCOLS (");
		sq.writerForCtl.println("start_time DATE 'YYYY-MM-DD HH24:MI:SS',");
		sq.writerForTxt.print("start_time" + splitSign);
		sq.writerForCtl.println("province_name,");
		sq.writerForTxt.print("province_name" + splitSign);
		sq.writerForCtl.println("file_id,");
		sq.writerForTxt.print("file_id" + splitSign);
		sq.writerForCtl.println("file_name CHAR(4000),");
		sq.writerForTxt.print("file_name " + splitSign);
		sq.writerForCtl.println("file_check_type,");
		sq.writerForTxt.print("file_check_type" + splitSign);
		sq.writerForCtl.println("file_check_result CHAR(4000)");
		sq.writerForTxt.print("file_check_result");

		sq.writerForTxt.println();
		sq.writerForTxt.flush();
		sq.writerForCtl.println(")");
		sq.writerForCtl.close();
		sqlldrs.put(tableName, sq);

		return true;
	}

	/**
	 * 写入sqlldr txt文件
	 * 
	 * @param resultList
	 */
	public void writeSqlldrTxt(List<NbiFileCheckResult> resultList) {
		SqlldrInfo info = sqlldrs.get(tableName);
		for (int i = 0; i < resultList.size(); i++) {
			NbiFileCheckResult result = resultList.get(i);
			info.writerForTxt.print(stampTime + splitSign);
			info.writerForTxt.print(result.getPROVINCE_NAME() + splitSign);
			info.writerForTxt.print(result.getFILE_ID() + splitSign);
			info.writerForTxt.print(result.getFILE_NAME() + splitSign);
			info.writerForTxt.print(result.getFILE_CHECK_TYPE() + splitSign);
			info.writerForTxt.print(result.getFILE_CHECK_RESULT());
			info.writerForTxt.println();
		}
		info.writerForTxt.flush();
	}

	/**
	 * 开始入库
	 */
	public void startSqlldr() {
		if (sqlldrs == null)
			return;
		Iterator<String> it = sqlldrs.keySet().iterator();
		stampTime = Util.getDateString_yyyyMMddHHmmss(task.getLastCollectTime());
		while (it.hasNext()) {
			String tn = it.next();
			SqlldrInfo sq = sqlldrs.get(tn);
			sq.close();
			String cmd = String.format("sqlldr userid=%s/%s@%s skip=1 control=%s bad=%s log=%s errors=9999999", SystemConfig.getInstance()
					.getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(),
					sq.ctl.getAbsoluteFile(), sq.bad.getAbsoluteFile(), sq.log.getAbsoluteFile());
			logger.debug(logKey + "执行 "
					+ cmd.replace(SystemConfig.getInstance().getDbPassword(), "*").replace(SystemConfig.getInstance().getDbUserName(), "*"));
			ExternalCmd execute = new ExternalCmd();
			try {
				int ret = execute.execute(cmd);
				SqlldrResult result = new SqlLdrLogAnalyzer().analysis(sq.log.getAbsolutePath());
				logger.debug(logKey + "exit=" + ret + " omcid=" + task.getDevInfo().getOmcID() + " 入库成功条数=" + result.getLoadSuccCount() + " 表名="
						+ result.getTableName() + " 数据时间=" + stampTime + " sqlldr日志=" + sq.log.getAbsolutePath());
				// LogMgr.getInstance().getDBLogger()
				// .log(task.getDevInfo().getOmcID(), result.getTableName(), stampTime, result.getLoadSuccCount(), task.getTaskID());
				if (ret == 0 && SystemConfig.getInstance().isDeleteLog()) {
					sq.txt.delete();
					sq.ctl.delete();
					sq.log.delete();
					sq.bad.delete();
				} else if (ret == 2 && SystemConfig.getInstance().isDeleteLog()) {
					sq.txt.delete();
					sq.ctl.delete();
					sq.bad.delete();
				}
			} catch (Exception ex) {
				logger.error(logKey + "sqlldr时异常", ex);
			}
		}

		sqlldrs.clear();
		sqlldrs = null;
	}

	/**
	 * @param str
	 * @return
	 */
	protected static String removeNull(String str) {
		return str == null ? "" : str.trim();
	}

	/**
	 * 在FTP上列出指定路径的文件。如果失败，将返回<code>null</code>.
	 * 
	 * @param path
	 *            FTP文件路径。
	 * @return FTP文件路径。
	 */
	protected FTPFile[] listFTPFiles(FTPTool ftpTool, String path) {
		FTPFile[] ftpFiles = null;
		boolean isException = false;
		try {
			ftpFiles = ftpTool.getFtpClient().listFiles(path);
		} catch (Exception e) {
			logger.error(logKey + "listFiles异常 - " + path, e);
			isException = true;
		}

		if (ftpFiles == null || ftpFiles.length == 0) {
			logger.debug(logKey + "重试listFiles - " + path);
			if (isException)
				ftpTool.login(2000, 2);
			try {
				ftpFiles = ftpTool.getFtpClient().listFiles(path);
			} catch (Exception e) {
				logger.error(logKey + "重试listFiles异常 - " + path, e);
			}

			if (ftpFiles == null || ftpFiles.length == 0) {
				logger.debug(logKey + "重试listFiles成功 - " + path + " 文件个数 - " + ftpFiles.length);
			} else {
				logger.error(logKey + "重试listFiles失败 - " + path);
				// TaskMgr.getInstance().newRegather(task, path, String.format("listFiles失败(ftpFiles%s)", ftpFiles == null ? "为null" : "长度为0"));
				return null;
			}
		} else {
			logger.debug(logKey + "listFiles成功 - " + path + " 文件个数 - " + ftpFiles.length);
		}

		return ftpFiles;
	}

	/**
	 * 登录到FTP.
	 * 
	 * @return 是否登录成功。
	 */
	protected boolean loginForFTP(FTPTool ftpTool) {
		if (ftpTool.login(2000, 3)) {
			logger.info(logKey + "FTP登录成功");
			return true;
		} else {
			logger.error(logKey + "FTP登录失败");
			synchronized (this) {
				if (!regatherFlag) {
					TaskMgr.getInstance().newRegather(task, "", "集团上报文件：多次登陆失败，全部补采");
					regatherFlag = true;
				}
			}
			return false;
		}
	}

	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
}
