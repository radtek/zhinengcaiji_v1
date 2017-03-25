package store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import store.sqlldrManage.SqlldrCmd;
import store.sqlldrManage.impl.SqlldrPool;
import task.CollectObjInfo;
import util.ExternalCmd;
import util.SqlldrResult;
import util.Util;
import util.loganalyzer.SqlLdrLogAnalyzer;
import exception.StoreException;
import framework.SystemConfig;

/**
 * sqlldr存储方式
 * 
 * @author YangJian
 * @since 3.1
 * @see SqlldrStoreParam
 */
public class SqlldrStore extends AbstractStore<SqlldrStoreParam> {

	private FileWriter fWriter;

	private List<String> buffer = new ArrayList<String>(MAX_SIZE);

	private static final int MAX_SIZE = 200;

	private int parserdCount;

	private SqlldrInfo info;

	// 江苏LTE4G网络采集,采集表中用mmeid替换了omcid:解析时把omcid作为mmeid入库
	protected String is4G = SystemConfig.getInstance().getLteIs4G();
	protected int sqlldrRetrytimes = SystemConfig.getInstance().getSqlldrRetrytims();

	public SqlldrStore() {
		super();
	}

	public SqlldrStore(SqlldrStoreParam param) {
		super(param);
	}

	@Override
	public void open() throws StoreException {
		SqlldrStoreParam param = this.getParam();
		if (param == null)
			throw new StoreException("缺少StoreParam对象");

		info = fillSqlldrInfo();
		// 创建控制文件
		makeFile_Ctl(info, param);
		// 创建txt文件并加上表头
		makeFile_Txt_Head(info, param);
		this.parserdCount = 0;
	}

	public void flush() throws StoreException {
		writeDataToTxtFile();
	}

	/**
	 * 执行sqlldr操作
	 */
	@Override
	public void commit() throws StoreException {
		runSqlldr();
	}

	/**
	 * 写数据
	 * 
	 * @return
	 * @throws StoreException
	 */
	public void write(String data) throws StoreException {
		parserdCount++;
		buffer.add(data);
		if (buffer.size() >= MAX_SIZE) {
			writeDataToTxtFile();
		}
	}

	@Override
	public void close() {
		if (fWriter != null) {
			try {
				fWriter.flush();
				fWriter.close();
			} catch (IOException e) {
			}
		}
		buffer.clear();
		this.info = null;
	}

	private SqlldrInfo fillSqlldrInfo() {
		SqlldrInfo info = new SqlldrInfo();

		String tbName = this.getParam().getTable().getName();
		long taskID = this.getTaskID();
		Timestamp dataTime = this.getDataTime();
		String strDateTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

		String fileNamePrex = taskID + "_" + tbName + "_" + strDateTime + "_" + this.getParam().getTable().getId()
				+ (getFlag() == null ? "" : "_" + getFlag());

		String folder = SystemConfig.getInstance().getCurrentPath() + File.separator + "ldrlog" + File.separator;
		File folderF = new File(folder);
		if (!folderF.exists() || !folderF.isDirectory())
			folderF.mkdirs();

		// 按年月存放日志
		Calendar calendar = Calendar.getInstance();
		int month = calendar.get(Calendar.MONTH) + 1; // 当前月份
		int year = calendar.get(Calendar.YEAR);
		String yearFolder = folder + year + "-" + month + File.separator;
		File yearFolderF = new File(yearFolder);
		if (!yearFolderF.exists() || !yearFolderF.isDirectory())
			yearFolderF.mkdir();

		String name = yearFolder + fileNamePrex;
		info.ctlFile = name + ".ctl";
		info.logFile = name + ".log";
		info.badFile = name + ".bad";
		info.txtFile = name + ".txt";
		info.tbName = tbName;

		return info;
	}

	/**
	 * 生成ctl文件
	 */
	private void makeFile_Ctl(SqlldrInfo info, SqlldrStoreParam param) throws StoreException {
		File f = new File(info.ctlFile);
		PrintWriter pw = null;
		try {
			String split = param.getTable().getSplitSign(); // 分隔符

			String columnNameList = param.getTable().listColumnNamesWithType(","); // 注意返回值中最后已经有一个分隔符（这里为逗号）

			pw = new PrintWriter(f);
			pw.println("load data");
			pw.println("CHARACTERSET " + SystemConfig.getInstance().getSqlldrCharset());
			pw.println("infile '" + info.txtFile + "' append into table " + info.tbName);
			pw.println("FIELDS TERMINATED BY \"" + split + "\"");
			pw.println("TRAILING NULLCOLS");
			pw.print("(" + columnNameList);
			pw.print("true".equals(is4G) ? "MMEID" : "OMCID");// 4g:omcid字段变成了mmeid
			pw.print(",COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS'"); // 加上系统字段名
			pw.print(")");
			pw.flush();
			pw.close();
		} catch (Exception e) {
			throw new StoreException("ctl文件(" + info.ctlFile + ")写入失败", e);
		} finally {
			if (pw != null)
				pw.close();
		}
	}

	/**
	 * 把缓存中的数据写到txt文件中,写完后清空缓存
	 */
	private void writeDataToTxtFile() throws StoreException {
		try {
			String split = this.getParam().getTable().getSplitSign(); // 分隔符
			// //时间字段所在的列索引,要入为stamptime字段值
			String indexOfTimeColumn = this.getParam().getTable().getIndexOfTimeColumn();
			if (fWriter == null) {
				fWriter = new FileWriter(new File(info.txtFile), true);
			}

			String nowStr = Util.getDateString(new Date());

			for (String str : buffer) {
				String time = null;
				if (indexOfTimeColumn != null) {
					// 截取时间字段
					int index = Integer.parseInt(indexOfTimeColumn);
					String subStr = str;
					for (int n = 0; n < index; n++) {
						subStr = subStr.substring(subStr.indexOf(split) + 1, subStr.length());
					}
					time = subStr.substring(0, subStr.indexOf(split));
				}
				// 确定stampTime字段值
				String stampTime = indexOfTimeColumn == null ? Util.getDateString(this.getDataTime()) : time;
				String sysFieldValue = this.getOmcID() + split + nowStr + split + stampTime;

				if (SystemConfig.getInstance().isSPAS()) {
					sysFieldValue = getCollectInfo().spasOmcId + split + nowStr + split + stampTime;
				}
				// 写入txt文件
				fWriter.write(str + sysFieldValue + "\n");
			}
			fWriter.flush();
		} catch (Exception e) {
			if (fWriter != null) {
				try {
					fWriter.close();
				} catch (IOException ioe) {
				}
			}
			throw new StoreException("数据写入文件(" + info.txtFile + ")时异常.", e);
		} finally {
			buffer.clear();
		}
	}

	/**
	 * 创建数据文件表头
	 * 
	 * @param info
	 * @param param
	 */
	private void makeFile_Txt_Head(SqlldrInfo info, SqlldrStoreParam param) throws StoreException {
		try {
			String split = param.getTable().getSplitSign(); // 分隔符
			// 4g:omcid字段变成了mmeid
			String strHead = param.getTable().listColumnNames(split) + ("true".equals(is4G) ? "MMEID" : "OMCID") + split + "COLLECTTIME" + split
					+ "STAMPTIME\n";
			if (fWriter == null) {
				fWriter = new FileWriter(new File(info.txtFile), false);
			}
			fWriter.write(strHead); // 写表头数据
			fWriter.flush();
		} catch (Exception e) {
			if (fWriter != null) {
				try {
					fWriter.close();
				} catch (IOException ioe) {
				}
			}
			throw new StoreException("创建txt数据文件(" + info.txtFile + ")表头失败", e);
		}
	}

	/**
	 * 执行sqlldr操作
	 * 
	 * @throws
	 */
	private void runSqlldr() throws StoreException {
		// 添加了采集任务的开始入库时间,用于预防sqlldr超时
		CollectObjInfo collectInfo = this.getCollectInfo();

		int ret = -1;
		if (collectInfo != null) {
			Date now = new Date();
			collectInfo.setSqlldrTime(new Timestamp(now.getTime()));
		}
		//
		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
		String strOraclePassword = SystemConfig.getInstance().getDbPassword();
		int skip = 1;

		String strLogPath = (info != null && Util.isNotNull(info.logFile) ? FilenameUtils.normalize(info.logFile).replace("\\\\", "\\") : "");
		String cmd = String.format(
				"sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999 bindsize=20000000 rows=5000 readsize=20000000",
				strOracleUserName, strOraclePassword, strOracleBase, skip, info.ctlFile, info.badFile, strLogPath);

		ExternalCmd sqlldrCmd = null;
		// 判断是否为0,如果为0,不予考虑进程池机制
		if (SqlldrPool.maxCount == 0)
			sqlldrCmd = new ExternalCmd();
		else {
			sqlldrCmd = new SqlldrCmd();
		}
		sqlldrCmd.taskID = collectInfo.getTaskID();
		String key = String.format("[taskId-%s][%s]", collectInfo.getTaskID(), Util.getDateString(getDataTime()));
		String logCmd = cmd.replace(strOracleUserName, "*").replace(strOraclePassword, "*");
		log.debug(key + "当前执行的SQLLDR命令为:" + logCmd);
		Timestamp sqlldrStartTime = new Timestamp(System.currentTimeMillis());
		try {
			ret = sqlldrCmd.execute(cmd);
			// sqlldr返回码为0-4时为正常,不需要重试;返回码为128时，是由于sqlldr命令没有启动需要在新的线程里重试，另外这里只重试三次
			if (ret == 128 && sqlldrRetrytimes >=1){
				int retryTimes = 1;
				Random r = new Random();
				 do{
					int delayTime = retryTimes * 60 + r.nextInt(retryTimes * 20);
					//因为三次以后还不能提交成功的情况比较少，所以从第四次开始减少提交的时间间隔
					delayTime=delayTime%240;
					log.debug(key + "执行sqlldr命令失败,返回码:128,等待(" + delayTime + ")秒后将新启线程执进行第(" + retryTimes + ")次重试,重试的sqlldr命令为:" + logCmd);
					TimeUnit.SECONDS.sleep(delayTime);
					FutureTask<Integer> futureTask = new FutureTask<Integer>(new ReTryCmd(key, sqlldrCmd, logCmd));
					Thread thread = new Thread(futureTask, key);
					thread.start();
					ret = futureTask.get();
					retryTimes++;
				}while (ret == 128 && retryTimes <= sqlldrRetrytimes);
			}
		} catch (Exception e) {
			throw new StoreException(key + "执行sqlldr命令失败(" + cmd + ")", e);
		}
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			InputStream logIn = new FileInputStream(info.logFile);
			SqlldrResult result = analyzer.analysis(logIn);
			IOUtils.closeQuietly(logIn);
			if (result == null)
				return;

			log.debug(key + "ret=" + ret + ", SQLLDR日志分析结果: omcid=" + (SystemConfig.getInstance().isSPAS() ? collectInfo.spasOmcId : this.getOmcID())
					+ " 入库成功条数=" + result.getLoadSuccCount() + " 表名=" + result.getTableName() + " 数据时间=" + this.getDataTime() + " sqlldr日志="
					+ strLogPath);

			int oi = this.getOmcID();
			if (SystemConfig.getInstance().isSPAS()) {
				oi = Integer.parseInt(collectInfo.spasOmcId);
			}
			dbLogger.log(oi, result.getTableName(), this.getDataTime(), result.getLoadSuccCount(), this.getTaskID(), collectInfo.getGroupId(),
					parserdCount, sqlldrStartTime, ret);
		} catch (Exception e) {
			log.error(key + "ret=" + ret + ", sqlldr日志分析失败,文件名:" + info.logFile + ",原因: ", e);
			try {
				dbLogger.log(this.getOmcID(), this.param.getTable().getName().toUpperCase(), this.getDataTime(), -2, this.getTaskID(),
						collectInfo.getGroupId(), parserdCount, sqlldrStartTime, ret);
			} catch (Exception ex) {
				log.error(key + "sqlldr日志分析失败后,尝试向log_clt_insert补入记录,发生异常。", e);
			}
		}
		this.parserdCount = 0;

		// 是否删除日志
		if (SystemConfig.getInstance().isDeleteLog()) {
			delLog(ret);
		}
	}

	/**
	 * 删除sqlldr日志,但是如果sqlldr失败也就是有bad文件则保留其文件(包括对应的bad,ctl,log,txt文件)不删除
	 */
	private void delLog(int ret) {
		if (ret == 0 || ret == 2) {
			delFile(info.badFile);
			delFile(info.ctlFile);
			if (fWriter != null) {
				try {
					fWriter.close();
				} catch (IOException e) {
				}
			}
			delFile(info.txtFile);
		}
		if (ret == 0) {
			delFile(info.logFile);
		}
	}

	private void delFile(String fPath) {
		File f = new File(fPath);
		if (f.exists()) {
			if (!f.delete())
				log.debug("删除文件失败:" + f);
			// else
			// log.debug("文件删除成功:" + f);
		} else {
			// log.debug("待删除的文件不存在:" + f);
		}
	}

	private class ReTryCmd implements Callable<Integer> {

		private String key;

		private ExternalCmd sqlldrCmd;

		private String logCmd;

		ReTryCmd(String key, ExternalCmd sqlldrCmd, String logCmd) {
			this.key = key;
			this.sqlldrCmd = sqlldrCmd;
			this.logCmd = logCmd;
		}

		@Override
		public Integer call() throws Exception {
			int ret = 0;
			try {
				ret = sqlldrCmd.execute();
				if (ret <= -1 || ret >= 5) {
					log.debug(key + "在新线程中执行重试的sqlldr命令返回码:" + ret + ",重试的sqlldr命令为:" + logCmd);
				}
			} catch (Exception e) {
				throw new StoreException(key + "在新线程中执行重试的sqlldr命令失败(" + logCmd + ")", e);
			}
			return ret;
		}

	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
	}
}

class SqlldrInfo {

	String tbName; // 表名

	String txtFile;

	String logFile;

	String badFile;

	String ctlFile;
}
