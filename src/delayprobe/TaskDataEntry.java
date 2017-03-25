package delayprobe;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

import task.CollectObjInfo;
import templet.AbstractTempletBase;
import templet.DBAutoTempletP;
import templet.DBAutoTempletP2;
import util.CommonDB;
import util.LogMgr;
import util.Util;
import collect.UwayFTPClient;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 描述一个任务的所有数据单元（即要采集的每一个文件或SELECT语句）
 * 
 * @author ChenSijiang 2010-08-05
 * @version 1.1
 */
public class TaskDataEntry {

	private CollectObjInfo taskInfo;

	// 保存上次扫描时的实例，用于比较
	private TaskDataEntry pre;

	// 比较成功的次数
	private int eqCount;

	// 比较的次数
	private int probeCount;

	// 标记加载数据是否成功
	private boolean isNoError;

	private List<DataEntry> entrys = new ArrayList<DataEntry>();

	private ProbeLogger probeLogger;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * 构造方法，根据任务信息，取到数据单元
	 * 
	 * @param taskInfo
	 */
	public TaskDataEntry(CollectObjInfo taskInfo) {
		this.taskInfo = taskInfo;
		isNoError = init();
	}

	public void addEntry(DataEntry de) {
		if (de != null && !entrys.contains(de)) {
			entrys.add(de);
		}
	}

	public List<DataEntry> getAll() {
		return entrys;
	}

	public ProbeLogger getProbeLogger() {
		return probeLogger;
	}

	public void setProbeLogger(ProbeLogger probeLogger) {
		this.probeLogger = probeLogger;
	}

	@SuppressWarnings("unchecked")
	private boolean init() {
		Connection con = null;
		try {
			// collectType为6，DBAuto方式采集，或者collectType为60，DBAuto2方式采集
			if (taskInfo.getCollectType() == 6 || taskInfo.getCollectType() == 60) {
				int parseTempletId = taskInfo.getParseTmpID();
				Result rs = CommonDB.queryForResult("select tempfilename from igp_conf_templet where tmpid=" + parseTempletId);
				String fileName = rs.getRows()[0].get("tempfilename").toString();

				AbstractTempletBase p = taskInfo.getCollectType() == 6 ? new DBAutoTempletP() : new DBAutoTempletP2();

				p.parseTemp(fileName);

				Map<String, Object> tables = (Map<String, Object>) p.getClass().getDeclaredMethod("getTemplets").invoke(p);

				con = CommonDB.getConnection(taskInfo.getTaskID(), taskInfo.getDBDriver(), taskInfo.getDBUrl(), taskInfo.getDevInfo().getHostUser(),
						taskInfo.getDevInfo().getHostPwd());
				if (con == null) {
					con = CommonDB.getConnection(taskInfo, 100, (byte) 3);
				}
				if (con == null) {
					throw new Exception("获取对方数据库连接失败");
				}
				Iterator<String> it = tables.keySet().iterator();
				while (it.hasNext()) {
					String selectStatement = null;
					String next = it.next();
					Object sql = tables.get(next).getClass().getDeclaredMethod("getSql").invoke(tables.get(next));
					sql = sql == null ? "" : sql.toString();
					Object condition = tables.get(next).getClass().getDeclaredMethod("getCondition").invoke(tables.get(next));
					condition = condition == null ? "" : condition.toString();
					if (Util.isNotNull(sql.toString())) {
						selectStatement = ConstDef.ParseFilePathForDB(sql.toString(), taskInfo.getLastCollectTime());
					} else if (Util.isNotNull(condition.toString())) {
						String tmp = ConstDef.ParseFilePathForDB(next, taskInfo.getLastCollectTime());
						selectStatement = ConstDef.ParseFilePathForDB("select * from " + tmp + " where " + condition.toString(),
								taskInfo.getLastCollectTime());
					} else {
						String tmp = ConstDef.ParseFilePathForDB(next, taskInfo.getLastCollectTime());
						selectStatement = ConstDef.ParseFilePathForDB("select * from " + tmp, taskInfo.getLastCollectTime());
					}

					long size = 0;

					try {
						if (CommonDB.tableExists(con, next, taskInfo.getTaskID())) {
							size = CommonDB.getRowCount(con, selectStatement);
							Thread.sleep(50);
						} else {
							size = -1;
						}
					} catch (Exception e) {
						throw e;
					}
					entrys.add(new DataEntry(next + " [实际采集所用语句:" + selectStatement + "]", size));
				}

			}
			// collectType为3，FTP文件方式采集
			else if (taskInfo.getCollectType() == ConstDef.COLLECT_TYPE_FTP && SystemConfig.getInstance().isProbeFTP()) {
				String encode = taskInfo.getDevInfo().getEncode();
				String[] collectPaths = taskInfo.getCollectPath().split(";");
				FTPClient ftp = new UwayFTPClient();
				ftp.connect(taskInfo.getDevInfo().getIP(), taskInfo.getDevPort());
				ftp.login(taskInfo.getDevInfo().getHostUser(), taskInfo.getDevInfo().getHostPwd());
				if (!SystemConfig.getInstance().isFtpPortMode())
					ftp.enterLocalPassiveMode();
				setFTPClientConfig(ftp);
				Set<String> tmp = new HashSet<String>();
				for (String path : collectPaths) {
					if (Util.isNull(path)) {
						continue;
					}
					List<String> list = Util.listFTPDirs(path, taskInfo.getDevInfo().getIP(), taskInfo.getDevPort(), taskInfo.getDevInfo()
							.getHostUser(), taskInfo.getDevInfo().getHostPwd(), encode, false, taskInfo.getParserID());
					tmp.addAll(list);
				}
				collectPaths = tmp.toArray(new String[0]);
				for (String path : collectPaths) {
					String fileName = ConstDef.ParseFilePath(path, taskInfo.getLastCollectTime());
					String o = fileName;
					fileName = Util.isNotNull(encode) ? new String(fileName.getBytes(encode), "iso_8859_1") : fileName;
					FTPFile[] fs = ftp.listFiles(fileName);
					if (fs.length == 0) {
						entrys.add(new DataEntry(o, -1));
					} else {
						for (FTPFile f : fs) {
							if (f != null) {
								if (o.lastIndexOf("/") >= 0) {
									String str = o.substring(0, fileName.lastIndexOf("/") + 1);
									entrys.add(new DataEntry(str + f.getName(), f.getSize()));
								} else {
									entrys.add(new DataEntry(Util.isNotNull(encode) ? new String(f.getName().getBytes("iso_8859_1"), encode) : f
											.getName(), f.getSize()));
								}
							}
						}
					}
				}
				ftp.disconnect();
			} else {
				throw new Exception("不支持的采集类型:" + taskInfo.getCollectType());
			}
		} catch (Exception e) {
			logger.error("获取采集目标时异常", e);
			return false;
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
		return true;
	}

	public boolean isNoError() {
		return isNoError;
	}

	public TaskDataEntry getPre() {
		return pre;
	}

	public void setPre(TaskDataEntry pre) {
		this.pre = pre;
	}

	public int getEqCount() {
		return eqCount;
	}

	public void setEqCount(int eqCount) {
		this.eqCount = eqCount;
	}

	public int getProbeCount() {
		return probeCount;
	}

	public void setProbeCount(int probeCount) {
		this.probeCount = probeCount;
	}

	private String formart(int max, int length) {
		if (length >= max) {
			return "    ";
		}
		StringBuilder sb = new StringBuilder();
		int diff = max - length;
		for (int i = 0; i < diff; i++) {
			sb.append(" ");
		}
		sb.append("    ");
		return sb.toString();
	}

	private int maxLength(List<DataEntry> list) {
		int max = 0;
		for (DataEntry de : list) {
			if (de.getName().length() >= max) {
				max = de.getName().length();
			}
		}
		return max;
	}

	/**
	 * 自动设置FTP服务器类型
	 */
	private void setFTPClientConfig(FTPClient ftp) {
		try {
			ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_NT));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_AS400));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_L8));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_MVS));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_NETWARE));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_OS2));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_OS400));
			} else {
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*"))) {
				ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_VMS));
			} else {
				return;
			}
		} catch (Exception e) {
			logger.error("配置FTP客户端时异常", e);
		}
	}

	public boolean compare() {
		boolean b = false;
		int preCount = 0;
		int thisCount = 0;
		int max = maxLength(getAll());
		for (DataEntry de : getAll()) {
			probeLogger.println("表名/文件名:" + de.getName() + formart(max, de.getName().length()) + "记录数/尺寸:"
					+ (de.getSize() == -1 ? "不存在" : de.getSize()));
			thisCount += de.getSize();
		}
		if (pre != null) {
			for (DataEntry de : pre.getAll()) {
				preCount += de.getSize();
			}
			if (preCount == thisCount) {
				eqCount++;
				b = true;
			} else {
				eqCount = 0;
			}
		}

		return b;
	}

	// public synchronized static boolean probeTask(CollectObjInfo taskInfo)
	// {
	// if ( taskInfo == null ) { return false; }
	// if ( taskInfo instanceof RegatherObjInfo ) { return false; }
	// int type = taskInfo.getCollectType();
	// int period = taskInfo.getPeriod();
	// int probeTime = taskInfo.getProbeTime();
	// if ( period != ConstDef.COLLECT_PERIOD_HOUR ) { return false; }
	// if ( type != 6 && type != 60 && type != 3 && type != 9 ) { return false;
	// }
	// if ( probeTime < 0 ) { return false; }
	// if ( SystemConfig.getInstance().isEnableDelayProbe() )
	// {
	// if ( type == 9 || type == 3 )
	// {
	// if ( !SystemConfig.getInstance().isProbeFTP() ) { return false; }
	// }
	//
	// String key = taskInfo.getTaskID() + "["
	// + Util.getDateString(taskInfo.getLastCollectTime()) + "]";
	// boolean isFirstProbe = firsts.containsKey(key) ? firsts.get(key) : true;
	//
	// if ( time % SystemConfig.getInstance().getProbeInterval() != 0
	// && !isFirstProbe ) { return false; }
	//
	// firsts.put(key, false);
	//
	// if ( errors.containsKey(key) ) { return false; }
	// TaskDataEntry newTde = new TaskDataEntry(taskInfo);
	// if ( !newTde.isNoError )
	// {
	// String log = key + ":执行探针时异常，此时间点不再使用探针";
	// logger.error(log);
	// newTde.setProbeLogger(new ProbeLogger(taskInfo.getTaskID()));
	// newTde.getProbeLogger().println(log);
	// newTde.getProbeLogger().dispose();
	// errors.put(key, null);
	// return false;
	// }
	// if ( taskEntrys.containsKey(taskInfo.getTaskID()) )
	// {
	// TaskDataEntry tde = taskEntrys.get(taskInfo.getTaskID());
	// newTde.probeCount = tde.probeCount + 1;
	// newTde.setPre(tde);
	// newTde.setEqCount(tde.getEqCount());
	// newTde.setProbeLogger(tde.getProbeLogger());
	// }
	// else
	// {
	// newTde.setProbeLogger(new ProbeLogger(taskInfo.getTaskID()));
	// newTde.probeCount = 1;
	// }
	// boolean bEq = newTde.compare();
	// String result = bEq ? "相等" : "不相等";
	// String log = "任务号: "
	// + taskInfo.getTaskID()
	// + ", 时间点: "
	// + Util.getDateString(taskInfo.getLastCollectTime())
	// + ", 任务设置的延时(分钟):"
	// + taskInfo.getCollectTimePos()
	// + ", 探测间隔(分钟):"
	// + SystemConfig.getInstance().getProbeInterval()
	// + ", 探测开始时间："
	// + probeTime
	// + "分"
	// + ", 探测次数: "
	// + newTde.probeCount
	// + (newTde.getPre() == null ? "(首次探测，尚无上次数据，不能比较)" : ", 比较结果: "
	// + result);
	// newTde.getProbeLogger().println(log);
	// logger.debug(log);
	// newTde.getProbeLogger().println("");
	// taskEntrys.put(taskInfo.getTaskID(), newTde);
	// boolean b = newTde.isNoError()
	// && newTde.getEqCount() >=
	// SystemConfig.getInstance().getDelayProbeTimes();
	// if ( b )
	// {
	// log = key + "经过"
	// + SystemConfig.getInstance().getDelayProbeTimes()
	// + "次比较，数据量未变化，确认可以开始采集";
	// newTde.getProbeLogger().println(log);
	// logger.debug(log);
	// newTde.getProbeLogger().println("");
	// newTde.getProbeLogger().dispose();
	// taskEntrys.remove(taskInfo.getTaskID());
	// }
	// return b;
	// }
	// else
	// {
	// return false;
	// }
	// }

	public static void main(String[] args) throws Exception {
		CollectObjInfo info = new CollectObjInfo(433);
		info.setLastCollectTime(new Timestamp(Util.getDate1("2010-07-10 00:00:00").getTime()));
		info.setCollectType(60);
		info.setParseTmpID(731);
		TaskDataEntry t = new TaskDataEntry(info);
		System.out.println(t.getAll());
	}
}
