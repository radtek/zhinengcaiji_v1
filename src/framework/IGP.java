package framework;

import java.io.File;
import java.io.FileFilter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import org.apache.log4j.Logger;

import parser.dt.RegionCache;
import util.CommonDB;
import util.DbPool;
import util.LogMgr;
import util.SpasUploadLoggingRec;
import util.Util;
import web.WebMgr;
import alarm.AlarmMgr;
import collect.FileRetention;
import collect.LoadConfMapDevToNe;
import console.ConsoleMgr;

/**
 * IGP主程序类
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class IGP {

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public static final Date SYS_START_TIME = new Date(); // 系统启动时间

	public IGP() {
		super();
	}

	/**
	 * 程序入口函数
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		runIGP();

	}

	/* 检查igp的lib目录中，是否有多个igp1.jar */
	private static void checkLib() {
		try {
			File libDir = new File("." + File.separator + "lib" + File.separator);
			if (libDir.exists() && libDir.isDirectory()) {
				File[] fs = libDir.listFiles(new FileFilter() {

					@Override
					public boolean accept(File pathname) {
						return pathname.getName().toLowerCase().contains("igp1") || pathname.isDirectory();
					}
				});
				if (fs.length > 1) {
					StringBuilder list = new StringBuilder();
					for (File f : fs) {
						list.append(f.getName()).append(", ");
					}
					list.delete(list.length() - 1, list.length());
					log.warn("！！！！！！！！！！！！！！！！！！！！！！！！！");
					log.warn("！注意：当前lib目录中可能有多个igp1.jar文件，或者有多余的目录。请不要在更新时将igp1.jar备份在lib目录，或在lib目录中新建子目录。请检查lib目录中的以下对象 - " + list);
					log.warn("！！！！！！！！！！！！！！！！！！！！！！！！！");
				}
			}
		} catch (Exception e) {
		}

	}

	public static void runIGP() {
		log.info("系统启动.");
		SystemConfig.getInstance();

		checkDataBase();

		Version ver = Version.getInstance();
		String strVer = ver.getExpectedVersion();
		if (!ver.isRightVersion()) {
			log.error("系统退出. 原因:版本号不一致. 内部版本号:" + strVer);
			System.out.println(1);
		}
		log.info("版本号:" + strVer);
		//在控制台看到版本号码
		System.out.print("版本号:" + strVer);

		// 启动控制台模块
		try {
			ConsoleMgr.getInstance().start();
		} catch (Exception e) {
			log.error("采集系统启动失败,原因: 控制台模块启动异常. ", e);
		}

		// 启动控制台模块
		try {
			WebMgr.getInstance().startServer();
		} catch (Exception e) {
			log.error("Web模块启动失败,原因: ", e);
		}

		new FileRetention().start();

		// 启动定时器加载网元缓存
		new LoadConfMapDevToNe().starts();

		new SpasUploadLoggingRec().start();

		new TempFileCleaner().start();

		Util.printEnvironmentInfo();

		PBeanMgr.getInstance();// 加载PBeanMgr

		AlarmMgr.getInstance();// 启动告警模块

		// 启动数据文件存活时间管理模块
		DataLifecycleMgr.getInstance().start();

		RegionCache.reLoad();

		checkLib();
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.startScan();
	}

	@Override
	public String toString() {
		return "IGP";
	}

	// 检查数据库连接是否可用；
	// 检查task表和device表的id是否有重复，
	// 因为有些地方可能没主键，而程序查询时，
	// 使用igp_conf_device和igp_conf_task表作为主表进行左联接，会导致读取到重复任务。
	// 20120524 chensijiang.
	private static void checkDataBase() {
		Connection con = null;
		ResultSet rs = null;
		PreparedStatement st = null;
		try {
			con = DbPool.getConn();
			if (con == null) {
				log.fatal("数据库连接不可用，程序退出。");
				System.exit(1);
			}
			String sql = "select task_id,count(*) as counts from igp_conf_task group by task_id having count(*)>1";
			st = con.prepareStatement(sql);
			/* 检查TASK表中是否有重复的TASK_ID，有些地方TASK表可能没建主键。 */
			rs = st.executeQuery();
			if (rs.next()) {
				do {
					log.fatal("igp_conf_task表中，TASK_ID有重复，程序将退出，TASK_ID=" + rs.getLong("task_id") + "，重复条数=" + rs.getInt("counts"));
				} while (rs.next());
				CommonDB.close(rs, st, con);
				System.exit(1);
			}
			CommonDB.close(rs, null, null);
			/* 检查device表中是否有重复的dev_id，有些地方device表可能没建主键。 */
			rs = st.executeQuery("select dev_id,count(*) as counts from igp_conf_device group by dev_id having count(*)>1");
			if (rs.next()) {
				do {
					log.fatal("igp_conf_device表中，DEV_ID有重复，程序将退出，DEV_ID=" + rs.getLong("dev_id") + "，重复条数=" + rs.getInt("counts"));
				} while (rs.next());
				CommonDB.close(rs, st, con);
				System.exit(1);
			}
		} catch (Exception e) {
		} finally {
			CommonDB.close(rs, st, con);
		}
	}
}
