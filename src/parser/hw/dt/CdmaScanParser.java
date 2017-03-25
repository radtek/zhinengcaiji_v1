package parser.hw.dt;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import parser.Parser;
import parser.hw.dt.bean.CltDtFileExplain;
import parser.nbi.check.bean.FilesMgrServerInfo;
import task.CollectObjInfo;
import task.DevInfo;
import task.RegatherObjInfo;
import task.TaskMgr;
import templet.TempletBase;
import templet.TempletRecord;
import util.DbPool;
import util.LogMgr;
import util.Util;
import cn.uway.alarmbox.db.pool.DBUtil;
import collect.FTPTool;
import framework.Factory;
import framework.SystemConfig;

/**
 * 用户手工导入路测文件扫描解析类<br>
 * 通过扫描 用户手工上传日志表(CLT_DT_FILE_EXPAND) 增加对用户手工导入路测文件支持 CdmaScanParser
 * 
 * @author lijiayu
 * @date 2014年8月5日
 */
public class CdmaScanParser extends Parser{

	private static final Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	// 扫描待处理的路程文件
	private static final String SELECT_SCAN_FILE_EXPLAIN = "SELECT FILE_NAME, USER_ID, USER_NAME, UPDATE_TIME, AUTO, FILE_FTP_PATH, FILE_AUTO_2_DELETE, DEAL_COMPLETE FROM CLT_DT_FILE_EXPAND WHERE DEAL_COMPLETE=0 AND FILE_AUTO_2_DELETE=1";

	// 更新待处理的路程文件
	private static final String UPDATE_SCAN_FILE_EXPLAIN = "UPDATE CLT_DT_FILE_EXPAND SET DEAL_COMPLETE = 1 WHERE FILE_AUTO_2_DELETE=1 AND FILE_NAME=? AND USER_ID=? AND FILE_FTP_PATH=?";

	// 全国通一下载路测手工导入的元素文件ftp配置表，语句由产品组提供
	private static final String SELECT_FTP_INFO = "select serveraddress,serverport,username,password from mod_filesmgr_serverinfo where protocal = 'DTCQT'";

	// 自定义文件路径
	private static final String MYDIR = "clt_dt_file_explain_scan";

	// 待处理的文件信息
	private List<CltDtFileExplain> needDealList;

	// FTP 连接信息
	private FilesMgrServerInfo ftpInfo = null;

	private CollectObjInfo task;

	private String logKey;

	private String stampTime;

	private boolean regatherFlag = false;

	private FTPTool ftpTool;

	public CdmaScanParser(){

	}

	public CdmaScanParser(CollectObjInfo task){
		this.task = task;
		long id = task.getTaskID();
		if(task instanceof RegatherObjInfo)
			id = task.getKeyID() - 10000000;
		this.stampTime = Util.getDateString(task.getLastCollectTime());
		this.logKey = task.getTaskID() + "-" + id;
		this.logKey = String.format("[%s][%s]", this.logKey, stampTime);
		// 初始化
		inits();
	}

	public boolean parse(){
		LOGGER.info("开始处理用户手工导入路测文件");
		long curMillionsecond = System.currentTimeMillis();

		// 加载待处理的手工导入的文件信息
		loadNeedDealFile();
		// 如果没加载到待处理的信息，直接返回，不处理
		if(null == needDealList || needDealList.isEmpty()){
			LOGGER.info("用户手工导入路测文件扫描解析类,没加载到待处理信息");
			return false;
		}
		// 获取 FTP 连接信息
		loadFtpInfo();
		if(null == ftpInfo){
			LOGGER.info("用户手工导入路测文件扫描解析类,没加载到连接的FTP信息!");
			return false;
		}
		// 从FTP上下载到手动上传的文件并解析文件
		try{
			downloadFilesAndParser();
		}catch(Exception e){
			LOGGER.error("处理用户手工导入路测文件失败!" + e.getMessage());
			return false;
		}finally{
			ftpTool.disconnect();
			// 清理
			clear();
		}
		LOGGER.info("处理用户手工导入路测文件完成,耗时:" + (System.currentTimeMillis() - curMillionsecond));
		return true;
	}

	private void inits(){
		needDealList = new ArrayList<CltDtFileExplain>();
	}

	private void clear(){
		needDealList.clear();
	}

	/**
	 * 加载待处理的手工导入的文件信息
	 */
	private void loadNeedDealFile(){
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try{
			conn = DbPool.getConn();
			ps = conn.prepareStatement(SELECT_SCAN_FILE_EXPLAIN);
			rs = ps.executeQuery();
			while(rs.next()){
				CltDtFileExplain fileExplain = new CltDtFileExplain();
				fileExplain.setFileName(rs.getString("FILE_NAME"));
				fileExplain.setUserId(rs.getString("USER_ID"));
				fileExplain.setUserName(rs.getString("USER_NAME"));
				fileExplain.setUpdateTime(rs.getTimestamp("UPDATE_TIME"));
				fileExplain.setAuto(rs.getInt("AUTO"));
				fileExplain.setFileFtpPath(rs.getString("FILE_FTP_PATH"));
				fileExplain.setFileAuto2Delete(rs.getString("FILE_AUTO_2_DELETE"));
				fileExplain.setDealComplete(rs.getInt("DEAL_COMPLETE"));
				needDealList.add(fileExplain);
			}
		}catch(SQLException e){
			LOGGER.error("执行SQL:[" + SELECT_SCAN_FILE_EXPLAIN + "]出现异常");
		}finally{
			DBUtil.close(rs, ps, conn);
		}
	}

	/**
	 * 获取 FTP 连接信息
	 */
	private void loadFtpInfo(){
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try{
			conn = DbPool.getConn();
			ps = conn.prepareStatement(SELECT_FTP_INFO);
			rs = ps.executeQuery();
			// 按产品的说法只有一条数据，全国统一
			if(rs.next()){
				ftpInfo = new FilesMgrServerInfo();
				ftpInfo.setSERVERADDRESS(trim(rs.getString("serveraddress")));
				ftpInfo.setSERVERPORT(rs.getInt("serverport"));
				ftpInfo.setUSERNAME(trim(rs.getString("username")));
				ftpInfo.setPASSWORD(trim(rs.getString("password")));
			}
		}catch(SQLException e){
			LOGGER.error("执行SQL:[" + SELECT_FTP_INFO + "]出现异常");
		}finally{
			DBUtil.close(rs, ps, conn);
		}
	}

	/**
	 * 处理null，并去空格
	 * 
	 * @param str
	 * @return
	 */
	private String trim(String str){
		return null == str ? "" : str.trim();
	}

	/**
	 * 下载文件，并解析
	 * 
	 * @throws Exception
	 */
	private void downloadFilesAndParser() throws Exception{
		// 重构task 设置FTP连接信息
		reBuildTaskInfo();
		// 建立连接
		ftpTool = new FTPTool(task);
		if(!loginForFTP())
			return;
		for(CltDtFileExplain fileExplain : needDealList){
			String localPath = SystemConfig.getInstance().getCurrentPath() + "/" + MYDIR;
			ftpTool.downFile(fileExplain.getFileFtpPath() + "/" + fileExplain.getFileName(), localPath);
			File tmpFile = new File(localPath + "/" + fileExplain.getFileName());
			// 下载失败
			if(!tmpFile.exists()){
				LOGGER.info("文件:" + tmpFile + " 下载失败,请检查FTP连接与文件路径是否配置正确!");
				continue;
			}
			// 调用解析类 解析文件
			boolean paserFlag = invokeHwdtCdmaParser(tmpFile.getAbsolutePath());
			// 更新状态
			if(paserFlag){
				updateState(fileExplain);
			}
		}

	}

	/**
	 * 登录到FTP.
	 * 
	 * @return 是否登录成功。
	 */
	private boolean loginForFTP(){
		if(ftpTool.login(2000, 3)){
			LOGGER.info(logKey + "FTP登录成功");
			return true;
		}else{
			LOGGER.error(logKey + "FTP登录失败");
			synchronized(this){
				if(!regatherFlag){
					TaskMgr.getInstance().newRegather(task, "", "用户手工导入路测文件扫描解析类：多次登陆失败，进行补采");
					regatherFlag = true;
				}
			}
			return false;
		}
	}

	/**
	 * 重构task 设置FTP连接信息
	 */
	private void reBuildTaskInfo(){
		DevInfo dev = task.getDevInfo();
		task.setDevPort(ftpInfo.getSERVERPORT());
		dev.setIP(ftpInfo.getSERVERADDRESS());
		dev.setHostUser(ftpInfo.getUSERNAME());
		dev.setHostPwd(ftpInfo.getPASSWORD());
	}

	/**
	 * 调用解析方法解析
	 * 
	 * @param filePathName 全路径的文件名
	 * @throws Exception
	 */
	private boolean invokeHwdtCdmaParser(String filePathName) throws Exception{
		LOGGER.info("开始调用解析类解析,本次要解析的文件为:" + filePathName);
		CollectObjInfo info = new CollectObjInfo(this.task.getTaskID());
		// 重设解析此文件需要依赖的Templet对象
		TempletRecord distTmpRecord = new TempletRecord();
		distTmpRecord.setId(100000);
		distTmpRecord.setType(8003);
		distTmpRecord.setName("入库");
		distTmpRecord.setEdition("V1");
		distTmpRecord.setFileName("clt_c_hw_dt_disk.xml");
		TempletBase distributeTemplet = Factory.createTemplet(distTmpRecord);
		info.setDistributeTemplet(distributeTemplet);
		info.setDevInfo(this.task.getDevInfo());
		info.setLastCollectTime(this.task.getLastCollectTime());
		Parser parser = new HwDtCdmaParser();
		parser.setFileName(filePathName);
		parser.setCollectObjInfo(info);
		return parser.parseData();
	}

	/**
	 * 更新状态
	 * 
	 * @param fileExplain CltDtFileExplain 实体类对象
	 */
	private void updateState(CltDtFileExplain fileExplain){
		Connection conn = null;
		PreparedStatement ps = null;
		try{
			conn = DbPool.getConn();
			ps = conn.prepareStatement(UPDATE_SCAN_FILE_EXPLAIN);
			int index = 1;
			ps.setString(index++, fileExplain.getFileName());
			ps.setString(index++, fileExplain.getUserId());
			ps.setString(index++, fileExplain.getFileFtpPath());
			ps.executeUpdate();
		}catch(SQLException e){
			LOGGER.error("执行SQL:[" + UPDATE_SCAN_FILE_EXPLAIN + "]出现异常");
		}finally{
			DBUtil.close(null, ps, conn);
		}
	}

	@Override
	public boolean parseData() throws Exception{
		return true;
	}
}
