package collect;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.LogMgr;

public class SFTPPoolManager implements PoolManager {

	protected static Logger logger = LogMgr.getInstance().getSystemLogger();

	private Map<String,SFTPClientPool> ftpPoolMap = new HashMap<String,SFTPClientPool>();

	private static class SFTPPoolManagerHolder {
		private static SFTPPoolManager instance = new SFTPPoolManager();
	}

	// 私有化构造方法
	private SFTPPoolManager() {
	}

	public static SFTPPoolManager getInstance() {
		return SFTPPoolManagerHolder.instance;
	}

	public synchronized SFTPClient getFTPClient(String ftpId) throws Exception{
		SFTPClientPool ftpClientPool = ftpPoolMap.get(ftpId);
		if(ftpClientPool == null){
			return null;
		}
		return ftpClientPool.getSftpClient();
	}
	
	/**
	 * 登录FTP
	 * 
	 * @param pool
	 *            FTP连接池
	 * @param tryTimes
	 *            最大重试次数
	 * @return 是否登录成功
	 */
	public SFTPClient login(CollectObjInfo info) {
		int tryTimes = 3;//sftpInfo.getLoginTryTimes();
		if (tryTimes < 0) {
			tryTimes = 3;
		}
		SFTPClient sFtpClient = null;
		for (int i = 0; i < tryTimes; i++) {
			try {
				sFtpClient = this.getFTPClient(info.getDevInfo().getIP());
			} catch (Exception e) {
				logger.debug("尝试从FTP连接池获取登陆链接失败", e);
			}
			if (sFtpClient != null) {
				logger.debug("FTP连接获取登陆链接成功");
				return sFtpClient;
			}
			logger.debug("尝试重新获取链接，次数:" + (i + 1));
		}
		return null;
	}

	public void addPool(CollectObjInfo info) throws Exception{
		if(ftpPoolMap.get(info.getDevInfo().getIP()) != null){
			return;
		}
		SFTPClientPool pool = new SFTPClientPool(info);
		ftpPoolMap.put(info.getDevInfo().getIP(), pool);
	}
	
	public SFTPClientPool getPool(CollectObjInfo info){
		return ftpPoolMap.get(info.getDevInfo().getIP());
	}

}
