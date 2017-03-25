package store;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

import store.gp.GpfdistStore;
import store.gp.GpfdistStoreParam;
import task.CollectObjInfo;
import templet.Table;
import util.Util;
import cn.uway.alarmbox.db.pool.DBUtil;
import exception.StoreException;
import framework.SystemConfig;

/**
 * 
 * StoreFactory
 * 
 * @author liangww 2012-9-10
 */
public class StoreFactory {

	/**
	 * sqlldrStore 类型
	 */
	public static final int SQLLDR_STORE_TYPE = 1;

	/**
	 * gpStore类型
	 */
	public static final int GP_STORE_TYPE = 2;

	/**
	 * 获取存储器
	 * 
	 * @param templetID
	 *            模板的id
	 * @param table
	 * @param collectInfo
	 * @return
	 * @throws Exception
	 */
	static public AbstractStore<?> getStore(int templetID, Table table, CollectObjInfo collectInfo,Timestamp lastCollectTime) throws StoreException {
		AbstractStore<?> store = null;

		SystemConfig systemConfig = SystemConfig.getInstance();
		int storeType = systemConfig.getStoreType();
		// TODO 目前还有两个地址没有调用getStore而直接使用SqlldrStoreParam
		if (storeType == SQLLDR_STORE_TYPE) {
			// liangww modify 2012-08-09 增加setTask
			SqlldrStoreParam sqlldrStoreParam = new SqlldrStoreParam(templetID, table);

			store = new SqlldrStore(sqlldrStoreParam);
			store.setCollectInfo(collectInfo);
			store.setTaskID(collectInfo.getTaskID());
			//collectInfo.getLastCollectTime()
			store.setDataTime(lastCollectTime);
			store.setOmcID(collectInfo.getDevInfo().getOmcID());
			store.setFlag(Util.getDateString_yyyyMMddHHmmssSSS(new Date()) + "_" + new Random().nextInt(Integer.MAX_VALUE));
		} else if (storeType == GP_STORE_TYPE) {
			GpfdistStoreParam gpfdistStoreParam = new GpfdistStoreParam();
			String driver = systemConfig.getStoreDbDriver();
			String url = systemConfig.getStoreDbUrl();
			String username = systemConfig.getStoreDbUserName();
			String password = systemConfig.getStoreDbPassword();
			Connection connection = null;
			try {
				connection = DBUtil.getConnection(driver, url, username, password);
			} catch (Exception e) {

				throw new StoreException("gpstore初始化时，获取connnetion失败", e);
			}

			gpfdistStoreParam.setCon(connection);
			gpfdistStoreParam.setDir(systemConfig.getGpfdistLocalPath());
			gpfdistStoreParam.setGpfdistRootUrl(systemConfig.getGpfdistRootUrl());
			gpfdistStoreParam.setTable(table);

			store = new GpfdistStore(gpfdistStoreParam);
			store.setCollectInfo(collectInfo);
			store.setTaskID(collectInfo.getTaskID());
			//collectInfo.getLastCollectTime()
			store.setDataTime(lastCollectTime);
			store.setOmcID(collectInfo.getDevInfo().getOmcID());
			store.setFlag(Util.getDateString_yyyyMMddHHmmssSSS(new Date()) + "_" + new Random().nextInt());
		} else {
			throw new StoreException("storeType:" + storeType + " not exists");
		}

		return store;
	}

}
