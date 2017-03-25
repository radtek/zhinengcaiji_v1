package store.gp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import store.AbstractStore;
import task.CollectObjInfo;
import util.LogMgr;
import util.Util;
import cn.uway.alarmbox.db.pool.DBUtil;
import exception.StoreException;
import framework.SystemConfig;

/**
 * Gpfdist 入库中方式。 GpStore
 * 
 * @author liangww 2012-6-30<br>
 * @version 1.0.0<br>
 *          1.0.1 liangww 2012-09-12 修改入库时，数据文件可以数据库中表格的字段顺序不一致<br>
 */
public class GpfdistStore extends AbstractStore<GpfdistStoreParam> {

	private final static String EXT_TABLE_PREFIX = "igp_ext_";

	private final static String TMP_TABLE_PREFIX = "igp_tmp_";

	private static final String CREATE_EXT_TABLE_SQL = "CREATE EXTERNAL TABLE %s ( like %s )\n" + " LOCATION (\n" + "    '%s'\n" + ")\n"
			+ " FORMAT 'text' (delimiter '%s' null '' escape '\"' )\n" + "ENCODING 'UTF8'\n";

	// + "LOG ERRORS INTO %s SEGMENT REJECT LIMIT 100 PERCENT";

	private static long id = 0;

	/**
	 * 
	 * @return
	 */
	public static synchronized long getID() {
		return ++id;
	}

	/**
	 * 
	 * @param srcTableName
	 * @param flag
	 * @return
	 */
	static String getExternalTableName(String srcTableName, String flag) {
		return EXT_TABLE_PREFIX + srcTableName + "_" + flag + getID();
	}

	/**
	 * @param srcTableName
	 * @param flag
	 * @return
	 */
	static String getTmpTableName(String srcTableName, String flag) {
		return TMP_TABLE_PREFIX + srcTableName + "_" + flag + getID();
	}

	private final Logger logger = LogMgr.getInstance().getSystemLogger();

	private List<String> dataBuf = new ArrayList<String>();

	private FileOutputStream out;		//

	private String filePath = null;				//

	public GpfdistStore(GpfdistStoreParam gpStoreParam) {
		super(gpStoreParam);
	}

	@Override
	public void write(String data) throws StoreException {
		// TODO Auto-generated method stub
		dataBuf.add(data);
		if (dataBuf.size() > 1000) {
			flush();
		}
	}

	@Override
	public void flush() throws StoreException {
		try {
			String nowStr = Util.getDateString(new Date());
			String sysFieldValue = this.getOmcID() + param.getSplit() + nowStr + param.getSplit() + Util.getDateString(this.getDataTime());

			for (String str : dataBuf) {
				out.write((str + sysFieldValue + "\n").getBytes("utf-8"));
			}
			out.flush();
		} catch (Exception e) {
			if (out != null) {
				try {
					out.close();
				} catch (IOException ioe) {
				}
			}
			throw new StoreException("数据写入文件(" + new File(param.getDir(), this.filePath).getAbsolutePath() + ")时异常.", e);
		} finally {
			dataBuf.clear();
		}
	}

	@Override
	public void commit() throws StoreException {
		// TODO Auto-generated method stub
		CollectObjInfo collectInfo = this.getCollectInfo();
		flush();

		String sql = null;
		String key = String.format("[taskId-%s][%s]", getTaskID(), Util.getDateString(this.getCollectInfo().getLastCollectTime()));
		// 清除表
		String extTableName = getExternalTableName(param.getTableName(), getFlag());
		String tmpTableName = getTmpTableName(param.getTableName(), getFlag());

		String columns = param.getTable().listColumnNames(",");

		String[] sqls = {
				// 先drop 外部和临时表
				String.format("drop external table if exists  %s CASCADE;", extTableName),
				String.format("drop table if exists  %s CASCADE;", tmpTableName),

				String.format("CREATE TABLE %s AS SELECT %s omcid,collecttime,stamptime from %s", tmpTableName, columns, param.getTableName()),

				String.format(CREATE_EXT_TABLE_SQL, extTableName, tmpTableName, param.getGpfdistRootUrl() + "/" + this.filePath, param.getSplit()),

				String.format("insert into %s(%s omcid,collecttime,stamptime) select %s omcid,collecttime,stamptime  from  %s", param.getTableName(),
						columns, columns, extTableName),};

		// TODO gpFdistStore不好统计个数的，目前只是算insert into时影响的个数 liangww 2012-09-11
		int num = 0;
		//String tableName = param.getTableName().toUpperCase();

		try {
			param.getCon().setAutoCommit(false);
			for (int i = 0; i < sqls.length; i++) {
				sql = sqls[i];
				num = DBUtil.executeUpdateO(param.getCon(), sql);
				param.getCon().commit();
				log.debug(key + " sql=" + sql);
			}

			log.debug(key + " gpfdist日志分析结果: omcid=" + (SystemConfig.getInstance().isSPAS() ? collectInfo.spasOmcId : this.getOmcID()) + " 入库成功条数="
					+ num + " 表名=" + param.getTableName() + " 数据时间=" + this.getDataTime());

			collectInfo.log("入库", "gpfdist日志分析结果: omcid=" + (SystemConfig.getInstance().isSPAS() ? collectInfo.spasOmcId : this.getOmcID())
					+ " 入库成功条数=" + num + " 表名=" + param.getTableName());

		} catch (Exception e) {
			logger.warn(key + "执行sql命令失败(" + sql + ")", e);
			try {
				param.getCon().rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
			}
			throw new StoreException(key + "执行sql命令失败(" + sql + ")", e);
		} finally {
			// 完成后把外部表和临时表
			try {
				DBUtil.executeUpdateO(param.getCon(), sqls[0]);
			} catch (Exception e) {
				logger.warn("清除外部表出错,sql:" + sqls[0], e);
			}

			try {
				DBUtil.executeUpdateO(param.getCon(), sqls[1]);
			} catch (Exception e) {
				logger.warn("清除临时表出错,sql:" + sqls[1], e);
			}
		}// finally
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		super.close();
		try {
			this.getParam().getCon().close();
		} catch (Exception e) {
			// TODO: handle exception
		}

		IOUtils.closeQuietly(this.out);

		// 删除文件
		new File(param.getDir(), this.filePath).delete();
	}

	@Override
	public void open() throws StoreException {
		// TODO Auto-generated method stub
		String tbName = param.getTableName();
		Timestamp dataTime = this.getDataTime();
		String strDateTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

		filePath = getTaskID() + "/" + tbName + "_" + strDateTime + "_" + (getFlag() == null ? "" : "_" + getFlag()) + ".txt";

		File file = new File(param.getDir(), filePath);
		try {
			file.getParentFile().mkdirs();
			if (!file.exists()) {
				file.createNewFile();
			}
			out = new FileOutputStream(file, true);
		} catch (Exception e) {
			// TODO: handle exception
			String key = String.format("[taskId-%s][%s]", getTaskID(), Util.getDateString(this.getCollectInfo().getLastCollectTime()));
			throw new StoreException(key + "流文件打开失败", e);
		}
	}

}
