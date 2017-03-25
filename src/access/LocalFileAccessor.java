package access;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

import util.DeCompression;
import util.ExternalCmd;
import util.Parsecmd;
import util.Util;
import framework.ConstDef;
import framework.DataLifecycleMgr;
import framework.SystemConfig;

/**
 * 本地文件数据接入器
 * 
 * @author YangJian
 * @since 3.0
 */
public class LocalFileAccessor extends AbstractAccessor {

	public LocalFileAccessor() {
		super();
	}

	@Override
	public boolean access() throws Exception {
		long taskID = this.getTaskID();
		// 需要采集的文件,多个文件以;间隔
		String[] strSubPath = this.getDataSourceConfig().getDatas();
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

		// 解析文件内容
		ArrayList<String> arrfileList = new ArrayList<String>();
		for (String subPath : strSubPath) {
			if (Util.isNull(subPath))
				continue;

			String strSubFilePath = ConstDef.ParseFilePath(subPath.trim(), taskInfo.getLastCollectTime());
			// 根据文件名建立目录
			String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strSubFilePath);
			File oldf = new File(strSubFilePath);
			if (!oldf.exists() || !oldf.isFile()) {
				log.debug(name + "：文件：" + strSubFilePath + "不存在");
				continue;
			}

			strTempPath = oldf.getPath();
			String[] strFileName = strTempPath.split(";");

			for (int j = 0; j < strFileName.length; ++j) {
				if (strFileName[j] == null)
					continue;

				// 如果是压缩文件，则需要先解压
				if (Util.isZipFile(strFileName[j]))
					// 解压根据当前的时间，周期类型保存原始数据
					// liangww modify 2012-06-04 修改decompress做出相应调整
					arrfileList = DeCompression.decompress(taskID, taskInfo.getParseTemplet(), strFileName[j], taskInfo.getLastCollectTime(),
							taskInfo.getPeriod(), true);
				else
					arrfileList.add(strFileName[j]);
			}

			// ftp下载文件之后的Shell命令
			String strShellCmdPrepare = taskInfo.getShellCmdPrepare();
			if (Util.isNotNull(strShellCmdPrepare)) {
				boolean bSuccess = Parsecmd.ExecShellCmdByFtp(strShellCmdPrepare, taskInfo.getLastCollectTime());
				if (!bSuccess)
					log.error(name + "：文件采集执行ShellCmdPrepare命令失败.");
			}

		}// 多个下载子任务
		if (arrfileList == null || arrfileList.isEmpty())
			return false;
		log.debug(name + "：解压缩文件个数:" + arrfileList.size());
		log.debug(name + ": 解析类型=" + taskInfo.getParseTmpType());
		// 给文件打时间戳
		Date dataTime = taskInfo.getLastCollectTime();

		// 给文件打时间戳
		for (String fileName : arrfileList) {
			parser.setDsConfigName(fileName);
			DataLifecycleMgr.getInstance().doFileTimestamp(fileName, dataTime);
			// 解析
			log.debug(name + "：localFile,当前要解析的文件为:" + fileName);
			parser.setFileName(fileName);
			try {
				parser.parseData();
			} catch (Exception e) {
				// 解析失败不需要补采
				log.error(name + ": 文件解析失败(" + fileName + "),原因:", e);
				continue;
			}
		}
		return true;
	}

	@Override
	public void configure() throws Exception {
		// do nothing
	}

	@Override
	public boolean doAfterAccess() throws Exception {
		boolean flag = false;
		// 采集之后执行的Shell命令
		String cmd = taskInfo.getShellCmdFinish();
		if (Util.isNotNull(cmd)) {
			flag = new ExternalCmd().execute(cmd) == 0 ? true : false;
		}
		return flag;
	}

}
