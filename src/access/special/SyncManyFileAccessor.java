package access.special;

import java.io.File;

import org.apache.log4j.Logger;

import task.CollectObjInfo;
import util.LogMgr;

public class SyncManyFileAccessor extends ManyFilesAccessor {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	public SyncManyFileAccessor(CollectObjInfo task) {
		super(task);
	}

	@Override
	public boolean handle() {
		log.debug(logKey + "W爱立信性能同步 - 请求锁");
		synchronized (SyncManyFileAccessor.class) {
			log.debug(logKey + "W爱立信性能同步 - 获取到锁");
			boolean b = super.handle();
			log.debug(logKey + "W爱立信性能同步 - 释放锁");
			return b;
		}

	}

	@Override
	protected boolean parse(File file) {
		return false;
	}

}
