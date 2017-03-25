package access.special;

import java.io.File;
import java.sql.Timestamp;

import parser.eric.pm.WCDMAEricssonPerformanceParser;
import task.CollectObjInfo;
import task.DevInfo;
import tools.socket.SocketClientBean;
import tools.socket.SocketClientHelper;
import util.Util;
import framework.SystemConfig;

/**
 * <p>
 * 爱立信WCDMA性能处理专用类。
 * </p>
 * <p>
 * 爱立信WCDMA性能是XML文件，每15分钟生成一批。其中，RNC级文件一个，NODE-B级文件有350个左右。
 * </p>
 * 
 * @author ChenSijiang
 */
public class EricssonWcdmaPerformanceAccessor extends ManyFilesAccessor {

	private WCDMAEricssonPerformanceParser p;

	/**
	 * 构造方法，指定任务信息。
	 * 
	 * @param task
	 *            任务信息。
	 */
	public EricssonWcdmaPerformanceAccessor(CollectObjInfo task) {
		super(task);
		this.p = new WCDMAEricssonPerformanceParser(task);
	}

	/* 本实现中，没有共享的写入资源，所以不用同步。 */
	@Override
	protected synchronized boolean parse(File file) {
		if (file == null) {
			// 告知第三方来完成入库
			String ip = SystemConfig.getInstance().getEricPmSocketIp();
			int port = SystemConfig.getInstance().getEricPmSocketPort();
			if (Util.isNotNull(ip) && port != 0) {
				SocketClientBean bean = new SocketClientBean();
				bean.setIp(ip);
				bean.setPort(port);
				SocketClientHelper.getInstance().handleMessage(bean, p.getMessages());
				return true;
			}
			// sqlldr入库
			p.startSqlldr();
		} else {
			p.parse(file);
			file.delete();
		}
		return true;
	}

	public static void main(String[] args) throws Exception {

		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(201);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2012-03-14 20:30:00").getTime()));
		obj.setCollectPath("/eric/A20120809.1000+0800-1015+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ECR04,MeContext=WBJ00028_statsfile.xml");
		DevInfo di = new DevInfo();
		di.setHostPwd("123");
		di.setOmcID(201);
		di.setHostUser("chensj");
		di.setIP("127.0.0.1");
		obj.setDevInfo(di);

		ManyFilesAccessor a = new EricssonWcdmaPerformanceAccessor(obj);
		a.handle();

	}
}
