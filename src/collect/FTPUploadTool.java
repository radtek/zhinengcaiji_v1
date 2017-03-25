package collect;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import task.CollectObjInfo;
import task.DevInfo;
import util.Util;

/**
 * <p>
 * 继承自{@link FTPTool}类，扩展了FTP上传的功能。
 * </p>
 * 
 * @author ChenSijiang 2010-11-8
 * @since 1.2
 */
public class FTPUploadTool extends FTPTool {

	// 所有需上传的文件
	protected List<File> uploadFiles = new ArrayList<File>();

	/**
	 * <p>
	 * 构造方法，调用父类的构造方法 。
	 * </p>
	 * 
	 * @param taskInfo
	 *            任务信息
	 * @see FTPTool#FTPTool(CollectObjInfo)
	 */
	public FTPUploadTool(CollectObjInfo taskInfo) {
		super(taskInfo);
	}

	public void test() throws Exception {
		File f = new File("D:\\Oracle\\ora90\\BIN\\orageneric9.dll");
		InputStream in = new FileInputStream(f);
		ftp.changeWorkingDirectory("/");
		System.out.println(ftp.storeFile("a.dll", in));
	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo info = new CollectObjInfo(123);
		info.setLastCollectTime(new Timestamp(Util.getDate1("2010-01-01 12:00:00").getTime()));
		DevInfo di = new DevInfo();
		di.setHostPwd("123");
		di.setHostUser("chensj");
		di.setIP("127.0.0.1");
		info.setDevInfo(di);
		info.setDevPort(21);
		FTPUploadTool t = new FTPUploadTool(info);
		t.login(2000, 3);
		t.test();
		t.disconnect();

	}
}
