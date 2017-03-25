package access.special;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.al.w.pm.WCDMA_AlcateLucentPerformanceParser;
import task.CollectObjInfo;
import task.DevInfo;
import util.DeCompression;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * W网阿朗性能。
 * 
 * @author ChenSijiang
 */
public class AlcatelLucentWcdmaPerformanceAccessor extends ManyFilesAccessor {

	WCDMA_AlcateLucentPerformanceParser parser;

	public AlcatelLucentWcdmaPerformanceAccessor(CollectObjInfo task) {
		super(task);
		this.task = task;
		parser = new WCDMA_AlcateLucentPerformanceParser(task);
	}

	@Override
	protected synchronized boolean parse(File file) {
		boolean b = false;
		if (file == null) {
			b = parser.startSqlldr();
			parser = new WCDMA_AlcateLucentPerformanceParser(task);
			return b;
		}

		File destFile = null;
		if (Util.isZipFile(file.getAbsolutePath())) {
			// destFile = decompGZ(file);
			// liangww modify 2012-07-09 修改成用DeCompression解压
			try {
				List<String> paths = DeCompression.decompress(task.getTaskID(), task.getParseTemplet(), file.getAbsolutePath(),
						task.getLastCollectTime(), task.getPeriod(), true);
				if (paths != null && paths.size() > 0) {
					destFile = new File(paths.get(0));
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
				logger.error("解压异常 - " + file.getAbsolutePath(), e);
			}
		} else {
			destFile = file;
		}

		if (destFile != null) {
			// liangww modify 2012-06-26 修改解析的是目标文件，而不是原始文件
			parser.parse(destFile);
			b = true;
			File parentFile = destFile.getParentFile();
			if (SystemConfig.getInstance().isDeleteWhenOff())
				destFile.delete();
			// 删除父文件夹
			if (parentFile != null && parentFile.exists())
				parentFile.delete();
		}

		return b;
	}

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	protected static File decompGZ(File gzFile) {
		GZIPInputStream in = null;
		OutputStream out = null;
		File f = null;
		try {
			String inFilename = gzFile.getAbsolutePath();
			in = new GZIPInputStream(new FileInputStream(inFilename));
			String outFilename = FilenameUtils.getFullPath(gzFile.getAbsolutePath()) + FilenameUtils.getBaseName(gzFile.getAbsolutePath());
			f = new File(outFilename);
			out = new FileOutputStream(outFilename);
			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
		} catch (Exception e) {
			logger.error("解压异常 - " + gzFile.getAbsolutePath(), e);
			return null;
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
		gzFile.delete();
		return f;
	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(302);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-07-06 20:00:00").getTime()));
		obj.setCollectPath("/al/nodeb/nodeb.zip");
		DevInfo di = new DevInfo();
		di.setHostPwd("123");
		di.setOmcID(302);
		di.setHostUser("chensj");
		di.setIP("127.0.0.1");
		obj.setDevInfo(di);

		ManyFilesAccessor aa = new AlcatelLucentWcdmaPerformanceAccessor(obj);
		aa.handle();
	}
}
