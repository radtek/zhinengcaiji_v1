package access.special;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import parser.hw.pm.w.xml.WV1XML;
import task.CollectObjInfo;
import task.DevInfo;
import tools.socket.SocketClientBean;
import tools.socket.SocketClientHelper;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * W网华为能性，XML方式。
 * 
 * @author ChenSijiang
 */
public class HuaweiWcdmaXMLPerformanceAccessor extends ManyFilesAccessor {

	protected WV1XML parser;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	public HuaweiWcdmaXMLPerformanceAccessor(CollectObjInfo task) {
		super(task);
		parser = new WV1XML();
		parser.setCollectObjInfo(task);
	}

	@Override
	protected synchronized boolean parse(File file) {
		if (file == null) {
			boolean b = true;
			// 告知第三方来完成入库
			String ip = SystemConfig.getInstance().getHwPmSocketIp();
			int port = SystemConfig.getInstance().getHwPmSocketPort();
			if (Util.isNotNull(ip) && port != 0) {
				SocketClientBean bean = new SocketClientBean();
				bean.setIp(ip);
				bean.setPort(port);
				SocketClientHelper.getInstance().handleMessage(bean, parser.getMessages());
			} else {
				// sqlldr入库
				b = parser.startSqlldr();
			}
			parser = new WV1XML();
			parser.setCollectObjInfo(task);
			return b;
		}

		/* 指向解压后的文件。 */
		File destFile = null;
		if (Util.isZipFile(file.getAbsolutePath())) {
			try {
				destFile = decompGZ(file);
			} catch (Exception e) {
				logger.error("解压异常 - " + file.getAbsolutePath(), e);
			}
		} else {
			destFile = file;
		}

		if (destFile == null) {
			return false;
		}

		parser.setFileName(destFile.getAbsolutePath());
		boolean b = false;
		try {
			b = parser.parseData();
		} catch (Exception e) {
			logger.error("解析时异常 - " + destFile.getAbsolutePath(), e);
			return false;
		} finally {
			if (SystemConfig.getInstance().isDeleteWhenOff())
				destFile.delete();
		}
		return b;
	}

	public static void main(String[] args) throws Exception {
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(999);
		obj.setDevPort(21);
		obj.setDevInfo(dev);
		obj.setParseTmpID(201101121);
		obj.setLastCollectTime(new Timestamp(Util.getDate1("2011-07-26 14:00:00").getTime()));
		obj.setCollectPath("/hw/A20120821.1000+0800-1100+0800_NNRNC9*.gz");
		DevInfo di = new DevInfo();
		di.setHostPwd("123");
		di.setHostUser("chensj");
		di.setIP("127.0.0.1");
		di.setOmcID(999);
		obj.setDevInfo(di);

		ManyFilesAccessor a = new HuaweiWcdmaXMLPerformanceAccessor(obj);
		a.handle();
	}

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
			logger.error("解压华为性能.gz文件异常 - " + gzFile.getAbsolutePath(), e);
			return null;
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
		gzFile.delete();
		return f;
	}
}
