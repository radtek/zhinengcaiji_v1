package sqlldr;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import org.apache.commons.io.IOUtils;

/**
 * 针对路测数据做的一些文件信息管理
 * @author lijiayu
 * @ 2013年9月27日
 */
public class SqlldrInfoForHwDt implements Closeable {

	File txt;

	File log;

	File bad;

	File ctl;

	//因为路测数据需要对文件重新做一次追加写入，所以这里用RandomAccessFile，并且需要提供给外部用
	RandomAccessFile rafForTxt;
	
	public RandomAccessFile getRafForTxt() {
		return rafForTxt;
	}

	
	public void setRafForTxt(RandomAccessFile rafForTxt) {
		this.rafForTxt = rafForTxt;
	}

	PrintWriter writerForCtl;	

	public SqlldrInfoForHwDt(File txt, File log, File bad, File ctl) {
		super();
		this.txt = txt;
		this.log = log;
		this.bad = bad;
		this.ctl = ctl;
		try {
			rafForTxt = new RandomAccessFile(txt, "rw");
			writerForCtl = new PrintWriter(ctl);
		} catch (Exception unused) {
		}
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(writerForCtl);
	}

}
