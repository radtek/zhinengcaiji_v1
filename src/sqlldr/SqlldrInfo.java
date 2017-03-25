package sqlldr;

import java.io.Closeable;
import java.io.File;
import java.io.PrintWriter;

import org.apache.commons.io.IOUtils;

public class SqlldrInfo implements Closeable {

	public File txt;

	public File log;

	public File bad;

	public File ctl;

	public PrintWriter writerForTxt;

	public PrintWriter writerForCtl;

	public SqlldrInfo(File txt, File log, File bad, File ctl) {
		super();
		this.txt = txt;
		this.log = log;
		this.bad = bad;
		this.ctl = ctl;
		try {
			writerForTxt = new PrintWriter(txt);
			writerForCtl = new PrintWriter(ctl);
		} catch (Exception unused) {
		}
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(writerForCtl);
		IOUtils.closeQuietly(writerForTxt);

	}

}
