package parser.lucent.cm;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;

class ALParamSqlldrInfo implements Closeable, Flushable {

	File txt;

	File ctl;

	File bad;

	File log;

	PrintWriter txtWriter;

	PrintWriter ctlWriter;

	Map<String, Integer> colIndex;

	private Map<Integer, String> indexCol;

	public ALParamSqlldrInfo(File txt, File ctl, File bad, File log) {
		super();
		this.txt = txt;
		this.ctl = ctl;
		this.bad = bad;
		this.log = log;
		try {
			this.txtWriter = new PrintWriter(txt);
			this.ctlWriter = new PrintWriter(ctl);
		} catch (Exception e) {
		}

		colIndex = new HashMap<String, Integer>();
	}

	public Map<Integer, String> indexCol() {
		if (indexCol != null)
			return indexCol;
		indexCol = new HashMap<Integer, String>();
		Iterator<Entry<String, Integer>> it = colIndex.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Integer> next = it.next();
			indexCol.put(next.getValue(), next.getKey());
		}
		return indexCol;
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(txtWriter);
		IOUtils.closeQuietly(ctlWriter);

	}

	@Override
	public void flush() {
		if (txtWriter != null)
			txtWriter.flush();
		if (ctlWriter != null)
			ctlWriter.flush();
	}

	public void delete(boolean delTxt, boolean delCtl, boolean delLog, boolean delBad) {
		if (delTxt)
			txt.delete();
		if (delCtl)
			ctl.delete();
		if (delLog)
			log.delete();
		if (delBad)
			bad.delete();
	}
}
