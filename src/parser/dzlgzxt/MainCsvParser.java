package parser.dzlgzxt;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import util.LogMgr;
import util.Util;
import util.opencsv.CSVReader;

/**
 * 解析主CSV文件。
 * 
 * @author ChenSijiang 2012-5-25
 */
class MainCsvParser implements Iterator<SoftMgrFormContent>, Closeable {

	private static final Logger log = LogMgr.getInstance().getSystemLogger();

	/* 文件第一行，即列头部分，单个列的索引位置到列名的映射。 */
	private Map<Integer, String> colIndexMap;

	private CSVReader csvReader;

	private FileInputStream in;

	private Reader r;

	private String[] buffLine;

	MainCsvParser(String file) throws Exception {
		FileInputStream in = null;
		Reader r = null;
		in = new FileInputStream(file);
		r = new InputStreamReader(in);
		csvReader = new CSVReader(r);
		String[] lineCollection = null;
		if ((lineCollection = csvReader.readNext()) != null)
			colIndexMap = parseHeader(lineCollection);
		else
			throw new Exception("文件无列头（空文件）：" + file);
	}

	@Override
	public boolean hasNext() {
		if (buffLine != null)
			return true;
		try {
			buffLine = csvReader.readNext();
		} catch (Exception e) {
			log.error("读取CSV时异常", e);
		}

		return (buffLine != null);
	}

	@Override
	public SoftMgrFormContent next() {
		if (!hasNext())
			return null;
		try {
			SoftMgrFormContent form = new SoftMgrFormContent();
			for (int i = 0; i < buffLine.length; i++) {
				String colName = colIndexMap.get(i);
				String val = buffLine[i];
				if (colName == null)
					continue;
				if (colName.equals("工单编号"))
					form.formId = val;
				else if (colName.equals("派发时间"))
					form.sendDate = strToTimestamp(val);
				else if (colName.equals("升级开始时间"))
					form.updateStartTime = strToTimestamp(val);
				else if (colName.equals("升级结束时间"))
					form.updateEndTime = strToTimestamp(val);
				else if (colName.equals("网络类型"))
					form.netType = val;
				else if (colName.equals("升级的对象节点"))
					form.updateElement = val;
				else if (colName.equals("设备类型"))
					form.deviceType = val;
				else if (colName.equals("厂商名称"))
					form.deviceVendor = val;
				else if (colName.equals("软/硬件申请前版本"))
					form.preVersion = val;
				else if (colName.equals("新软件补丁号"))
					form.patchVersion = val;
				else if (colName.equals("升级后版本"))
					form.afterVersion = val;
			}
			return form;
		} catch (Exception e) {
			log.error("CSV转为工单内容时异常", e);
			return null;
		} finally {
			buffLine = null;
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		try {
			if (csvReader != null)
				csvReader.close();
			csvReader = null;
		} catch (Exception e) {
		}
		IOUtils.closeQuietly(r);
		IOUtils.closeQuietly(in);
		buffLine = null;
	}

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	/* 解析列头，返回索引到列名的映射。 */
	static Map<Integer, String> parseHeader(String[] headLine) throws Exception {
		Map<Integer, String> h = new HashMap<Integer, String>();
		for (int i = 0; i < headLine.length; i++)
			h.put(i, headLine[i]);
		return h;
	}

	static Timestamp strToTimestamp(String val) {
		try {
			return new Timestamp(Util.getDate1(val).getTime());
		} catch (Exception e) {
			return null;
		}
	}
}
