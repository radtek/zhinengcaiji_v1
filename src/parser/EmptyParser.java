package parser;

import java.io.FileReader;
import task.CollectObjInfo;

/**
 * 空模板解析
 * 
 * @author sunxg
 * @since 1.0
 */
public class EmptyParser extends Parser {

	// 剩余未解析的字符串
	private String m_OddString = "";

	public EmptyParser() {
	}

	public EmptyParser(CollectObjInfo TaskInfo) {
		super(TaskInfo);
	}

	@Override
	public boolean parseData() throws Exception {
		FileReader reader = null;
		try {
			log.debug("空模板-开始解析" + fileName);
			reader = new FileReader(fileName);
			char[] buff = new char[65536];

			int iLen = 0;
			while ((iLen = reader.read(buff)) > 0) {
				BuildData(buff, iLen);
			}

			// 加结束标记
			String strEnd = "\n**FILEEND**";
			BuildData(strEnd.toCharArray(), strEnd.length());
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (Exception e) {
			}
		}

		return true;
	}

	public boolean BuildData(char[] chData, int iLen) {
		m_OddString += new String(chData, 0, iLen);

		boolean bLastCharN = false;// 最后一个字符是\n
		if (m_OddString.charAt(m_OddString.length() - 1) == '\n')
			bLastCharN = true;

		// 分行
		String[] strzRowData = m_OddString.split("\n");

		// 没有数据
		if (strzRowData.length == 0)
			return true;

		// 特殊标记表示达到最后一行
		int nRowCount = strzRowData.length - 1;
		m_OddString = strzRowData[nRowCount];
		if (m_OddString.equals("**FILEEND**"))
			m_OddString = "";

		// 如果最后一个字符是\n 下次采集的时候,将是补上\n这个字符
		if (bLastCharN)
			m_OddString += "\n";

		// 最后一行不解析,与下次数据一起解析
		for (int i = 0; i < nRowCount; ++i) {
			try {
				if (strzRowData[i] == null || strzRowData[i].trim().equals(""))
					continue;
				ParseLineData(strzRowData[i] + "\n");
			} catch (Exception e) {
				log.error("BuildData", e);
			}
		}

		return true;
	}

	public void ParseLineData(String strOldRow) {
		distribute.DistributeData(strOldRow.getBytes(), 0);
	}

	public static void main(String[] args) {
		// EmptyParser parser = new EmptyParser(null);
		// parser.setFileName("d:\\mr_detail_20090922_210000.log");
		// parser.parseData();
	}
}
