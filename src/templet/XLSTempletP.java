package templet;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import framework.SystemConfig;

/**
 * XLSTempletP
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class XLSTempletP extends AbstractTempletBase {

	public class SheetInfo {

		public String m_strSheetName;

		public boolean m_bHasTitle;
		// public int m_iColumnNum;
	}

	// Sheet信息
	public Map<Integer, SheetInfo> m_mapSheet = new HashMap<Integer, SheetInfo>();

	public void parseTemp(String TempContent) throws Exception {
		if (TempContent == null || TempContent.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = null;

		// 模板文件路径
		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + TempContent;
		File file = new File(TempletFilePath);
		doc = builder.parse(file);

		NodeList nl = doc.getElementsByTagName("SHEET");
		for (int i = 0; i < nl.getLength(); ++i) {
			SheetInfo shtInfo = new SheetInfo();
			int idx = Integer.parseInt(doc.getElementsByTagName("INDEX").item(i).getFirstChild().getNodeValue());

			shtInfo.m_strSheetName = doc.getElementsByTagName("SHEETNAME").item(i).getFirstChild().getNodeValue();
			if (doc.getElementsByTagName("HASTITLE").item(i).getFirstChild().getNodeValue().equals("0"))
				shtInfo.m_bHasTitle = false;
			else
				shtInfo.m_bHasTitle = true;

			this.m_mapSheet.put(Integer.valueOf(idx), shtInfo);
		}
	}
}
