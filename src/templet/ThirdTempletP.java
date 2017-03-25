package templet;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import framework.SystemConfig;

/**
 * ThirdTempletP
 * 
 * @author IGP TDT
 * @since 1.0
 */
public class ThirdTempletP extends AbstractTempletBase {

	public int nAppType = 1; // APPTYPE, 应用类型， 1-MR定位处理

	public int nLocateJava = 1; // LOCATEJAVA,定位程序, 0-C++库,1-Java

	public int nLocate = 1; // LOCATE,是否定位,中创硬采时，内容中有定位，则不用定位了

	public int nMRSource = 0; // MRSOURCE,MR数据源的类型，0-中创硬采，1-中兴硬采,

	// 2-中兴硬采IMSI、TMSI、session版, 3-摩托MR采集
	public int ncontextappendtype = 0; // 内容追究方式, 0-新建文件填充内容. 1-将内容从文件的最后一行开始追加

	public int nfilenamesplittype = 0; // 定位文件拆分方式 0(不对内容进行拆分)

	// 1(对定位后文件的MR内容进行按1小时拆分

	public void parseTemp(String TempContent) throws Exception {
		if (TempContent == null || TempContent.trim().equals(""))
			return;

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		builder = factory.newDocumentBuilder();
		Document doc = null;

		// 模板文件路径
		String TempletFilePath = SystemConfig.getInstance().getTempletPath();
		TempletFilePath = TempletFilePath + File.separatorChar + TempContent;
		File file = new File(TempletFilePath);
		doc = builder.parse(file);

		NodeList pn = doc.getElementsByTagName("PUBLIC");
		if (pn.getLength() >= 1) {
			try {
				nAppType = Integer.parseInt(doc.getElementsByTagName("APPTYPE").item(0).getFirstChild().getNodeValue());

				nLocateJava = Integer.parseInt(doc.getElementsByTagName("LOCATEJAVA").item(0).getFirstChild().getNodeValue());
				nLocate = Integer.parseInt(doc.getElementsByTagName("LOCATE").item(0).getFirstChild().getNodeValue());
				nMRSource = Integer.parseInt(doc.getElementsByTagName("MRSOURCE").item(0).getFirstChild().getNodeValue());
				ncontextappendtype = Integer.parseInt(doc.getElementsByTagName("CONTEXTAPPENDTYPE").item(0).getFirstChild().getNodeValue());
			} catch (Exception e) {

			}
			try {
				nfilenamesplittype = Integer.parseInt(doc.getElementsByTagName("FILENAMESPLITTYPE").item(0).getFirstChild().getNodeValue());
			} catch (Exception e) {

			}
		}

	}

	public int getNcontextappendtype() {
		return ncontextappendtype;
	}

	public void setNcontextappendtype(int ncontextappendtype) {
		this.ncontextappendtype = ncontextappendtype;
	}

	public int getNfilenamesplittype() {
		return nfilenamesplittype;
	}

	public void setNfilenamesplittype(int nfilenamesplittype) {
		this.nfilenamesplittype = nfilenamesplittype;
	}

}
