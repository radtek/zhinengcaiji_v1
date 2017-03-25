package parser.hw.spas;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.IOUtils;

import util.LogMgr;
import util.Util;

/**
 * 龙计划，福建华为参数与配置，XML文件格式是一样的，这里统一解析。
 * 
 * @author ChenSijiang 2012-6-20
 */
class HW_XMLParser implements Closeable {

	private File xmlFile;

	private InputStream in;

	private XMLStreamReader reader;

	public HW_XMLParser(File xmlFile) throws Exception {
		super();
		this.xmlFile = xmlFile;
		this.initFile();
		try {
			this.in = new FileInputStream(this.xmlFile);
			initXML();
		} catch (Exception e) {
			this.close();
			throw e;
		}
	}

	protected void initFile() {
		RandomAccessFile raf = null;
		FileChannel chnanel = null;
		MappedByteBuffer memMap = null;
		try {
			raf = new RandomAccessFile(this.xmlFile, "rw");
			chnanel = raf.getChannel();
			memMap = chnanel.map(MapMode.READ_WRITE, 0, this.xmlFile.length());
			int len = memMap.limit();
			for (int i = 0; i < len; i++) {
				if (memMap.get(i) == (byte) 0x07)
					memMap.put(i, (byte) 0x20);
			}
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().warn("预处理华为XML文件出错，后续解析可能无法进行。文件：" + this.xmlFile, e);
		} finally {
			try {
				if (chnanel != null)
					chnanel.close();
			} catch (Exception e) {
			}
			try {
				if (raf != null)
					raf.close();
			} catch (Exception e) {
			}
		}
	}

	protected void initXML() throws Exception {
		XMLInputFactory fac = XMLInputFactory.newInstance();
		fac.setProperty("javax.xml.stream.supportDTD", false);
		fac.setProperty("javax.xml.stream.isValidating", false);
		reader = fac.createXMLStreamReader(in);
	}

	String tagName = null;

	String className = null;

	String fdn = null;

	Map<String, String> attrs = new HashMap<String, String>();

	int type = -1;

	public HW_XML_MO_Entry parseNextMO() throws Exception {
		while (reader.hasNext()) {
			type = reader.next();

			if (type == XMLStreamConstants.START_ELEMENT || type == XMLStreamConstants.END_ELEMENT)
				tagName = reader.getLocalName();

			if (tagName == null) {
				continue;
			}
			switch (type) {
				case XMLStreamConstants.START_ELEMENT :
					if (tagName.equals("attr")) {
						if (Util.isNotNull(className)) {
							String n = null, v = null;
							n = reader.getAttributeValue(null, "name");
							v = reader.getElementText();
							if (Util.isNotNull(n))
								attrs.put(n.toUpperCase(), v);
						}
					} else if (tagName.equals("MO") && className == null) {
						className = reader.getAttributeValue(null, "className");
						if (Util.isNotNull(className)) {
							className = className.toUpperCase();
							fdn = reader.getAttributeValue(null, "fdn");
							attrs.put("FDN", fdn);
						}
					} else if (tagName.equals("MO") && className != null) {
						HW_XML_MO_Entry entry = new HW_XML_MO_Entry(className, fdn, attrs);
						className = null;
						fdn = null;
						attrs = new HashMap<String, String>();
						className = reader.getAttributeValue(null, "className");
						if (Util.isNotNull(className)) {
							className = className.toUpperCase();
							fdn = reader.getAttributeValue(null, "fdn");
							attrs.put("FDN", fdn);
						}
						return entry;
					}
					break;
				default :
					break;
			}
		}
		return null;
	}

	@Override
	public void close() {
		try {
			if (reader != null)
				reader.close();
		} catch (Exception e) {
		}
		reader = null;
		IOUtils.closeQuietly(in);
		in = null;
		xmlFile = null;
	}

	@Override
	protected void finalize() throws Throwable {
		this.close();
		super.finalize();
	}

	public static void main(String[] args) throws Exception {
		// HW_XMLParser p = new HW_XMLParser(new File("F:\\ftp_root\\home\\uway\\FJ\\CMExport_C_FJ_XM_BSS_1_134.134.12.226_2012061905.xml"));
		// HW_XML_MO_Entry entry = null;
		// while ((entry = p.parseNextMO()) != null)
		// {
		// System.out.println(entry);
		// }
	}
}
