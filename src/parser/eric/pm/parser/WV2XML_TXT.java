package parser.eric.pm.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import parser.Parser;
import util.Util;

/**
 * 联通二期W爱立信性能文件解析
 * 
 * @author YangJian
 * @since 1.0
 */
public class WV2XML_TXT extends Parser {

	private XmlDefaultHandler txtHandler;

	@Override
	public boolean parseData() {
		boolean ret = false;
		XmlAsTxtParser sp = null;
		try {
			sp = new XmlAsTxtParser();
			txtHandler = new XmlDefaultHandler();
			sp.parse(this.getFileName(), txtHandler);
			ret = true;
		} catch (Exception e) {
			log.error("解析文件失败,原因:", e);
		} finally {
			if (txtHandler != null)
				txtHandler.clear();
			txtHandler = null;
		}

		return ret;
	}

	// 单元测试
	public static void main(String[] args) {
		WV2XML_TXT xml = new WV2XML_TXT();
		xml.setFileName("C:\\Users\\yangjian\\Desktop\\eric\\A20100610.0830+0800-0845+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC07,MeContext=DGRNC07_statsfile.xml");
		xml.parseData();
	}

	/**
	 * 自定义XML文件SAX方式处理类
	 * 
	 * @author YangJian
	 * @since 1.0
	 */
	class XmlDefaultHandler implements XmlAsTextHandler {

		private MD currMD; // 当前处理的MD对象

		private String currMOID = null; // 最近一个moid

		private boolean isSnTag = false; // 是否是SN标签

		private boolean isMdTagPresent = false; // 是否出现md标签

		private boolean isMtTagPresent = false; // 是否出现mt标签

		private boolean isMoidTagPresent = false; // 是否出现moid标签

		private boolean isRTagPresent = false; // 是否出现r标签

		private Data mds = null; // 解析后最终的数据

		@Override
		public void content(String content) throws Exception {
			if (Util.isNull(content))
				return;

			// r 标签
			if (isRTagPresent) {
				handleR(content);
			}
			// moid 标签
			else if (isMoidTagPresent) {
				handleMOID(content);
			}
			// mt 标签
			else if (isMtTagPresent) {
				handleMT(content);
			}
			// md 标签
			else if (isMdTagPresent) {
				handleMD(content);
			}
			// sn 标签
			else if (isSnTag)
				handleSN(content);
		}

		@Override
		public void startElement(String qName) throws Exception {
			if (qName.equalsIgnoreCase("r")) {
				isRTagPresent = true;
			} else if (qName.equalsIgnoreCase("moid")) {
				isMoidTagPresent = true;
			} else if (qName.equalsIgnoreCase("mt")) {
				isMtTagPresent = true;
			} else if (qName.equalsIgnoreCase("md")) {
				isMdTagPresent = true;
			} else if (qName.equalsIgnoreCase("sn"))
				isSnTag = true;
		}

		@Override
		public void endElement(String qName) throws Exception {
			if (qName.equalsIgnoreCase("r")) {
				isRTagPresent = false;
			} else if (qName.equalsIgnoreCase("moid")) {
				isMoidTagPresent = false;
			} else if (qName.equalsIgnoreCase("mt")) {
				isMtTagPresent = false;
			} else if (qName.equalsIgnoreCase("md")) {
				isMdTagPresent = false;
				if (currMD != null)
					mds.addMD(currMD);
				currMOID = null;
				// currMD = null;
				currMD.setCurrent(false);
			} else if (qName.equalsIgnoreCase("sn"))
				isSnTag = false;
		}

		@Override
		public void startDocument() throws Exception {
			log.debug("开始读取XML文件");
			mds = new Data();
		}

		@Override
		public void endDocument() throws Exception {
			log.debug("XML文件读取结束");
			mds.print();
		}

		/** 处理SN标签中的内容 */
		private void handleSN(String snContent) {
			// log.debug(snContent);
			String[] fields = snContent.split(",");
			mds.subNetworkRoot = fields[0].substring(fields[0].indexOf("=") + 1);
			mds.subNetworkValue = fields[1].substring(fields[1].indexOf("=") + 1);
			mds.meContextValue = fields[2].substring(fields[2].indexOf("=") + 1);
			mds.rncName = mds.subNetworkValue;
		}

		/** 处理mt标签中的内容 */
		private void handleMT(String content) {
			// log.debug("mt:" + content);
			if (currMD == null || !currMD.isCurrent())
				return;
			currMD.addMT(content);
		}

		/** 处理moid标签中的内容 */
		private void handleMOID(String content) {
			// log.debug("moid:" + content);
			currMOID = content;
			if (currMD == null || !currMD.isCurrent())
				return;
			currMD.addMOID(content);
		}

		/** 处理r标签中的内容 */
		private void handleR(String content) {
			// log.debug("r:" + content);
			if (currMD == null || !currMD.isCurrent() || Util.isNull(currMOID))
				return;
			currMD.addR(content, currMOID);
		}

		/** 处理md标签中的内容 */
		private void handleMD(String content) {
			currMD = new MD();
		}

		public void clear() {
			currMD = null;
			currMOID = null;
			mds.clear();
		}
	}

	/**
	 * 对应文件中一个md节点
	 * 
	 * @author YangJian
	 * @since 1.0
	 */
	class MD {

		private List<String> mtNames = new ArrayList<String>(); // mt值集

		private String tableName; // 此MD所属表名

		/** <moid,r值数组> */
		private Map<String, ArrayList<String>> mv = new HashMap<String, ArrayList<String>>();

		private boolean currFlag = true; // 是否是当前正在处理的MD

		public String getTableName() {
			return this.tableName;
		}

		public void setTabelName(String tbName) {
			this.tableName = tbName;
		}

		public void addMT(String mtValue) {
			this.mtNames.add(mtValue);
		}

		public List<String> getMtNames() {
			return this.mtNames;
		}

		public void addMOID(String moid) {
			this.tableName = moid.substring(moid.lastIndexOf(",") + 1, moid.lastIndexOf("="));
			mv.put(moid, new ArrayList<String>());
		}

		public void addR(String rValue, String moid) {
			if (mv.containsKey(moid)) {
				mv.get(moid).add(rValue);
			} else {
				ArrayList<String> lst = new ArrayList<String>();
				lst.add(rValue);
				mv.put(moid, lst);
			}
		}

		public boolean isCurrent() {
			return this.currFlag;
		}

		public void setCurrent(boolean b) {
			this.currFlag = b;
		}

		public void clear() {
			this.mtNames.clear();
			this.tableName = null;
			this.mv.clear();
		}
	}

	/**
	 * 解析一个XML后的所有数据存放在此结构中
	 * 
	 * @author YangJian
	 * @since 1.0
	 */
	class Data {

		// 以下为公共字段
		public String rncName;

		public String subNetworkRoot;

		public String subNetworkValue;

		public String meContextValue;

		/** <表名,MD对象集合> */
		private Map<String, ArrayList<MD>> mdsMap = new HashMap<String, ArrayList<MD>>();

		public void addMD(MD md) {
			if (md == null)
				return;

			String tbName = md.getTableName();
			if (Util.isNull(tbName))
				return;

			ArrayList<MD> mdList = mdsMap.get(tbName);
			if (mdList == null)
				mdList = new ArrayList<MD>();

			mdList.add(md);
			mdsMap.put(tbName, mdList);
		}

		public int size() {
			return this.mdsMap.size();
		}

		public int getMDCount() {
			int count = 0;
			Collection<ArrayList<MD>> values = mdsMap.values();
			for (ArrayList<MD> mdList : values) {
				count = count + mdList.size();
			}
			return count;
		}

		public void print() {
			// StringBuffer sb = new StringBuffer("\n----------------------------------\n");
			// sb.append(this.subNetworkRoot).append(" ").append(this.subNetworkValue).append(" ").append(this.rncName).append("\n");
			// sb.append("md count:").append(this.getMDCount()).append(",table count:").append(this.size()).append("\n");
			// Set<Entry<String, ArrayList<MD>>> entrys = mdsMap.entrySet();
			// for (Entry<String, ArrayList<MD>> entry : entrys)
			// {
			// sb.append("表名:" + entry.getKey());
			// sb.append(" 对应的md个数:" + entry.getValue().size()).append("\n");
			// }
			// sb.append("----------------------------------");
			// log.debug(sb.toString());
		}

		public void clear() {
			Collection<ArrayList<MD>> values = mdsMap.values();
			for (ArrayList<MD> mdList : values) {
				for (MD md : mdList) {
					md.clear();
				}
				mdList.clear();
			}
			mdsMap.clear();
		}
	}
}
