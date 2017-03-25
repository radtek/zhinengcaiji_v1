package parser.eric.pm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import parser.Parser;
import task.CollectObjInfo;
import task.DevInfo;
import util.DbPool;
import util.LogMgr;
import util.Util;
import framework.SystemConfig;

/**
 * 联通二期W爱立信性能文件解析
 * 
 * @author YangJian
 * @since 1.0
 */
public class WV2XML extends Parser {

	/**
	 * 映射表信息 <表名,<原始字段名,映射后的短名>>
	 * <p>
	 * 此类被装载时从爱立信性能映射表中加载所有的列映射信息，如果程序在解析XML文件mt的值的时候，发现mt的值在映射表原始字段列中不存在则不予以处理， 只处理存在的列
	 * <p>
	 */
	public static Map<String, HashMap<String, String>> mapInfo = new HashMap<String, HashMap<String, String>>();
	static {
		loadMapInfoFromDB();
	}

	private ConcurrentSqlldrTool sqlldrTool;

	// -------------数据入库（sqlldr方式）的日志存放目录----------------
	// 系统数据目录/
	// |
	// ldrlog/
	// |
	// eric_w_pm/
	// |
	// 月份文件夹/
	// ------------------------------------------------------------

	private XmlDefaultHandler saxHandler;

	public int omcID;

	public Timestamp dataTime;

	public long taskID;

	/** 数据入库方式的阀值，默认为5条，也就是说5条之内采取逐条插入，5条以上采取sqlldr入库 */
	public static final int DATALOAD_THRESHOLD = 5;

	public WV2XML() {
		super();
		makeLogFolder();
	}

	public WV2XML(CollectObjInfo obj) {
		super(obj);
		makeLogFolder();
	}

	@Override
	public boolean parseData() {
		omcID = this.collectObjInfo.getDevInfo().getOmcID();
		dataTime = this.collectObjInfo.getLastCollectTime();
		taskID = this.collectObjInfo.getTaskID();

		boolean ret = false;
		SAXParserFactory sf = SAXParserFactory.newInstance();
		SAXParser sp = null;
		try {
			sp = sf.newSAXParser();
			saxHandler = new XmlDefaultHandler();
			sp.getXMLReader().setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			sp.getXMLReader().setFeature("http://xml.org/sax/features/validation", false);
			// handleRElement(new File(this.getFileName()));
			File xfile = new File(this.getFileName());
			sp.parse(xfile, saxHandler);
			sqlldr();
			ret = true;
		} catch (Exception e) {
			log.error("解析文件失败,原因:", e);
		} finally {
			if (saxHandler != null)
				saxHandler.clear();
		}

		return ret;
	}

	/**
	 * Sqlldr入库数据
	 */
	private void sqlldr() {
		Data data = saxHandler.getData();
		if (data == null)
			return;

		Map<String, ArrayList<MD>> tableRecords = data.getMdsMap();
		if (tableRecords.size() == 0)
			return;
		// 遍历处理每个表
		Set<Entry<String, ArrayList<MD>>> entries = tableRecords.entrySet();
		// <文件路径,此文件中数据的条数>
//		Map<String, Integer> files = new HashMap<String, Integer>(entries.size());
		String tbName = null;
		for (Entry<String, ArrayList<MD>> entry : entries) // 开始遍历每个表
		{
			/** <字段,字段的索引> */
			Map<String, Integer> columnMap = new HashMap<String, Integer>();
			/** 表示此表中所有的记录 <moid,r值列表> records为一个表最终数据 */
			Map<String, ArrayList<String>> records = new HashMap<String, ArrayList<String>>();
			/** 大字段 <字段名,大小> 这里的大小决定在写ctl文件时char为多少的值 */
			Map<String, Integer> largeColumns = new HashMap<String, Integer>();
			tbName = entry.getKey();
			if (Util.isNull(tbName))
				continue;
			try {
				ArrayList<MD> mds = entry.getValue();
				if (mds == null || mds.size() == 0)
					continue;

				// ---------- 处理单个表中的md数据 -------------------------

				// 第一步：合并所有md中mt，使不重复,变成我们需要的表的表头（字段列表）,并且带上字段的索引以方便查找A字段是第几个字段
				for (MD md : mds) {
					List<String> mts = md.getMtNames();
					for (int i = 0; i < mts.size(); i++) {
						String mt = mts.get(i);
						if (!columnMap.containsKey(mt)) {
							int index = columnMap.size();
							columnMap.put(mt, index);
						}
					}
				}
				// 并且取出其中的一个(只取一个，因为我们假定在一个表中每个moid有相同的结构)moid进行分析，得出moid相关的字段
				for (MD md : mds) {
					Set<String> moids = md.getMv().keySet();
					for (String strMoid : moids) {
						if (Util.isNull(strMoid))
							continue;

						String[] moidFields = strMoid.split(",");
						for (String moidField : moidFields) {
							String[] strs = moidField.split("=");
							String fieldName = strs[0]; // 第1个为字段名，序号为0
							// 从映射表获得映射名
							if (mapInfo.containsKey(tbName)) {
								String sn = mapInfo.get(tbName).get(fieldName);
								fieldName = sn == null ? fieldName : sn;
							}

							// 把分析moid中得到的字段加到字段映射中
							int fieldIndex = columnMap.size();
							if (!columnMap.containsKey(fieldName))
								columnMap.put(fieldName, fieldIndex);
						}
						break;
					}
					break;
				}

				int columnCount = columnMap.size();
				if (columnCount == 0)
					continue;

				// 第二步: 形成一个行列矩阵保存此表中的数据, 方法为：遍历mv中moid和r的值,也就是取字段对应的值并填充到矩阵当中
				for (MD md : mds) {
					List<String> mts = md.getMtNames();
					// 会出现没有mt或者mt没内容的情况，则跳过
					if (mts.size() == 0)
						continue;
					Set<Entry<String, ArrayList<String>>> mvs = md.getMv().entrySet();
					// <moid,r值列表>
					for (Entry<String, ArrayList<String>> mvEntry : mvs) {
						String moid = mvEntry.getKey();
						// 分析r列表，加入到矩阵中
						ArrayList<String> rList = mvEntry.getValue();
						// 这个条件 ==0 是考虑到 有时候 r没有值的情况，即为空,必须初始化
						if (rList.size() == 0) {
							// 必须初始化
							ArrayList<String> iValues = new ArrayList<String>(columnCount);
							for (int h = 0; h < columnCount; h++)
								iValues.add(null);
							records.put(moid, iValues);
						}

						for (int i = 0; i < rList.size(); i++) {
							String mt = mts.get(i); // 获取字段名
							int columnIndex = columnMap.get(mt); // 获取字段对应的索引
							String r = rList.get(i); // 获取对应的r值
							r = (r == null ? "" : r); // 考虑r为空的情况

							// 处理大字段情况
							data.checkLargeField(r, mt, largeColumns);

							// 把r值填到对应的矩阵位置上
							if (!records.containsKey(moid)) {
								ArrayList<String> rValues = new ArrayList<String>(columnCount);
								// 必须初始化
								for (int j = 0; j < columnCount; j++)
									rValues.add(null);
								records.put(moid, rValues);
							}
							records.get(moid).set(columnIndex, r);
						}

						// 分析moid中包含的字段值，加入矩阵中
						String[] moidFields = moid.split(",");
						for (String moidField : moidFields) {
							String[] strs = moidField.split("=");
							String fieldName = strs[0]; // 字段名
							String fieldValue = strs[1]; // 第2个为字段值，序号为1

							// 从映射表获得映射名
							if (mapInfo.containsKey(tbName)) {
								String sn = mapInfo.get(tbName).get(fieldName);
								fieldName = sn == null ? fieldName : sn;
							}

							// 把分析moid中得到的字段值加到矩阵中
							int cIndex = columnMap.get(fieldName) == null ? -1 : columnMap.get(fieldName);
							if (cIndex > -1)
								records.get(moid).set(cIndex, fieldValue);
						}
					}
				}
			} catch (Exception e) {
				log.error("Task-" + this.taskID + "  表名  " + tbName + " 出现异常", e);
			}

			// 第三步: 针对形成的表数据矩阵 records，把他交由sqlldrTool进行生成数据和提交入库
			SqlldrParam param = new SqlldrParam();
			param.columnMap = columnMap;
			param.records = records;
			param.tbName = tbName;
			param.commonFields = data.commonFields;
			param.largeColumns = largeColumns;
			param.omcID = this.omcID;
			param.dataTime = this.dataTime;
			param.taskID = this.taskID;

			sqlldrTool.writeTable(param);

			// SqlldrTool sqlldr = new SqlldrTool(param);
			// SqlldrInfo info = sqlldr.execute();
			//
			// if (info != null)
			// files.put(info.txtFile, records.size());
			// // 第四步: 执行sqlldr 或者批量插入
			// if ( records.size() > DATALOAD_THRESHOLD )
			// {
			// SqlldrTool sqlldr = new SqlldrTool(param);
			// sqlldr.execute();
			// }
			// else
			// {
			// InsertTool iTool = new InsertTool(param);
			// iTool.execute();
			// }

		} // 处理所有表的循环结束

		// 生成fd文件
		// writeFd(files);

	}

	@SuppressWarnings("unused")
	private void writeFd(Map<String, Integer> files) {
		Date date = new Date();
		XMLWriter writer = null;
		String time = Util.getDateString_yyyyMMddHHmmssSSS(date);
		String fdPath = SystemConfig.getInstance().getFdPath() + File.separator + time + "_" + taskID + ".fd";
		try {
			Document doc = DocumentHelper.createDocument();
			Element root = doc.addElement("ds_desc");
			root.addElement("group").addAttribute("id", String.valueOf(collectObjInfo.getGroupId()));
			root.addElement("task").addAttribute("id", String.valueOf(taskID));
			root.addElement("stamptime").addAttribute("value", Util.getDateString(date));
			root.addElement("omc").addAttribute("id", String.valueOf(omcID));
			root.addElement("datatime").addAttribute("value", Util.getDateString(dataTime));
			Element filesE = root.addElement("files");
			Set<Entry<String, Integer>> entries = files.entrySet();
			for (Entry<String, Integer> e : entries)
				filesE.addElement("file").addAttribute("url", e.getKey()).addAttribute("count", String.valueOf(e.getValue()));
			writer = new XMLWriter(new FileOutputStream(fdPath));
			writer.write(doc);
		} catch (IOException e) {
			log.error("生成fd文件时发生错误.", e);
		} finally {
			if (writer != null)
				try {
					writer.close();
				} catch (IOException e) {
				}
		}
	}

	/**
	 * 加载所有的列映射信息
	 */
	public static synchronized void loadMapInfoFromDB() {
		// String sql =
		// "SELECT COL_NAME,SHORT_COL_NAME,TAB_NAME FROM CLT_PM_W_ERIC_MAP WHERE ISUSED=1";
		String sql = "SELECT COL_NAME,SHORT_COL_NAME,TAB_NAME FROM CLT_PM_W_ERIC_MAP ";
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = DbPool.getConn();
			ps = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String rawName = rs.getString("COL_NAME");
				String shortName = rs.getString("SHORT_COL_NAME").toUpperCase();
				String tbName = rs.getString("TAB_NAME").toUpperCase();

				if (mapInfo.containsKey(tbName)) {
					HashMap<String, String> fMap = mapInfo.get(tbName);
					// 如果表里有重复的映射定义，只会加载第一个
					if (!fMap.containsKey(rawName)) {
						fMap.put(rawName, shortName);
					}
				} else {
					HashMap<String, String> fMap = new HashMap<String, String>();
					fMap.put(rawName, shortName);
					mapInfo.put(tbName, fMap);
				}
			}
			rs.close();
		} catch (Exception e) {
			LogMgr.getInstance().getSystemLogger().error("加载映射表信息时异常:" + sql, e);
		} finally {
			if (con != null) {
				try {
					if (ps != null) {
						ps.close();
					}
					con.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	/**
	 * 创建日志文件夹
	 */
	private static void makeLogFolder() {
		String dataRootPath = SystemConfig.getInstance().getCurrentPath() + File.separator;
		String ldrLogPath = dataRootPath + "ldrlog" + File.separator;
		File ldrlogFile = new File(ldrLogPath);
		if (!ldrlogFile.exists() || !ldrlogFile.isDirectory())
			ldrlogFile.mkdir();

		String ericpmPath = ldrLogPath + "eric_w_pm" + File.separator;
		File ericpmFile = new File(ericpmPath);
		if (!ericpmFile.exists() || !ericpmFile.isDirectory())
			ericpmFile.mkdir();

		Calendar calendar = Calendar.getInstance();
		int month = calendar.get(Calendar.MONTH) + 1; // 当前月份
		int year = calendar.get(Calendar.YEAR);
		String timePath = ericpmPath + year + "-" + month + File.separator;
		File timeFile = new File(timePath);
		if (!timeFile.exists() || !timeFile.isDirectory())
			timeFile.mkdir();
	}

	/**
	 * 自定义XML文件SAX方式处理类
	 * 
	 * @author YangJian
	 * @since 1.0
	 */
	class XmlDefaultHandler extends DefaultHandler {

		private MD currMD; // 当前处理的MD对象

		private String currMOID = null; // 最近一个moid

		private boolean isSnTag = false; // 是否是SN标签

		private boolean isMdTagPresent = false; // 是否出现md标签

		private boolean isMtTagPresent = false; // 是否出现mt标签

		private boolean isMoidTagPresent = false; // 是否出现moid标签

		private boolean isRTagPresent = false; // 是否出现r标签

		/** 判断正在处理的md也就是表是否是我们需要的表 */
		public boolean isNeedHandleTable = true;

		private boolean isSF = false;

		private int currR_ID = -1; // 当前md节点处理中 mv 标签处理中的 r标签是第几个

		/**
		 * 在一个md节点中需要被跳过的mt值编号（不在映射表里面的mt值都将被定义为被跳过,mt值编号指的是在一个md中mt出现的是第几个）列表, md标签结束时必须clear此list
		 */
		private List<Integer> skipMT_IDs = new ArrayList<Integer>();

		private boolean mtSkipFinished = false; // mt是否已经移除完成,事实上当第一个mv完成后也就是一个moid节点开始的时候，mt列表已经被移除完了，剩下的mv操作不需要移除了。

		private Data mds = null; // 解析后最终的数据

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (!isNeedHandleTable)
				return;

			String content = new String(ch, start, length);
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
			else if (isMdTagPresent && !isSF) {
				handleMD(content);
			}
			// sn 标签
			else if (isSnTag)
				handleSN(content);
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attrs) throws SAXException {
			// 如果发现不是我们之前定义好的表（md），那么就不用处理
			if (!isNeedHandleTable)
				return;

			if (qName.equalsIgnoreCase("r")) {
				isRTagPresent = true;
				currR_ID++;
			} else if (qName.equalsIgnoreCase("moid")) {
				isMoidTagPresent = true;
				// 当出现moid的时候r编号需要被复位
				currR_ID = -1;
			} else if (qName.equalsIgnoreCase("mt")) {
				isMtTagPresent = true;
			} else if (qName.equalsIgnoreCase("md")) {
				isMdTagPresent = true;
				isNeedHandleTable = true;
				mtSkipFinished = false;
				// 下面信息在出现md标签的时候必须复位
				currR_ID = -1;
				skipMT_IDs.clear();
			} else if (qName.equalsIgnoreCase("sn"))
				isSnTag = true;
			else if (qName.equalsIgnoreCase("sf"))
				isSF = true;
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (!isNeedHandleTable && !qName.equalsIgnoreCase("md"))
				return;

			if (qName.equalsIgnoreCase("r")) {
				isRTagPresent = false;
			} else if (qName.equalsIgnoreCase("moid")) {
				isMoidTagPresent = false;
				handleMOID_EndTag();
			} else if (qName.equalsIgnoreCase("mt")) {
				isMtTagPresent = false;
			} else if (qName.equalsIgnoreCase("md")) {
				isMdTagPresent = false;

				currR_ID = -1;
				skipMT_IDs.clear();

				if (currMD != null && isNeedHandleTable)
					mds.addMD(currMD);
				currMOID = null;
				// currMD = null;
				currMD.setCurrent(false);

				isNeedHandleTable = true;
			} else if (qName.equalsIgnoreCase("sn"))
				isSnTag = false;
			else if (qName.equalsIgnoreCase("sf"))
				isSF = false;
		}

		@Override
		public void startDocument() throws SAXException {
			// log.debug("开始读取XML文件 - " + WV2XML.this.getFileName());
			mds = new Data();
		}

		@Override
		public void endDocument() throws SAXException {
			// log.debug("XML文件读取结束 - " + WV2XML.this.getFileName());
			mds.print();
		}

		/** 处理SN标签中的内容 */
		private void handleSN(String snContent) {
			// log.debug(snContent);
			String[] fields = snContent.split(",");
			String subnetwork = fields[1].substring(fields[1].indexOf("=") + 1);

			mds.commonFields.add(new Field("SUBNETWORKROOT", fields[0].substring(fields[0].indexOf("=") + 1)));
			mds.commonFields.add(new Field("SUBNETWORK", subnetwork));
			mds.commonFields.add(new Field("MECONTEXT", fields[2].substring(fields[2].indexOf("=") + 1)));
			mds.commonFields.add(new Field("RNC_NAME", subnetwork));

			try {
				mds.isNodeB = !subnetwork.trim().equalsIgnoreCase(fields[2].substring(fields[2].indexOf("=") + 1).trim());
			} catch (Exception e) {
			}
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

		/** 处理moid关闭标签 */
		private void handleMOID_EndTag() {

			if (mtSkipFinished)
				return;

			// 主要处理 mt的各个值是否在映射表中，不在的话则找出其编号是多少,另外就是进行字段映射替换
			// 为什么在这里处理： 因为只有moid结束标签出现时才知道表名，也就是此时才能知道该表的字段是什么
			if (Util.isNull(currMD.tableName))
				return;

			List<String> mts = currMD.getMtNames();
			if (mts.size() <= 0)
				return;

			int size = mts.size();
			for (int i = size - 1; i >= 0; i--) {
				String mtValue = mts.get(i);
				if (Util.isNull(mtValue)) {
					mts.remove(i);
					skipMT_IDs.add(i);
					continue;
				}

				// 判断mt是否在映射表中
				if (mapInfo.containsKey(currMD.tableName)) {
					isNeedHandleTable = true;
					if (!mapInfo.get(currMD.tableName).containsKey(mtValue)) {
						mts.remove(i);
						skipMT_IDs.add(i);
					} else {
						// 替换为短名
						mts.set(i, mapInfo.get(currMD.tableName).get(mtValue));
					}
				} else {
					isNeedHandleTable = false;
				}
			}

			mtSkipFinished = true; // 表示mt列表的移除以及映射工作已经完成，下面的同一个md中的mv处理不需要在进行此操作了.
		}

		/** 处理r标签中的内容 */
		private void handleR(String content) {
			// log.debug("r:" + content);
			if (currMD == null || !currMD.isCurrent() || Util.isNull(currMOID))
				return;

			// 看此r是否需要跳过,包含则需要跳过
			if (skipMT_IDs.contains(currR_ID))
				return;

			currMD.addR(content, currMOID);
		}

		/** 处理md标签中的内容 */
		private void handleMD(String content) {
			currMD = new MD();
		}

		public Data getData() {
			return this.mds;
		}

		public void clear() {
			currMD = null;
			currMOID = null;
			if (mds != null)
				mds.clear();
		}
	}

	public void setSqlldrTool(ConcurrentSqlldrTool sqlldrTool) {
		this.sqlldrTool = sqlldrTool;
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

		private static final String TABLE_PREFIX = "CLT_PM_W_ERIC_"; // 表名前缀

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

		public Map<String, ArrayList<String>> getMv() {
			return mv;
		}

		public void addMOID(String moid) {
			String tn = TABLE_PREFIX + moid.substring(moid.lastIndexOf(",") + 1, moid.lastIndexOf("="));
			if (tn.length() > 30)
				tn = tn.substring(0, 30);
			this.tableName = tn.toUpperCase();
			if (this.tableName.equalsIgnoreCase("clt_pm_w_eric_vcltp") && saxHandler.getData().isNodeB)
				this.tableName = "CLT_PM_W_ERIC_NODEBVCLTP";
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
			// String tr = rValue;
			// if ( tr != null && tr.equals("!!!null!!!") )
			// {
			// tr = "";
			// }
			// if ( mv.containsKey(moid) )
			// {
			// mv.get(moid).add(tr);
			// }
			// else
			// {
			// ArrayList<String> lst = new ArrayList<String>();
			// lst.add(tr);
			// mv.put(moid, lst);
			// }
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

		// 为公共字段
		public List<Field> commonFields;

		/** <表名,MD对象集合> */
		private Map<String, ArrayList<MD>> mdsMap = new HashMap<String, ArrayList<MD>>();

		/*--------------
		 * 大字段的定义为长度大于255个字符，因为sqlldr方式入库的时候写ctl文件的时候如果是varchar2类型的，那么长度大于255的时候必须指定长度，否则会报错.
		 * 我们在解析r标签的时候判断长度是否大于255，如果大于则加到此map中。
		 */

		public boolean isNodeB = false;

		public Data() {
			commonFields = new ArrayList<Field>();
		}

		public Map<String, ArrayList<MD>> getMdsMap() {
			return mdsMap;
		}

		/**
		 * 检查大字段类型
		 * 
		 * @param rValue
		 *            r值
		 * @param mtName
		 *            短名
		 */
		public void checkLargeField(String rValue, String mtName, Map<String, Integer> largeColumns) {
			if (rValue.length() < 255)
				return;

			if (!largeColumns.containsKey(mtName)) {
				largeColumns.put(mtName, rValue.length() + 10);
			}
		}

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
			// StringBuffer sb = new StringBuffer(
			// "\n----------------------------------\n");
			// for (Field field : commonFields) {
			// sb.append(field.value).append(" ");
			// }
			// sb.append("\n");
			// sb.append("md count:").append(this.getMDCount()).append(
			// ",table count:").append(this.size()).append("\n");
			// Set<Entry<String, ArrayList<MD>>> entrys = mdsMap.entrySet();
			// for (Entry<String, ArrayList<MD>> entry : entrys) {
			// sb.append("表名:" + entry.getKey());
			// sb.append(" 对应的md个数:" + entry.getValue().size()).append("\n");
			// }
			// sb.append("----------------------------------");
			// log.debug(taskID + ": " + sb.toString());
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

	static void handleRElement(File f) {
		if (f == null || !f.exists() || !f.isFile())
			return;
		File tmp = new File(f.getParent(), f.getName() + ".tmp");
		InputStream in = null;
		OutputStream out = null;
		BufferedReader br = null;
		Reader r = null;
		PrintWriter pw = null;
		try {
			in = new FileInputStream(f);
			r = new InputStreamReader(in);
			br = new BufferedReader(r);
			pw = new PrintWriter(tmp);
			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.contains("<r></r>"))
					line = line.replace("<r></r>", "<r>!!!null!!!</r>");
				pw.println(line);
				pw.flush();
			}

		} catch (Exception e) {

		} finally {
			IOUtils.closeQuietly(pw);
			IOUtils.closeQuietly(out);
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(r);
			IOUtils.closeQuietly(in);
		}
		f.delete();
		tmp.renameTo(f);
	}

	// 单元测试
	public static void main(String[] args) {
		File f = new File(
				"F:\\ftp_root\\pk2\\uway\\eric\\pm\\A20110709.2000+0800-2015+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=ECR16,MeContext=WBJ00656_statsfile.xml");
		handleRElement(f);

		long curr = System.currentTimeMillis();
		CollectObjInfo obj = new CollectObjInfo(755123);
		DevInfo dev = new DevInfo();
		dev.setOmcID(111);
		obj.setDevInfo(dev);
		obj.setLastCollectTime(new Timestamp(0));

		WV2XML xml = new WV2XML();
		xml.collectObjInfo = obj;
		// xml
		// .setFileName("D:\\eri_pm_13\\A20110113.1730+0800-1745+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=DGRNC01,MeContext=DGRNC01_statsfile.xml");
		xml.setFileName("C:\\Users\\ChenSijiang\\Desktop\\wfrnc01\\A20110214.1000+0800-1015+0800_SubNetwork=ONRM_ROOT_MO,SubNetwork=WFRNC01,MeContext=WFRNC01_statsfile.xml");
		xml.parseData();
		System.out.println((System.currentTimeMillis() - curr) / 1000.00);
	}
}
