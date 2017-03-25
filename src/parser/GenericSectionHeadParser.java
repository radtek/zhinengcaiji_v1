package parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import parser.dt.DtLibInvoker;
import parser.dt.Region;
import parser.dt.RegionCache;
import task.CollectObjInfo;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import templet.GenericSectionHeadP.DS;
import templet.GenericSectionHeadP.Field;
import templet.GenericSectionHeadP.Fields;
import templet.GenericSectionHeadP.Templet;
import templet.TempletBase;
import util.CSVLineParser;
import util.LogMgr;
import util.Util;
import distributor.GenericSectionHeadDistributor;
import exception.ParseException;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 带表头的按段解析
 * <p>
 * 适应于一个Ascii文件中多张表并且每张表都带表头的情况,能自动适应表头字段顺序的变化(包括字段新增、删除、顺便变化等) <br>
 * 典型应用:bell性能解析
 * <p>
 * 1.配置关注字段后数据源中字段的顺序可以进行调换而不影响数据采集;<br>
 * 2.数据区域段之间的顺序调换也不影响正常的采集;<br>
 * 3.模板中定义的ds处理完后程序即可退出,不再读取剩余的数据;
 * </p>
 * </p>
 * 
 * @author YangJian,Litp
 * @since 3.1
 * @see GenericSectionHeadP
 * @see GenericSectionHeadD
 * @see GenericSectionHeadDistributor
 */
public class GenericSectionHeadParser extends Parser {

	private StringBuilder strBuffer = new StringBuilder();

	private boolean isPublicStart = false; // 公共数据区域开始标记是否已经找到

	private boolean isPublicEnd = false; // 公共数据区域结束标记是否已经找到

	private boolean isPublicFinished = false; // 公共数据区域是否已经处理完毕

	private boolean isDataAreaStartFound = false; // 普通数据区域开始标记是否找到

	private boolean isDataAreaEndFound = false; // 普通数据区域结束标记是否找到

	private int currDsID = -1; // 当前处理的DS所属的编号

	private boolean lastCommit = false; // 是否需要最后提交，针对没有配置结束标记情况（意思为读取剩下所有的文件内容）

	private GenericSectionHeadP.Templet lastATempletP;

	boolean isDT = false;// 是否actix路测

	private DtLibInvoker invoker = null;

	public static String splitSign = "`~"; // 分隔符

	private String dllName = "DtLibInvokerProxy_x86";

	private String path = "." + File.separator + "GMS_info_bj0816.csv";// "D:\\聊天软件\\qq\\Users\\549070861\\FileRecv\\GMS_info_bj0816.csv";

	private long mapData = 0;

	/** 已经被处理过的数据区域DS列表,到时候再读取文件的时候可以进行比较，如果要读取的DS（模板中配置的）都已经处理完，则提前结束 */
	private List<Integer> dsHandled = new ArrayList<Integer>();

	public GenericSectionHeadParser() {
		super();
	}

	public GenericSectionHeadParser(CollectObjInfo obj) {
		this.collectObjInfo = obj;
		this.distribute = new GenericSectionHeadDistributor(obj);
	}

	/**
	 * 判断两个字符串是否是逻辑意义上的相等。一个字符串是有通配符的，另一个是没有通配符的。 例如，"*_Adjacent_cell_handover_01Jul2010_*.csv"
	 * 可以匹配到"4_Adjacent_cell_handover_01Jul2010_0433.csv"，于是它们相等。 如果没有通配符，就按String.equals()方法来判断。
	 * 
	 * @param shortFileName
	 *            实际的文件名
	 * @param fileName
	 *            解析模板中，<FILENAME>中配的文件名
	 * @return 是否相等
	 */
	private boolean logicEquals(final String shortFileName, final String fileName) {
		// 不包含通配符的情况下，当作普通的String.equals()方法处理
		if (!fileName.contains("*") && !fileName.contains("?")) {
			return shortFileName.equals(fileName);
		}

		String s1 = shortFileName.replaceAll("\\.", ""); // 把.号去掉，因为它在正则表达式中有意义。
		String s2 = fileName.replaceAll("\\.", ""); // 把.号去掉，因为它在正则表达式中有意义。
		s1 = s1.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\*", ".*"); // *换成.*，表示多匹配多个字符
		s2 = s2.replaceAll("\\?", "."); // ?换成.，表示匹配单个字符

		return Pattern.matches(s2, s1); // 通过正则表达式方式判断
	}

	/**
	 * 找出解析此文件需要依赖的Templet对象
	 * 
	 * @return
	 */
	private List<GenericSectionHeadP.Templet> getTempletByFileName(GenericSectionHeadP templetP, String localFilename) {
		List<GenericSectionHeadP.Templet> aTempletPs = new ArrayList<GenericSectionHeadP.Templet>();
		// 取不包含路径的文件名
		Map<String, Templet> templets = templetP.getTemplets();
		Set<String> fileNames = templets.keySet();
		for (String fName : fileNames) {
			if (fName != null && fName.contains(">")) {
				if (FilenameUtils.getName(localFilename).equalsIgnoreCase(fName.subSequence(fName.indexOf(">") + 1, fName.length()).toString())) {
					aTempletPs.add(templets.get(fName));
				}
			} else if (this.getDsConfigName().equalsIgnoreCase(fName.trim())
					|| logicEquals(this.getDsConfigName(), ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()))
					|| FilenameUtils.wildcardMatch(FilenameUtils.getName(localFilename),
							ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()))
					|| ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()).endsWith(FilenameUtils.getName(localFilename))) {
				aTempletPs.add(templets.get(fName));
			} else if (fName.trim().indexOf("*") > -1) {
				if (FilenameUtils.wildcardMatch(FilenameUtils.getName(localFilename),
						ConstDef.ParseFilePath(FilenameUtils.getName(fName.trim()), collectObjInfo.getLastCollectTime()))) {
					// if (wildCardMatch(ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()), localFilename, "*")) {
					aTempletPs.add(templets.get(fName));
				}
			}
		}
		return aTempletPs;
	}

	/**
	 * 通配符匹配
	 * 
	 * @param src
	 *            带通配符的字符串
	 * @param dest
	 *            不带通配符的字符串
	 * @param wildCard
	 *            通配符
	 * @return
	 */
	public boolean wildCardMatch(String src, String dest, String wildCard) {
		String[] fieldName = Util.split(src, wildCard);
		int start = -1;
		boolean flag = true;
		for (int n = 0; n < fieldName.length; n++) {
			if ("".equals(fieldName[n]))
				continue;
			int index = dest.indexOf(fieldName[n]);
			if (index > start) {
				start = index;
				continue;
			} else {
				flag = false;
				break;
			}
		}
		return flag;
	}

	@Override
	public boolean parseData() {
		if (Util.isNull(this.fileName))
			return false;
		collectObjInfo.spasOmcId = collectObjInfo.getDevInfo().getOmcID() + "";
		if (SystemConfig.getInstance().isSPAS()) {
			log.debug("isSPAS=true");
			String nfileName = FilenameUtils.normalize(this.fileName);
			String zip = collectObjInfo.filenameMap.get(nfileName);
			if (zip == null || !zip.contains("_PARA_")) {
				log.warn(collectObjInfo.getTaskID() + " 文件" + fileName + "，未找到对应的原始压缩包名。list=" + collectObjInfo.filenameMap);
			} else {
				zip = FilenameUtils.getBaseName(zip);
				String[] sp = zip.split("_");
				collectObjInfo.spasOmcId = sp[5] + sp[6];
			}
		}
		long taskID = this.collectObjInfo.getTaskID();

		TempletBase tBaseP = this.collectObjInfo.getParseTemplet();
		if (!(tBaseP instanceof GenericSectionHeadP))
			return false;
		GenericSectionHeadP templetP = (GenericSectionHeadP) tBaseP;
		// 找出解析此文件需要依赖的Templet对象
		List<GenericSectionHeadP.Templet> aTempletPs = getTempletByFileName(templetP, this.fileName);
		if (aTempletPs.size() == 0) {
			log.error(taskID + ": 解析失败,原因：找不到对应的解析模板." + this.fileName);
			return false;
		}

		boolean flag = true;
		for (GenericSectionHeadP.Templet aTempletP : aTempletPs) {
			global = true;
			TempletBase tBaseD = this.collectObjInfo.getDistributeTemplet();
			if (!(tBaseD instanceof GenericSectionHeadD)) {
				flag = false;
				continue;
			}

			GenericSectionHeadD templetD = (GenericSectionHeadD) tBaseD;

			int pTid = aTempletP.getId(); // 解析模板中templet中的id
			// 寻找解析模板id是否在分发模板中对应存在，不存在则不予以解析
			GenericSectionHeadD.Templet aTempletD = templetD.getTemplet(pTid);
			if (aTempletD == null) {
				log.error(taskID + ": 解析失败:原因:找不到对应的分发模板. 解析模板编号=" + pTid);
				flag = false;
				continue;
			}

			// 调用动态库
			String type = aTempletP.getType();
			if (type != null && type.equals("DT")) {
				isDT = true;
				if (Util.is32Digit()) {
					dllName = "DtLibInvokerProxy_x86";
				} else {
					dllName = "DtLibInvokerProxy_x64";
				}
				invoker = new DtLibInvoker(dllName);

				File mapFile = new File(path);
				if (!mapFile.exists()) {
					log.debug("文件：" + path + "不存在，请检查，采集将停止");
					isDT = false;
					return false;
				}

				mapData = invoker.LoadMaper(path, 2);

				if (mapData < 0) {
					log.debug("mapData = " + mapData + "，加载地图信息配置有误，请检查，采集将停止");
					closeInvoke();
					return false;
				}
			}

			// 如果文件为空或者行数<=1，也需要记录采集日志表
			InputStream inn = null;
			try {
				inn = new FileInputStream(this.fileName);
				List<?> lis = IOUtils.readLines(inn);
				if (lis == null || lis.size() <= 1)
					LogMgr.getInstance()
							.getDBLogger()
							.log(collectObjInfo.getDevInfo().getOmcID(), aTempletD.getTables().get(0).getName(), collectObjInfo.getLastCollectTime(),
									0, collectObjInfo.getTaskID());
				else {
					int cc = 0;
					for (Object o : lis) {
						if (o != null) {
							String ss = o.toString();
							if (Util.isNotNull(ss))
								cc++;
						}
					}
					if (cc <= 1)
						LogMgr.getInstance()
								.getDBLogger()
								.log(collectObjInfo.getDevInfo().getOmcID(), aTempletD.getTables().get(0).getName(),
										collectObjInfo.getLastCollectTime(), 0, collectObjInfo.getTaskID());
				}
			} catch (Exception e) {
				log.error(taskID + "：检索文件行数出错");
			} finally {
				IOUtils.closeQuietly(inn);
			}

			strBuffer.delete(0, strBuffer.length());
			BufferedReader br = null;
			String strLine = null;
			try {
				br = null;
				if (aTempletP.getEncoding() != null)
					br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName), aTempletP.getEncoding()));
				else
					br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName)));

				ClassEntry entry = new ClassEntry();
				/* templet节点的skipLine属性，控制跳过文件多少行。 */
				if (aTempletP.getSkipLine() > 0) {
					for (int i = 0; i < aTempletP.getSkipLine(); i++) {
						String line = br.readLine();

						if (util.Util.isNull(line))
							continue;
						line = line.trim();
						entry.headList.add(line);
					}
				}
				

				// 获取public节点，如果没有，则认为文件中不存在公共头部门或者不需要解析
				GenericSectionHeadP.Public publicEle = aTempletP.getPublicElement();

				char[] buf = new char[1024];
				int readCount = -1;
				while ((readCount = br.read(buf)) != -1) {
					// 如果模板中定义的数据区域DS个数和我们已经处理的ds个数相同则提前结束读取文件,没必要继续读取下去
					if (aTempletP.getDsMap().size() == dsHandled.size())
						break;

					strBuffer.append(buf, 0, readCount);
					// -----处理公共数据区域-----
					// 这里考虑到公共数据区域可能一次性没读取完的情况，所以采取循环读取直到找到公共区域结束标记
					while (!isPublicFinished) {
						handlePublicArea(publicEle);
						if (!isPublicEnd && !isPublicFinished)
							break;
					}
					if (!isPublicEnd)
						continue;

					// -----处理普通数据区域-----
					// 处理缓存中所有的行 --等于null时表明本次从文件中读取的数据块已经处理完或者不足一行数据需要再读
					while ((strLine = handleLine(aTempletP, entry)) != null) {
						if (strLine.trim().length() == 0)
							continue;
					}
				}
				if (lastCommit) {
					handleALineData(lastATempletP, strBuffer.toString()); // 解析最后一行
					this.distribute.commit();
				} else {

				}
			} catch (FileNotFoundException e) {
				log.error(taskID + ": 解析失败,原因:文件不存在:" + this.fileName);
			} catch (IOException e) {
				log.error(taskID + ": 解析失败"+strLine+",原因:", e);
			} catch (Exception e) {
				log.error(taskID + ": 解析失败"+strLine+",原因：", e);
			} finally {
				// 释放动态库内存
				if (isDT) {
					closeInvoke();
				}

				strBuffer.delete(0, strBuffer.length());
				dsHandled.clear();
				resetState();
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
					}
				}
			}
		}

		return flag;
	}

	/**
	 * 释放mapData,isDT恢复默认值
	 */
	private void closeInvoke() {
		invoker.ReleaseMaper(mapData);
		isDT = false;
	}

	/** 将所有的状态都重新设值 */
	private void resetState() {
		log.debug(this.collectObjInfo.getTaskID() + ": isDataAreaStartFound=" + isDataAreaStartFound + " isDataAreaEndFound=" + isDataAreaEndFound
				+ " currDsID=" + currDsID);

		isPublicStart = false; // 公共数据区域开始标记是否已经找到
		isPublicEnd = false; // 公共数据区域结束标记是否已经找到
		isPublicFinished = false; // 公共数据区域是否已经处理完毕
		currDsID = -1;
		lastCommit = false;
		lastATempletP = null;

		isDataAreaStartFound = false;
		isDataAreaEndFound = false;
	}

	/**
	 * 处理公共数据区域
	 * 
	 * @param publicEle
	 */
	private void handlePublicArea(GenericSectionHeadP.Public publicEle) {
		if (publicEle == null) {
			isPublicFinished = true;
			isPublicStart = true;
			isPublicEnd = true;
			return;
		}

		// public描述的数据源是否已经处理,如果没有处理
		if (!isPublicFinished) {
			String startSign = publicEle.getStartSign(); // public开始字符
			String endSign = publicEle.getEndSign(); // public结束字符

			// 寻找public描述的数据源 ---只要public描述开始结束标记其中一个没找到就接着找
			if (!isPublicStart || !isPublicEnd) {
				if (isPublicStart) {
					isPublicEnd = strBuffer.indexOf(endSign) > -1; // 判断是否已经找到public的结束标记
				} else {
					isPublicStart = strBuffer.indexOf(startSign) > -1;
					if (isPublicStart)
						isPublicEnd = strBuffer.indexOf(endSign) > -1;
				}
			}
			// 同时找到开始标记和结束标记,处理其中的数据
			else {
				handlePublicData(publicEle);
				// 从strBuffer中去除public描述的数据
				strBuffer.delete(0, strBuffer.indexOf(endSign) + endSign.length());
				isPublicFinished = true;
			}
		}
	}

	boolean global = true;

	/**
	 * 处理一行数据; 从strBuffer中读取一行,每读取一行就返回此行并在strBuffer中删除此行
	 * 
	 * @return 返回null就是需要再读数据才能凑足一行
	 */
	private String handleLine(GenericSectionHeadP.Templet aTempletP, ClassEntry entry) throws Exception {
		String strLine = null;

		// 如果没有找到开始标记,则寻找是否此段数据存在开始标记
		if (!isDataAreaStartFound) {
			// 处理多行表头，并合并表头，设置索引对应关系。add on 2011-10-19
			if (aTempletP.isCombineflag() && global) {
				List<String> list = entry.headList;
				currDsID = 0;
				int i = 0;
				for (String head : list) {
					String[] tempHeads = null;
					if (aTempletP.getDsMap().get(currDsID).getMeta().getMultiSplitSign() != null) {
						tempHeads = head.replace("\"", "").split(aTempletP.getDsMap().get(currDsID).getMeta().getMultiSplitSign());
					} else {
						tempHeads = head.replace("\"", "").split(aTempletP.getDsMap().get(currDsID).getMeta().getHeadSplitSign());
					}
					entry.mapRows.put(i, Arrays.asList(tempHeads));
					i++;
				}
				process(entry);

				isDataAreaStartFound = true;

				// 找出表头中我们需要的字段在数据区域块中的索引
				String lineHead = findFieldIndexInHeadCombine(aTempletP, entry);

				// log.debug(lineHead);
				global = false;

				return lineHead;
			}
			// end

			// 读取一行数据
			strLine = readLine(strBuffer);
			// 返回null表示数据不足以凑足一行，需要继续读取文件
			if (strLine == null)
				return null;

			// 判断strBuffer中是否存在段开始标记，如果存在则获取他所属的DS编号 -- 即寻找表头所在行
			currDsID = getDsFromLineData(aTempletP, strLine);
			// 处理表头所在行
			if (currDsID > -1) {
				// 如果已处理ds列表中存在此id的话，则退出，说明模板配置有问题
				if (dsHandled.contains(currDsID)) {
					throw new ParseException("模板配置有误." + strLine);
				}

				isDataAreaStartFound = true;

				// 找出表头中我们需要的字段在数据区域块中的索引
				findFieldIndexInHead(aTempletP, strLine);
			}
		}
		// 如果已经找到开始标记，则寻找是否已经出现结束标记
		else {
			if (!isDataAreaEndFound) {
				DS ds = aTempletP.getDsMap().get(currDsID);
				String endSign = ds.getMeta().getEndSign();
				int ePos = -1;
				if (endSign == null) // 当用户没有配置结束标记时则认为这个文件剩下的都是属于一个数据源区域的，则一直读下去
				{
					ePos = -1;
					lastCommit = true;
					lastATempletP = aTempletP;
				} else
					ePos = strBuffer.indexOf(endSign);
				isDataAreaEndFound = ePos > -1;
				// 如果此次读取的数据块中包含结束标记，则把strBuffer中到结束标记处之间的数据全部处理完，并从strBuffer中移除掉
				if (isDataAreaEndFound) {
					StringBuilder tmpBuffer = new StringBuilder();
					// 这里为什么需要加上\n，是基于 按段解析的时候
					// 经常是两段之间有3个连续的\n也就是\n\n\n，这样，如果使用\n\n作为结束符，
					// 程序在比较的时候先截取了前面2个\n\n，这样就导致一个正常行的数据没有结束符\n了，所以我们人为的加上.这里必须加上否则会丢失一条数据
					tmpBuffer.append(strBuffer.substring(0, ePos)).append("\n");
					strBuffer.delete(0, ePos + endSign.length());
					String tmpLine = null;
					while ((tmpLine = readLine(tmpBuffer)) != null) {
						// 处理一行数据
						handleALineData(aTempletP, tmpLine);
					}

					// 如果剩下的缓冲中还有数据表明还需要处理，不能返回null，这里取巧返回空字符串
					if (strBuffer.length() > 0)
						strLine = "";
					else
						strLine = null;
					// 此数据区域的最后一块被处理完，复位标识位，准备下一个数据区域的判定
					dsHandled.add(currDsID); // 把当前处理的DS加入到已处理的列表中
					currDsID = -1;
					isDataAreaStartFound = false;
					isDataAreaEndFound = false;
					this.distribute.commit();
				}
				// 没找到结束标记，表明需要处理的是数据
				else {
					strLine = readLine(strBuffer);
					// 返回null表示数据不足以凑足一行，需要继续读取文件
					if (strLine == null)
						return null;

					// 处理一行数据
					handleALineData(aTempletP, strLine);
				}
			}
		}

		return strLine;
	}

	/**
	 * 从strBuffer中读取一行,每读取一行就返回此行并在strBuffer中删除此行
	 * 
	 * @return 返回null就是需要再读数据才能凑足一行
	 */
	private String readLine(StringBuilder buffer) {
		// 读取一行数据
		int endPos = buffer.indexOf("\n");
		if (endPos == -1)
			return null;

		String strLine = buffer.substring(0, endPos);
		buffer.delete(0, endPos + 1);

		strLine = strLine.replace("\n", "");
		strLine = strLine.replace("\r", "");
		return strLine;
	}

	/**
	 * 处理一行数据(除表头以外的数据)
	 * 
	 * @param aTempletP
	 * @param strLine
	 */
	private void handleALineData(GenericSectionHeadP.Templet aTempletP, String strLine) {
		if (Util.isNull(strLine))
			return;

		String splitSign = aTempletP.getDsMap().get(currDsID).getFields().getSplitSign(); // 获得数据中的字段分隔符号
		// String[] strFieldValues = strLine.split(splitSign);
		boolean blindFault = (this.fileName != null && (this.fileName.toLowerCase().contains("blindpoint_") || this.fileName.toLowerCase().contains(
				"faultorder_")));
		String[] strFieldValues = null;
		String multiSplitSign = aTempletP.getDsMap().get(currDsID).getFields().getMultiSplitSign();
		if (multiSplitSign != null) {
			strFieldValues = Util.split(strLine, multiSplitSign);
		} else
			strFieldValues = CSVLineParser.splitCSV(strLine, splitSign.toCharArray()[0], blindFault);
		int len = strFieldValues.length;
		Collection<Field> cFields = aTempletP.getDsMap().get(currDsID).getFields().getFields().values();
		double longitude = 0;
		double latitude = 0;
		long piece_id = 0;
		Region region = null;
		if (isDT) {
			for (Field f : cFields) {
				int indexInHead = f.getIndexInHead();
				if (indexInHead > -1 && indexInHead < len) {
					String val = strFieldValues[indexInHead].trim();
					if (indexInHead == 2) {// 经度
						longitude = Double.parseDouble(val);
					}
					if (indexInHead == 3) {// 纬度
						latitude = Double.parseDouble(val);
					}
				}
			}
			piece_id = invoker.JudgePointInPoly(mapData, longitude, latitude);
			try {
				region = RegionCache.getRegion(piece_id);
			} catch (Exception e) {
				log.error("获取多边形id报错", e);
			}
		}
		for (Field f : cFields) {
			int indexInHead = f.getIndexInHead();
			// 表头中存在列
			if (indexInHead > -1 && indexInHead < len) {
				String val = strFieldValues[indexInHead].trim();
				if (val == null || "null".equals(val)) {
					val = "";
				}
				// 增加对如这种时间格式“2013-7-25 6:55:00.0”的处理
				if (f.getSpecialTime() != null && "1".equals(f.getSpecialTime())) {
					if (val.indexOf(".") > -1)
						val = val.substring(0, val.indexOf("."));
				}
				// 增加对"460,460"的处理，用逗号分拆，只取其中一个值
				if (f.getIsSplit() != null && "true".equals(f.getIsSplit())) {
					if (val.indexOf(f.getSplitSign()) > -1) {
						String[] valArray = Util.split(val, f.getSplitSign());
						int index = Integer.parseInt(f.getIndexOfValue());
						val = valArray[index];
						val = val.replace("'", "");
					}
				}
				f.setValue(val);
			}
			// 表头中不存在列->回填
			else if (isDT) {
				if (f.getName().equals("GRID_ID")) {
					f.setValue(getGrid(longitude, latitude));
				} else if (f.getName().equals("PIECE_ID")) {
					f.setValue(String.valueOf(piece_id));
				} else if (f.getName().equals("PIECE_NAME")) {
					f.setValue(region == null ? "" : region.getPieceName());
				} else if (f.getName().equals("REGION_ID")) {
					f.setValue(region == null ? "" : String.valueOf(region.getRegionId()));
				} else if (f.getName().equals("REGION_NAME")) {
					f.setValue(region == null ? "" : region.getRegionName());
				}
			}
		}

		// liangww add 2012-03-30
		beforDistribute(cFields);

		try {
			if (this.distribute != null)
				this.distribute.distribute(aTempletP, currDsID);
			else
				log.error(this.collectObjInfo.getTaskID() + " 分发器为null，请检查任务配置，可能igp_conf_task表中的DISTRIBUTORID配错或为空，task_id="
						+ this.collectObjInfo.getTaskID());
		} catch (Exception e) {
			log.error(this.collectObjInfo.getTaskID() + ": 分发数据异常,原因:", e);
		}
	}

	/**
	 * 获取网格id
	 * 
	 * @param longitude
	 * @param latitude
	 * @return
	 */
	private String getGrid(double longitude, double latitude) {
		String longi = String.valueOf(Math.round(longitude * 1000));
		String lati = String.valueOf(Math.round(latitude * 1000));
		return longi + lati;
	}

	/**
	 * 从一行数据中获取此数据属于哪个DS
	 * 
	 * @param aTempletP
	 * @param strLine
	 * @return 返回DS编号
	 */
	private int getDsFromLineData(GenericSectionHeadP.Templet aTempletP, String strLine) {
		String line = strLine.toLowerCase(); // requried字段大小写不敏感
		int id = -1;
		Collection<DS> cDS = aTempletP.getDsMap().values();
		for (DS ds : cDS) {
			List<String> roList = ds.getRequiredOccur();
			boolean found = true;
			for (String ro : roList) {
				if (line.indexOf(ro.toLowerCase()) == -1) {
					found = false;
					break;
				}
			}
			// 如果找到数据区域开始标记
			if (found) {
				id = ds.getId();
				break;
			}
		}

		return id;
	}

	/**
	 * 寻找解析模板中定义的字段在表头所在的行中的索引
	 * 
	 * @param aTempletP
	 * @param strLine
	 */
	private void findFieldIndexInHead(GenericSectionHeadP.Templet aTempletP, String strLine) {
		String[] strFieldNames = null;
		// String multiSplitSign = aTempletP.getDsMap().get(currDsID).getMeta().getMultiSplitSign();
		// if (multiSplitSign != null) {
		// // strFieldNames = strLine.replace("\"", "").split(aTempletP.getDsMap().get(currDsID).getMeta().getMultiSplitSign());
		// strFieldNames = Util.split(strLine.replace("\"", ""), multiSplitSign);
		// } else {
		// String tmpHead = switchLine(strLine, splitSign);
		// strFieldNames = Util.split(tmpHead, splitSign);
		// }
		if (aTempletP.getDsMap().get(currDsID).getMeta().getMultiSplitSign() != null) {
			strFieldNames = strLine.replace("\"", "").split(aTempletP.getDsMap().get(currDsID).getMeta().getMultiSplitSign());
		} else {
			strFieldNames = strLine.replace("\"", "").split(aTempletP.getDsMap().get(currDsID).getMeta().getHeadSplitSign());
		}

		Collection<Field> cFields = aTempletP.getDsMap().get(currDsID).getFields().getFields().values();
		int len = strFieldNames.length;
		
		if(aTempletP.getDsMap().get(currDsID).isIndexDepend()){
			int i = 0;
			for (Field f : cFields) {
				f.setIndexInHead(i++);
			}
		}else{
			// 遍历寻找解析模板中定义的字段对应在表头里是第几个
			for (Field f : cFields) {
				String fName = f.getName().trim();
				for (int i = 0; i < len; i++) {
					String strFName = strFieldNames[i].replace((char) 0xfeff, '\0').trim();
					if (strFName.equalsIgnoreCase(fName)) {
						f.setIndexInHead(i);
						break;
					}
				}
			}
		}
	}

	/**
	 * 转换分隔符(,')
	 * 
	 * @param line
	 * @return String 转换后的字符串
	 */
	public static String switchLine(String line, String splitSign) {
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;// 双引号标记
		char tmpChar = '0';
		try {
			for (char s : line.toCharArray()) {
				if ((s == ',' || s == '，') && flag == false) {
					strList.add(sb.toString() + splitSign);
					sb = new StringBuffer();
					tmpChar = s;
					continue;
				}
				if (s == '\"') {
					if (flag == true) {
						flag = false;
					} else if (tmpChar == ',' || tmpChar == '，' || sb.length() == 0) {
						flag = true;
					} else {
						sb.append(s);
					}
					tmpChar = s;
					continue;
				}
				sb.append(s);
				tmpChar = s;
			}
		} catch (Exception e) {
		}
		if (sb.toString().length() > 0) {
			strList.add(sb.toString());
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += ss;
		}
		return strValue;
	}

	/**
	 * 转换分隔符(,')
	 * 
	 * @param line
	 * @return String 转换后的字符串
	 */
	public static String switchLine(String line, char orgiSplitSign, String splitSign) {
		List<String> strList = new ArrayList<String>();
		StringBuffer sb = new StringBuffer();
		boolean flag = false;// 双引号标记
		char tmpChar = '0';
		try {
			for (char s : line.toCharArray()) {
				if ((s == orgiSplitSign) && flag == false) {
					strList.add(sb.toString() + splitSign);
					sb = new StringBuffer();
					tmpChar = s;
					continue;
				}
				if (s == '\"') {
					if (flag == true) {
						flag = false;
					} else if (tmpChar == orgiSplitSign) {
						flag = true;
					} else {
						sb.append(s);
					}
					tmpChar = s;
					continue;
				}
				sb.append(s);
				tmpChar = s;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (sb.toString().length() > 0) {
			strList.add(sb.toString());
		}
		String strArray[] = strList.toArray(new String[strList.size()]);
		String strValue = "";
		for (String ss : strArray) {
			strValue += ss;
		}
		return strValue;
	}

	/**
	 * 将多行表头合并后，并设置字段对应的索引编号
	 * 
	 * @param aTempletP
	 * @param entry
	 * @return
	 */
	private String findFieldIndexInHeadCombine(GenericSectionHeadP.Templet aTempletP, ClassEntry entry) {
		List<List<String>> list = entry.result;
		// currDsID
		Collection<Field> cFields = aTempletP.getDsMap().get(currDsID).getFields().getFields().values();

		StringBuilder sb = new StringBuilder();

		// 遍历寻找解析模板中定义的字段对应在表头里是第几个
		int c = 0;
		for (Field f : cFields) {
			String fName = f.getName();
			for (int j = c; j < list.size(); j++) {
				List<String> l = list.get(j);

				boolean b = false;
				for (int m = 0; m < l.size(); m++) {
					// System.out.println(l.get(m));
					if (l.get(m).equalsIgnoreCase(fName)) {
						f.setIndexInHead(j);
						b = true;
						break;
					}
				}
				if (b) {
					sb.append(fName).append(",");
					break;
				}
			}
			c++;
		}
		return sb.toString();
	}

	/**
	 * 处理public的数据
	 * 
	 * @param publicEle
	 */
	private void handlePublicData(GenericSectionHeadP.Public publicEle) {
		Fields fields = publicEle.getFields();
		if (fields == null || fields.getFields() == null || fields.getFields().size() <= 0)
			return;

		Set<Entry<Integer, Field>> fieldSet = fields.getFields().entrySet();
		for (Entry<Integer, Field> entry : fieldSet) {
			Field field = entry.getValue();
			String startSign = field.getStartSign();
			String endSign = field.getEndSign();
			int beginIndex = strBuffer.indexOf(startSign);
			int endIndex = strBuffer.indexOf(endSign, beginIndex + startSign.length());
			String fValue = strBuffer.substring(beginIndex + startSign.length(), endIndex);
			fValue = fValue.trim();
			field.setValue(fValue);
			// log.debug(fValue);
		}
	}

	/**
	 * ClassEntry
	 * 
	 * @author liuwx 2011-10-19
	 */
	class ClassEntry {

		int index;

		Map<Integer, List<String>> mapRows;

		Map<Integer, List<String>> mapColumns;

		List<List<String>> result;

		List<String> headList;

		public ClassEntry() {
			mapRows = new HashMap<Integer, List<String>>();
			mapColumns = new HashMap<Integer, List<String>>();
			result = new ArrayList<List<String>>();
			headList = new ArrayList<String>();
		}
	}

	/**
	 * 行列转换
	 * 
	 * @param en
	 */
	public void process(ClassEntry en) {
		for (int i = 0; i < en.mapRows.size(); i++) {
			List<String> list1 = en.mapRows.get(i);
			for (int j = 0; j < list1.size(); j++) {
				if (util.Util.isNull(list1.get(j)))
					continue;
				if (en.mapColumns.get(j) == null) {
					en.mapColumns.put(j, new ArrayList<String>());
				}
				if (!en.mapColumns.get(j).contains(list1.get(j)))
					en.mapColumns.get(j).add(list1.get(j));
			}
		}
		for (int m = 0; m < en.mapColumns.size(); m++) {
			List<String> list = en.mapColumns.get(m);
			if (list == null || (list.size() == 0 && util.Util.isNull(list.get(0))))
				continue;
			en.result.add(list);
		}
	}

	/**
	 * 在处理一行数据时，分派前的操作（留给子类处理）
	 * 
	 * @author liangww 2012-03-30
	 * @see MacroParser
	 * @param cFields
	 */
	protected void beforDistribute(Collection<Field> cFields) {

	}

	public static void main(String[] args) {
		// CollectObjInfo obj = new CollectObjInfo(999);
		// obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		// obj.setDevInfo(new DevInfo());
		//
		// TempletBase pTemp = new GenericSectionHeadP();
		// pTemp.buildTmp(407003);
		// obj.setParseTemplet(pTemp);
		//
		// GenericSectionHeadD dTemp = new GenericSectionHeadD();
		// dTemp.buildTmp(407004);
		// obj.setDistributeTemplet(dTemp);
		//
		// GenericSectionHeadParser parser = new GenericSectionHeadParser(obj);
		// parser.fileName = "C:\\Users\\ChenSijiang\\Desktop\\Domain124_Handoverstatisticmeasurement_15Aug2010_1800-15Aug2010_1900.csv";
		// // 它只是和templet模板中的file属性相等即可
		// parser.setDsConfigName("/export/home/omc/tmp/ftp/csv/ftp/Domain*_Handoverstatisticmeasurement_%%D%%EM%%Y_%%H%%m-%%ND%%NEM%%NY_%%NH%%Nm.csv|3600000");
		//
		// parser.parseData();
		// System.out.println((int) '\n');
		// System.out.println((int) '\r');
		String strLine = "TS201406230012453|辽源|15143786811||||||普通客户|归档|业务受理|固网受理|有线宽带密码重置||\"n0437xyz10816025  |吉林省辽源市东丰县通山街丰泽苑B区EPON丰泽苑B区1号楼3单元402\"||普通|2014/6/23 20:56:01|2014/6/24 20:59:07||||2014/6/23 20:59:07||";
		String[] strFieldValues = CSVLineParser.splitCSV(strLine, '|', false);
		for (String str : strFieldValues) {
			System.out.println(str);
		}
	}
}
