package parser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

import task.CollectObjInfo;
import task.DevInfo;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import templet.GenericSectionHeadP.DS;
import templet.GenericSectionHeadP.Field;
import templet.GenericSectionHeadP.Templet;
import templet.TempletBase;
import util.Util;
import distributor.GenericSectionHeadDistributor;
import framework.ConstDef;

/**
 * 没有头的按段解析 txt or csv
 * 
 * @author liangww
 * @since
 * @see GenericSectionHeadP
 * @see GenericSectionHeadD
 * @see GenericSectionHeadDistributor
 */
public class GenericUnSectionHeadParser extends Parser {

	private int currDsID = -1; // 当前处理的DS所属的编号

	public GenericUnSectionHeadParser() {
		super();
	}

	public GenericUnSectionHeadParser(CollectObjInfo obj) {
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
			if (this.getDsConfigName().equalsIgnoreCase(fName.trim())
					|| logicEquals(this.getDsConfigName(), ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()))
					|| FilenameUtils.wildcardMatch(FilenameUtils.getName(localFilename),
							ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()))
					|| ConstDef.ParseFilePath(fName.trim(), collectObjInfo.getLastCollectTime()).endsWith(FilenameUtils.getName(localFilename))) {
				aTempletPs.add(templets.get(fName));
			}
		}
		return aTempletPs;
	}

	@Override
	public boolean parseData() {
		if (Util.isNull(this.fileName))
			return false;

		long taskID = this.collectObjInfo.getTaskID();

		TempletBase tBaseP = this.collectObjInfo.getParseTemplet();
		if (!(tBaseP instanceof GenericSectionHeadP))
			return false;

		System.out.println();
		GenericSectionHeadP templetP = (GenericSectionHeadP) tBaseP;
		// 找出解析此文件需要依赖的Templet对象
		List<GenericSectionHeadP.Templet> aTempletPs = getTempletByFileName(templetP, this.fileName);
		if (aTempletPs.size() == 0) {
			log.error(taskID + ": 解析失败,原因：找不到对应的解析模板." + this.fileName);
			return false;
		}
		for (GenericSectionHeadP.Templet aTempletP : aTempletPs) {
			//
			if (aTempletP.getDsMap().size() != 1) {
				log.error(taskID + ": 解析失败,原因：aTempletP的ds个数不为1， aTempletP_id=" + aTempletP.getId());
				return false;
			}
		}

		boolean flag = true;
		for (GenericSectionHeadP.Templet aTempletP : aTempletPs) {
			TempletBase tBaseD = this.collectObjInfo.getDistributeTemplet();
			if (!(tBaseD instanceof GenericSectionHeadD)) {
				flag = false;
				continue;
			}

			// 因为这是固定为一个的
			Iterator<DS> cDSItr = aTempletP.getDsMap().values().iterator();
			this.currDsID = cDSItr.next().getId();

			GenericSectionHeadD templetD = (GenericSectionHeadD) tBaseD;

			int pTid = aTempletP.getId(); // 解析模板中templet中的id
			// 寻找解析模板id是否在分发模板中对应存在，不存在则不予以解析
			GenericSectionHeadD.Templet aTempletD = templetD.getTemplet(pTid);
			if (aTempletD == null) {
				log.error(taskID + ": 解析失败:原因:找不到对应的分发模板. 解析模板编号=" + pTid);
				flag = false;
				continue;
			}

			String tableName = aTempletD.getTables().get(0).getName().toUpperCase();

			BufferedReader br = null;
			try {
				br = null;
				if (aTempletP.getEncoding() != null)
					br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName), aTempletP.getEncoding()));
				else
					br = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName)));

				/* templet节点的skipLine属性，控制跳过文件多少行。 */
				if (aTempletP.getSkipLine() > 0) {
					for (int i = 0; i < aTempletP.getSkipLine(); i++) {
						br.readLine();
					}
				}

				String strLine = null;
				while ((strLine = br.readLine()) != null) {
					if (strLine.trim().length() == 0)
						continue;

					handleALineData(aTempletP, strLine); // 解析一行

				}
				this.distribute.commit();
			} catch (FileNotFoundException e) {
				log.error(taskID + ": 解析失败,原因:文件不存在:" + this.fileName);
			} catch (IOException e) {
				log.error(taskID + ": 解析失败,原因:", e);
			} catch (Exception e) {
				log.error(taskID + ": 解析失败,原因：", e);
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
					}
				}

				this.currDsID = -1;
			}
		}

		return flag;
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
		String[] strFieldValues = strLine.split(splitSign);
		int len = strFieldValues.length;
		Collection<Field> cFields = aTempletP.getDsMap().get(currDsID).getFields().getFields().values();
		for (Field f : cFields) {
			int indexInHead = f.getIndex();
			if (indexInHead > -1 && indexInHead < len) {
				f.setValue(strFieldValues[indexInHead].trim());
			}
		}

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
	 * 在处理一行数据时，分派前的操作（留给子类处理）
	 * 
	 * @author liangww 2012-03-30
	 * @see MacroParser
	 * @param cFields
	 */
	protected void beforDistribute(Collection<Field> cFields) {

	}

	public static void main(String[] args) {
		CollectObjInfo obj = new CollectObjInfo(999);
		obj.setLastCollectTime(new Timestamp(new Date().getTime()));
		obj.setDevInfo(new DevInfo());

		TempletBase pTemp = new GenericSectionHeadP();
		pTemp.buildTmp(407003);
		obj.setParseTemplet(pTemp);

		GenericSectionHeadD dTemp = new GenericSectionHeadD();
		dTemp.buildTmp(407004);
		obj.setDistributeTemplet(dTemp);

		GenericUnSectionHeadParser parser = new GenericUnSectionHeadParser(obj);
		parser.fileName = "C:\\Users\\ChenSijiang\\Desktop\\Domain124_Handoverstatisticmeasurement_15Aug2010_1800-15Aug2010_1900.csv";
		// 它只是和templet模板中的file属性相等即可
		parser.setDsConfigName("/export/home/omc/tmp/ftp/csv/ftp/Domain*_Handoverstatisticmeasurement_%%D%%EM%%Y_%%H%%m-%%ND%%NEM%%NY_%%NH%%Nm.csv|3600000");

		parser.parseData();
		System.out.println((int) '\n');
		System.out.println((int) '\r');
	}
}
