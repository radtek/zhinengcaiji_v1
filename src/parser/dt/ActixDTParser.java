package parser.dt;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

import framework.ConstDef;

import parser.Parser;
import templet.GenericSectionHeadD;
import templet.GenericSectionHeadP;
import templet.TempletBase;
import templet.GenericSectionHeadP.Templet;
import util.Util;
import util.opencsv.CSVReader;

/**
 * Actix路测数据解析parser类
 * 
 * @author chenrongqiang @ 2013-10-17
 */
public class ActixDTParser extends Parser {

	@Override
	public boolean parseData() throws Exception {
		if (Util.isNull(this.fileName))
			return false;
		
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
		
		FileInputStream inputStream = new FileInputStream(this.fileName);
		Reader reader = new BufferedReader(new InputStreamReader(inputStream));
		CSVReader csvReader = new CSVReader(reader);
		String[] lines = null;
		
		boolean flag = true;
		for (GenericSectionHeadP.Templet aTempletP : aTempletPs) {
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
			
			while((lines = csvReader.readNext()) != null){
				
			}
		}
		return false;
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
			}
		}
		return aTempletPs;
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

}
