package util.loganalyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import util.LogMgr;
import util.SqlldrResult;
import util.Util;
import util.file.FileUtil;

/**
 * oracle Sql loader 日志分析工具
 * 
 * @author liuwx
 */
public class SqlLdrLogAnalyzer implements LogAnalyzer
{
	private static Map<String, String> configMap = null;//new HashMap<String, String>();
	// 临时存储config的键值对信息
	// private Map<String, String> cmap = null;
	// 存储用户自定义规则列表各正则表达式匹配后的结果
	private List<String> matchRuleList = new ArrayList<String>();
	// 存储用户自定义规则列表
	private static List<String> uerDefineRuleList = new ArrayList<String>();
	// 临时存储用户自定义规则列表
	// private List<String> tempUserDefineRuleList = null;
	private SqlldrResult sqlldrResult = new SqlldrResult();
	// 配置模板文件名
	private static String configPath = "SqlLdrLogAnalyseTemplet.xml";
	
	// 通配符模板
	private static String templetMatchFile = "SqlLdrLogAnalyseTemplet*.xml";
	
	// 中文标识的模板
	private static String templetMatchFile_ch = "_ch.xml";
	
	// 中文标识的模板
	private static String templetMatchFile_en = "_en.xml";
	
	// 配置信息目录
	private static String TempletFileDir= "." + File.separator + "conf";
	
	// 配置信息路径
	private static String TempletFilePath = TempletFileDir + File.separator + configPath;

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	public SqlLdrLogAnalyzer()
	{
		// cmap = new HashMap<String, String>(configMap);
		// tempUserDefineRuleList = new ArrayList<String>(uerDefineRuleList);
	}

	static
	{
		try
		{
//			load();
		}
		catch (Exception e)
		{
			configMap = null;
			uerDefineRuleList = null;
			logger.error("加载SqlLdrLogAnalyseTemplet.xml时异常", e);
		}
	}

	/**
	 * 分析日志文件
	 */
	@Override
	public SqlldrResult analysis(String fileName) throws LogAnalyzerException
	{
		try
		{
			return analysis(new FileInputStream(fileName));
		}
		catch (FileNotFoundException e)
		{
			throw new LogAnalyzerException("文件未找到", e);
		}
	}

	/*
	 * 分析日志文件
	 */
	@Override
	public SqlldrResult analysis(InputStream in) throws LogAnalyzerException
	{
		BufferedReader br = null;
		InputStreamReader isr = null;
		try
		{

			isr = new InputStreamReader(in, "GBK");
			br = new BufferedReader(isr);
			
			/* 找模板 */
			String head = br.readLine();
			while( head == null || "".equals(head)){
				head = br.readLine();
			}
			if(configMap == null){
				// 判断oracle中/英文版本
				boolean isChinese = isChineseVersion(head);
				
				//遍历模板
				String templetFile = null;
				List<String> list = FileUtil.getFileNames(TempletFileDir, templetMatchFile);
				if(list == null || list.size() == 0){
					templetFile = null;;
				} else if(list.size() == 1){
					templetFile = list.get(0);
				} else if (list.size() == 2){
					templetFile = findRightTemplet(isChinese, list);
				} else {
					templetFile = TempletFilePath;
				}
				
				if(templetFile == null){
					throw new LogAnalyzerException("sqlldr日志解析模板不存在");
				}
				
				//加载中/英文模板
				loadTemplet(templetFile);
				
				//最后验证
				if(isChinese(configMap.get("tableName")) != isChinese){
					throw new LogAnalyzerException("sqlldr日志解析模板与oracle客户端的版本不匹配(中英文不匹配)");
				}
			}
			
			// 判断是否是oracle 日志文件
			boolean isOracleLog = isOracleLog(head);
			if ( !isOracleLog )
			{
				// sysbase处理方式 暂时没有处理
				return new SqlldrResult();
			}
			else
			{
				// oracle 处理方式
				String lineData = null;
				try
				{
					while ((lineData = br.readLine()) != null)
					{
						lineData = lineData.trim();
						getSqlldrAnalyseResult(lineData);
					}
				}
				catch (Exception e)
				{
					IOUtils.closeQuietly(br);
					IOUtils.closeQuietly(isr);
					IOUtils.closeQuietly(in);
					throw new LogAnalyzerException("分析log文件出现异常", e);
				}
				sqlldrResult.setRuleList(matchRuleList);
			}
		}
		catch (Exception e)
		{
			logger.error("分析SQLLDR日志时异常", e);
		}
		finally
		{
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(in);
		}
		return sqlldrResult;
	}

	/*
	 * 销毁
	 */
	@Override
	public void destory()
	{
		configMap = null;
		matchRuleList = null;
		uerDefineRuleList = null;
	}

	private synchronized static void loadTemplet(String templet) throws Exception
	{
		if(configMap != null){
			return;
		}
		Map<String, String> tempConfigMap = new HashMap<String, String>();
		String isOracleLog;// 是否是oracle日志文件
		String tableName;// 表名
		String loadSuccCount;// 载入成功行数
		String data;// 数据错误行数没有加载
		String when;// when子句失败行数没有加载
		String nullField;// 字段为空行数
		String skip;// 跳过的逻辑记录总数
		String read;// 读取的逻辑记录总数
		String refuse;// 拒绝的逻辑记录总数
		String abandon;// 废弃的逻辑记录总数
		String startTime;// 开始运行时间
		String endTime;// 运行结束时间
		String rule;// 用户自定义规则
		if ( templet == null || templet.trim().equals("") )
			throw new LogAnalyzerException("文件为空，请检验");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		try
		{
			builder = factory.newDocumentBuilder();
		}
		catch (ParserConfigurationException e)
		{
			throw new LogAnalyzerException("解析SqlLdrLogAnalyseTemplet.xml配置模板发生异常");
		}
		Document doc = null;
		File file1 = new File(templet);
		try
		{
			doc = builder.parse(file1);
		}
		catch (Exception e)
		{
			throw new LogAnalyzerException("解析SqlLdrLogAnalyseTemplet.xml配置模板发生异常", e);
		}
		/* 获取动用信息 ,system-rule节点信息 */
		NodeList systemRuleNodeList = doc.getElementsByTagName("system-rule");
		if ( systemRuleNodeList.getLength() >= 1 )
		{
			if ( doc.getElementsByTagName("is-oracle-log").item(0).getFirstChild() == null )
			{
				isOracleLog = "";
			}
			else
			{
				isOracleLog = doc.getElementsByTagName("is-oracle-log").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("isOracleLog", isOracleLog);
			if ( doc.getElementsByTagName("table-name").item(0).getFirstChild() == null )
			{
				tableName = "";
			}
			else
			{
				tableName = doc.getElementsByTagName("table-name").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("tableName", tableName);
			if ( doc.getElementsByTagName("load-succ-count").item(0).getFirstChild() == null )
			{
				loadSuccCount = "";
			}
			else
			{
				loadSuccCount = doc.getElementsByTagName("load-succ-count").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("loadSuccCount", loadSuccCount);
			if ( doc.getElementsByTagName("data").item(0).getFirstChild() == null )
			{
				data = "";
			}
			else
			{
				data = doc.getElementsByTagName("data").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("data", data);
			if ( doc.getElementsByTagName("when").item(0).getFirstChild() == null )
			{
				when = "";
			}
			else
			{
				when = doc.getElementsByTagName("when").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("when", when);
			if ( doc.getElementsByTagName("null-field").item(0).getFirstChild() == null )
			{
				nullField = "";
			}
			else
			{
				nullField = doc.getElementsByTagName("null-field").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("nullField", nullField);
			if ( doc.getElementsByTagName("skip").item(0).getFirstChild() == null )
			{
				skip = "";
			}
			else
			{
				skip = doc.getElementsByTagName("skip").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("skip", skip);
			if ( doc.getElementsByTagName("read").item(0).getFirstChild() == null )
			{
				read = "";
			}
			else
			{
				read = doc.getElementsByTagName("read").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("read", read);
			if ( doc.getElementsByTagName("refuse").item(0).getFirstChild() == null )
			{
				refuse = "";
			}
			else
			{
				refuse = doc.getElementsByTagName("refuse").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("refuse", refuse);
			if ( doc.getElementsByTagName("abandon").item(0).getFirstChild() == null )
			{
				abandon = "";
			}
			else
			{
				abandon = doc.getElementsByTagName("abandon").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("abandon", abandon);
			if ( doc.getElementsByTagName("start-time").item(0).getFirstChild() == null )
			{
				startTime = "";
			}
			else
			{
				startTime = doc.getElementsByTagName("start-time").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("startTime", startTime);
			if ( doc.getElementsByTagName("end-time").item(0).getFirstChild() == null )
			{
				endTime = "";
			}
			else
			{
				endTime = doc.getElementsByTagName("end-time").item(0).getFirstChild().getNodeValue();
			}
			tempConfigMap.put("endTime", endTime);
		}
		/* 获取动用信息 ,user define rule节点信息 */
		NodeList userRuleNodeList = doc.getElementsByTagName("user-define-rule");
		if ( userRuleNodeList.getLength() >= 1 )
		{
			for (int i = 0; i < userRuleNodeList.getLength(); i++)
			{
				Node userRule = userRuleNodeList.item(i);
				NodeList nodelist = userRule.getChildNodes();
				for (int j = 0; j < nodelist.getLength(); j++)
				{
					Node nodeRule = nodelist.item(j);
					if ( nodeRule.getNodeType() == Node.ELEMENT_NODE
							&& nodeRule.getNodeName().toLowerCase().equals("rule") )
					{
						rule = getNodeValue(nodeRule);
						uerDefineRuleList.add(rule);
					}
				}
			}
		}
		configMap = tempConfigMap;
	}

	@Override
	public void init() throws LogAnalyzerException
	{
	}

	// 获取当前节点的值
	private static String getNodeValue(Node CurrentNode)
	{
		String strValue = "";
		NodeList nodelist = CurrentNode.getChildNodes();
		if ( nodelist != null )
		{
			for (int i = 0; i < nodelist.getLength(); i++)
			{
				Node tempnode = nodelist.item(i);
				if ( tempnode.getNodeType() == Node.TEXT_NODE )
				{
					strValue = tempnode.getNodeValue();
				}
			}
		}
		return strValue;
	}

	// 判断是否是oracle日志文件
	private boolean isOracleLog(String head)
	{
		return head.contains(configMap.get("isOracleLog"));
	}
	
	// 判断是否是oracle日志文件
	private boolean isChineseVersion(String head)
	{
		return isChinese(head);
	}

	private void getSqlldrAnalyseResult(String line)
	{
		boolean b = false;
		for (int i = 0; i < uerDefineRuleList.size(); i++)
		{
			String regRule = uerDefineRuleList.get(i);
			if ( regRule == null
					|| (regRule != null && "".equals(regRule.trim())) )
			{
				continue;
			}
			String result = regexQueryGroup(line, uerDefineRuleList.get(i), 0);
			if ( Util.isNotNull(result) )
			{
				// tempUserDefineRuleList.remove(i);
				b = true;
			}
		}
		if ( b )
			return;

		for (Entry<String, String> en : configMap.entrySet())
		{
			String key = en.getKey();
			if ( "isOracleLog".equalsIgnoreCase(key) )
				continue;
			String value = en.getValue();
			String result = null;
			try
			{
				result = regexQueryGroup(line, value, 1);
			}
			catch (Exception e)
			{
				logger.error("正则表达示出现异常," + value + "原因:{}", e);
			}
			if ( Util.isNotNull(result) )
			{
				// cmap.remove(key);
				if ( "tableName".equals(key) )
				{
					result = result.replace("'", "");
					result = result.replaceAll("\"", "");
					if(result.indexOf(".") > -1)
						result = result.substring(result.indexOf(".") + 1);
					sqlldrResult.setTableName(result);
				}
				else if ( "loadSuccCount".equals(key) )
				{
					sqlldrResult.setLoadSuccCount(stringToInt(result));
				}
				else if ( "data".equals(key) )
				{
					sqlldrResult.setData(stringToInt(result));
				}
				else if ( "when".equals(key) )
				{
					sqlldrResult.setWhen(stringToInt(result));
				}
				else if ( "nullField".equals(key) )
				{
					sqlldrResult.setNullField(stringToInt(result));
				}
				else if ( "skip".equals(key) )
				{
					sqlldrResult.setSkip(stringToInt(result));
				}
				else if ( "read".equals(key) )
				{
					sqlldrResult.setRead(stringToInt(result));
				}
				else if ( "refuse".equals(key) )
				{
					sqlldrResult.setRefuse(stringToInt(result));
				}
				else if ( "abandon".equals(key) )
				{
					sqlldrResult.setAbandon(stringToInt(result));
				}
				else if ( "startTime".equals(key) )
				{
					sqlldrResult.setStartTime(result);
				}
				else if ( "endTime".equals(key) )
				{
					sqlldrResult.setEndTime(result);
				}
			}
		}
	}

	// 通过正则表达式查找
	private String regexQueryGroup(String str, String regEx, int group)
	{
		String resultValue = "";
		if ( regEx == null || (regEx != null && "".equals(regEx.trim())) ) { return resultValue; }
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		boolean result = m.find();// 查找是否有匹配的结果
		if ( result )
		{
			resultValue = m.group(group);// 找出匹配的结果
			if ( group == 0 )
			{
				if ( !matchRuleList.contains(resultValue) )
				{
					matchRuleList.add(resultValue);
				}
			}
		}
		return resultValue;
	}

	private int stringToInt(String str)
	{
		if ( str == null || (str != null && str.trim().equals("")) ) { return 0; }
		str = str.trim();
		return Integer.valueOf(str).intValue();
	}
	
	/**
	 * 找到合适的解析模板
	 * @param isChinese
	 * @return
	 */
	public String findRightTemplet(boolean isChinese, List<String> list){
		if(list == null || list.size() != 2)
			return null;
		
		String temp1 = list.get(0);
		String temp2 = list.get(1);
		if(isChinese) {
			if(temp1.toLowerCase().endsWith(templetMatchFile_ch)){
				return temp1;
			}
			if(temp2.toLowerCase().endsWith(templetMatchFile_ch)){
				return temp2;
			}
			if(temp1.toLowerCase().endsWith(templetMatchFile_en)){
				return temp2;
			}
			if(temp2.toLowerCase().endsWith(templetMatchFile_en)){
				return temp1;
			}
		} else {
			if(temp1.toLowerCase().endsWith(templetMatchFile_ch)){
				return temp2;
			}
			if(temp2.toLowerCase().endsWith(templetMatchFile_ch)){
				return temp1;
			}
			if(temp1.toLowerCase().endsWith(templetMatchFile_en)){
				return temp1;
			}
			if(temp2.toLowerCase().endsWith(templetMatchFile_en)){
				return temp2;
			}
		}
		return "";
	}
	
	// 根据Unicode编码完美的判断中文汉字和符号
    private boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
    }

    // 完整的判断中文汉字和符号
    public boolean isChinese(String strName) {
        char[] ch = strName.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (isChinese(c)) {
                return true;
            }
        }
        return false;
    }
    
	public static void main(String[] args)
	{
//		List<String> list = new ArrayList<String>();
//		list.add("表 \"DS\".\"A_L_CELL_BLER_H\",已加载从每个逻辑记录");
//		list.add("表 A_L_CELL_BLER_H,已加载从每个逻辑记录");
//		list.add("插入选项对此表 APPEND 生效");
//		int i = 0;
//		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
//		for(i = 0;i<list.size();i++){
//			String re = analyzer.regexQueryGroup(list.get(i), "表\\s?(\\S+),", 1);
//			re = re.replace("'", "");
//			re = re.replaceAll("\"", "");
//			if(re.indexOf(".") > -1)
//				re = re.substring(re.indexOf(".") + 1);
//			System.out.println(re);
//		}
		
		try
		{
			InputStream in = new FileInputStream("/home/yuy/my/yuyi/yy/igp_v1/现场反馈问题/广东阿朗/0_101104_20130401000000_0.log");
			new SqlLdrLogAnalyzer().analysis(in);
			IOUtils.closeQuietly(in);
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
