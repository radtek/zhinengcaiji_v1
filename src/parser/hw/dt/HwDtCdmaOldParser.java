package parser.hw.dt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import parser.AbstractStreamParser;
import parser.hw.dt.bean.LongiLatitude;
import parser.hw.dt.bean.PerLineState;
import sqlldr.SqlldrForHwDtImpl;
import templet.Table;
import templet.Table.Column;
import templet.TempletBase;
import templet.hw.cdma.dt.DtCdmaTempletOldD;
import util.LogMgr;
import collect.LoadConfMapDevToNe;
import exception.ParseException;
import framework.SystemConfig;

/**
 * 华为 CDMA 路测数据的解析Parser类
 * 
 * @author lijiayu @ 2013年9月10日
 * @author sunt @ 2015-08-06 发现厂家文件新老版本同时存在,所以恢复该解析类
 */
public class HwDtCdmaOldParser extends AbstractStreamParser{

	private static final Logger LOGGER = LogMgr.getInstance().getSystemLogger();

	// 表的前缀
	public static final String TABLE_PREFIX = "CLT_DT_";

	// 组装Sqlldr fileds terminated by "|"
	public static final String SQLLDER_SEPARATOR = "|";

	// 路测数据的拆分符 "\t"
	public static final String DT_SPLIT = "\t";

	// cellName这个字段在数据库长度是300
	public static final int CELL_NAME_LENGTH = 300;

	// 网元数据一般长度为16预留长度为18
	public static final int NI_CELL_LENGTH = 18;

	// 用于生成 GPS_LINK_SEQ（GPS唯一索引）
	public String time = null;

	// 记录第一个时分秒
	public String startime = null;

	// 记录最后一个时分秒
	public String endtime = null;

	// 全局的日期 一个文件一个日期
	public String dateStr;

	// 用于查找最近时间点的关联gpsLinkSeq
	public List<Integer> gpsLinkSeqList;

	/*
	 * 用于 通过gpsLinkSeq得到经纬度信息 Map <Integer对应一个gpsLinkSeq, LongiLatitude
	 * 对应一个经纬度对象>
	 */
	public Map<Integer,LongiLatitude> longiLatMap;

	/*
	 * 用于存放每一个表对应的Sqlldr相关的文件 <String 对应一个表名, SqlldrForHwDtImpl 一个SQLLDR处理对象>
	 */
	Map<String,SqlldrForHwDtImpl> sqlldrMap;

	/*
	 * 用于存放每一个表，每一行的数据状态 <Integer 行号, PerLineState 每一行的关键信息对象>
	 */
	Map<String,Map<Integer,PerLineState>> plsMap;

	// EVENT_INDEX 的（序列自增）
	private int eventIndex = 0;

	// 问题事件个数
	private int eventProblemNumber = 0;

	/**
	 * 用于缓存CPM表每一个字段的缓存信息<br>
	 * Map<Integer 时间,String [] 整组数据><br>
	 * CLT_DT_CPM一组全部字段均无效才继承。
	 */
	Map<Integer,String []> cpmCacher;

	/**
	 * 用于缓存CPM原始记录缓存信息<br>
	 * Map<String 时间,String [] 整组数据><br>
	 */
	Map<String,String []> cdmDataCacher;

	/**
	 * 用于缓存CSC原始记录缓存信息<br>
	 * Map<String 时间,String [] 整组数据><br>
	 */
	Map<String,String []> cscDataCacher;

	/**
	 * 用于缓存CSC表每一个字段的缓存信息<br>
	 * Map<Integer 时间,String 数据值><br>
	 * CLT_DT_CSC以每个字段的数据进行继承
	 */
	Map<Integer,String> cscFieldCacher;

	/**
	 * 异常事件代码
	 */
	List<String> evtProblemCodes;

	/**
	 * 保存所有的表信息
	 */
	Map<String,Table> tables;

	/**
	 * 用于验证同一时间点的相同数据<br>
	 * Map<String 表名,Map<String 时间字符串,Short 用1表示存在>>
	 */
	Map<String,Map<String,Short>> validDatas;

	// 用于保存纯文件的名称 如：DL10000000_04_20130815122436_C_CQ_MO
	private String pureFileName;

	public String getPureFileName(){
		return pureFileName;
	}

	public void setPureFileName(String pureFileName){
		this.pureFileName = pureFileName;
	}
	
	// 文件首行字符串，放在最后入库，入库前替换掉vendor字段
	private String[] firstLineArray;
	// 处理csc表时可以关联到
	private String vendor = "";

	/**
	 * 初始化
	 */
	private void init(DtCdmaTempletOldD.Templet templetD){
		gpsLinkSeqList = new ArrayList<Integer>();
		longiLatMap = new HashMap<Integer,LongiLatitude>();
		sqlldrMap = new HashMap<String,SqlldrForHwDtImpl>();
		plsMap = new HashMap<String,Map<Integer,PerLineState>>();
		cpmCacher = new HashMap<Integer,String []>();
		cdmDataCacher = new HashMap<String,String []>();
		cscDataCacher = new HashMap<String,String []>();
		cscFieldCacher = new HashMap<Integer,String>();
		evtProblemCodes = new ArrayList<String>();
		initTables(templetD);
		validDatas = new HashMap<String,Map<String,Short>>();
		String codesStr = SystemConfig.getInstance().getHwdtCdmaProblemCodes();
		String [] codes = CdmaUtil.split(codesStr, ",");
		for(int i = 0; i < codes.length; i++){
			evtProblemCodes.add(codes[i]);
		}
		eventIndex = 0;
		eventProblemNumber = 0;
	}

	@Override
	public boolean parseData() throws Exception{
		// 找出解析此文件需要依赖的Templet对象
		TempletBase tBaseD = this.collectObjInfo.getDistributeTemplet();
		if(!(tBaseD instanceof DtCdmaTempletOldD))
			return false;
		DtCdmaTempletOldD templetD = (DtCdmaTempletOldD)tBaseD;
		if(null == templetD.getTemplet()){
			LOGGER.error(this.getCollectObjInfo().getTaskID() + ": 解析失败,原因：找不到对应的分发模板." + this.fileName);
			return false;
		}
		setPureFileName(this.getFileName().substring(this.getFileName().lastIndexOf("\\") + 1,
				this.getFileName().lastIndexOf(".")));
		// 加载文件，使用BufferedReader 逐行读取
		BufferedReader br = null;
		long beginTime = System.currentTimeMillis();

		try{
			LOGGER.info("begin to parse hwDtCdma Data currentTimeMillis is:" + beginTime);
			// 初始化
			init(templetD.getTemplet());
			br = new BufferedReader(new FileReader(this.getFileName()));
			String strline = null;
			long numberLine = 0l;
			// 检查首行有效性
			if(!checkFirstLine(br.readLine())){
				throw new ParseException("不支持的路测文件，fileName:"+this.getFileName());
			}
			numberLine++;
			
			// 逐行解析数据，并写入对应的Sqlldr入库文件中
			while((strline = br.readLine()) != null){
				handleALineData(strline, templetD.getTemplet());
				numberLine++;
				if(numberLine % 10000 == 0)
					LOGGER.info("parsing hwDtCdma Data current line number is:" + numberLine);
			}
			// 数据继承需先全部缓存再写数据
			handleInheritData(templetD.getTemplet());
			LOGGER.info("parsed hwDtCdma Data finished current line number is:" + numberLine);
			LOGGER.info("parsed hwDtCdma Data finished expend time:" + (System.currentTimeMillis() - beginTime));
			// 追加固定字段的值,以及一些关联字段、网无信息etc。 追加完一个文件就执行入库
			appendFixValue();
			dealFirstLine(templetD.getTemplet());
		}catch(Exception e){
			LOGGER.error("华为 CDMA 路测数据的解析 出现错误:" + e);
			e.printStackTrace();
		}finally{
			System.out.println("close");
			if(null != br)
				try{
					br.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			// 清理缓存数据
			clear();
		}
		LOGGER.info("expend time:" + (System.currentTimeMillis() - beginTime));
		return true;
	}
	
	/**
	 * 检查首行有效性
	 * 当且仅当以“FileHeader”开头，而且网络制式是cdma时才返回true
	 * @throws ParseException
	 */
	private Boolean checkFirstLine(String firstLine){
		if((null == firstLine)||("".equals(firstLine))){
			return false;
		}
		firstLineArray = CdmaUtil.split(firstLine, DT_SPLIT);
		// 邓博文拍板：如果不是cdma的数据，抛弃不处理
		if((!firstLineArray[0].equalsIgnoreCase("FileHeader"))||(!firstLineArray[4].equalsIgnoreCase("CDMA"))){
			return false;
		}
		return true;
	}
	
	private void dealFirstLine(DtCdmaTempletOldD.Templet templetD) throws IOException, SQLException{
		firstLineArray[2] = vendor;
		// 解析
		parseALineData(firstLineArray, templetD);
		// 追加固定字段
		SqlldrForHwDtImpl sqlldr = hanldeFixColumns("CLT_DT_FILE");
		// 入库
		doSqlldr(sqlldr);
	}

	/**
	 * 清理缓存数据
	 */
	private void clear(){
		time = null;
		startime = null;
		dateStr = null;
		gpsLinkSeqList.clear();
		longiLatMap.clear();
		sqlldrMap.clear();
		plsMap.clear();
		cpmCacher.clear();
		cdmDataCacher.clear();
		cscDataCacher.clear();
		cscFieldCacher.clear();
		evtProblemCodes.clear();
		tables.clear();
		validDatas.clear();
	}

	/**
	 * 逐行解析数据 解析每一行数据，并写入对应的Sqlldr入库文件中
	 * 
	 * @param strline
	 * @param templetP
	 */
	private void handleALineData(String strline, DtCdmaTempletOldD.Templet templetD) throws Exception{
		String [] strs = CdmaUtil.split(strline, DT_SPLIT);
		// 如果为空直接不处理
		if(null == strs[0])
			return;
		// 如果是FieldDefine 只是定义数据，做模板校验
		if(strs[0].equals("FieldDefine")){
			return;
		}
		// 如果是Date 是全局日期要保存起来
		if(strs[0].equals("DATE")){
			setDateStr(strs);
			return;
		}
		// 如果是Time是接下来每一个表的对应时间
		if(strs[0].equals("TIME")){
			setTime(strs);
			return;
		}
		// 如果Time出现在其它定义之后，这是不符合规则的 除了FileHeader之外
		if((time == null || time.equals("")) && !strs[0].equals("FileHeader"))
			return;
		// 如果是GPS,记录Time与经纬度 用于关联 GPS肯定出现在TIME之后
		if(strs[0].equals("GPS")){
			gpsLinkSeqList.add(Integer.parseInt(time));
			// 对每一个gpsLinkSeq都对应一个经纬度用于做关联
			LongiLatitude longila = new LongiLatitude();
			longila.setLongitude(Double.parseDouble(strs[1]));
			longila.setLatitude(Double.parseDouble(strs[2]));
			longiLatMap.put(Integer.parseInt(time), longila);
		}
		// 走下来的就是要处理的数据
		parseALineData(strs, templetD);
	}

	/**
	 * 解析一条数据，并且把数据写入文件
	 * 
	 * @param strs 拆分为数组的每一行数据
	 * @param templetD 分发模板
	 * @throws IOException
	 * @throws SQLException
	 */
	private void parseALineData(String [] strs, DtCdmaTempletOldD.Templet templetD) throws IOException, SQLException{
		Table table = tables.get(strs[0]);
		// 校验同一时间点该数据是否已存在，同一时间点只会有一条相同表的数据
		if(validIsExistAtSameTime(table))
			return;
		// CASF要特殊处理
		if(strs[0].equals("CASF")){
			specialHandleForNi(strs, table);
			return;
		}
		// CPM要特殊处理
		if(strs[0].equals("CPM")){
			specialHandleForCPM(strs, table);
			return;
		}
		// EVT另外一种的特殊处理
		if(strs[0].equals("EVT")){
			specialHandleForEvt(strs, table);
			return;
		}
		// CDM要特殊处理
		if(strs[0].equals("CDM")){
			// 先只缓存CDM 数据
			this.cdmDataCacher.put(this.time, strs);
			return;
		}
		// CSC要特殊处理
		if(strs[0].equals("CSC")){
			// 先只缓存CSC 数据
			this.cscDataCacher.put(this.time, strs);
			return;
		}
		writePerLine(strs, table);
		return;
	}

	private void initTables(DtCdmaTempletOldD.Templet templetD){
		tables = new HashMap<String,Table>();
		Map<Integer,Table> dtMap = templetD.getTables();
		Iterator<Integer> iteratorDs = dtMap.keySet().iterator();
		// 每一个表都必须要有对应的分发模板，所有直接遍历分发模板，确定要入库的字段信息
		while(iteratorDs.hasNext()){
			Table table = dtMap.get(iteratorDs.next());
			String name = table.getName().substring(TABLE_PREFIX.length());
			// CTL_DT_FILE 表的没有按规则 在每一行的一个字符前加 CLT_DT_ 做表名 
			if(name.equals("FILE")){
				tables.put("FileHeader", table);
			}else{
				tables.put(name, table);
			}
		}
	}

	/**
	 * 处理如CSC、CDM 需要继承的单字段数据。 <BR>
	 * 因为时间是无序的，如果按序解码，会出现继承不到数据。<BR>
	 * 所以数据都是先缓存，后排序
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	private void handleInheritData(DtCdmaTempletOldD.Templet templetD) throws IOException, SQLException{
		Map<Integer,Table> dtMap = templetD.getTables();
		// 处理CDM表
		dealInheritData(cdmDataCacher, dtMap.get(10));
		// 处理CSC表
		dealInheritData(cscDataCacher, dtMap.get(4));
	}

	/**
	 * 处理如CSC、CDM 需要继承的单字段数据。
	 */
	private void dealInheritData(Map<String,String []> cacher, Table table) throws IOException, SQLException{
		Map<Integer,String> fieldCacher = new HashMap<Integer,String>();
		List<String> columnTimes = new ArrayList<String>(cacher.keySet());
		// 先排序
		Collections.sort(columnTimes, CdmaUtil.getComparatorStrTime());
		for(String index : columnTimes){
			String [] strs = cacher.get(index);
			// 每次的时间也要变
			this.time = String.valueOf(index);
			specialHandleForInheritFields(strs, table, fieldCacher);
		}
		fieldCacher.clear();
	}

	/**
	 * 特别处理如:CDM,CSC等数据为空，需按字段继承的数据
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	private void specialHandleForInheritFields(String [] strs, Table table, Map<Integer,String> fieldCacher)
			throws IOException, SQLException{
		// 写数据前，先缓存,放后会增加缓存
		// 定义最后一个记录是否继承的字段
		StringBuilder inheritFlag = new StringBuilder();
		for(int i = 1; i < strs.length; i++){
			if(strs[i] == null || strs[i].trim().equals("")){
				String value = fieldCacher.get(i);
				if(value == null || value.trim().equals("")){
					inheritFlag.append("0,");
				}else{
					strs[i] = value;
					inheritFlag.append("1,");
				}
			}else{
				// 缓存，存在的数据
				fieldCacher.put(i, strs[i]);
				inheritFlag.append("0,");
			}
		}
		int len = strs.length;
		// 给数组添加一个字段
		strs = Arrays.copyOf(strs, len + 1);
		strs[len] = inheritFlag.substring(0, inheritFlag.length() - 1);
		writePerLine(strs, table);
	}

	/**
	 * 特别处理如:Evt,EVENT_INDEX 文件名+事件代码+0001（序列增加）<br>
	 * 2014-07-23 要求修改为 从 1 开始累加
	 * 
	 * @param strs
	 * @param define
	 * @throws SQLException
	 * @throws IOException
	 */
	private void specialHandleForEvt(String [] strs, Table table) throws IOException, SQLException{
		String eventIndexStr = String.valueOf(++ eventIndex);
		// 事件代码
		String eventCode = strs[1];
		String eventFlag = "0";
		if(evtProblemCodes.contains(eventCode)){
			eventFlag = "1";
			eventProblemNumber++;
		}
		int len = strs.length;
		strs = Arrays.copyOf(strs, len + 2);
		strs[len] = eventIndexStr;
		strs[len + 1] = eventFlag;
		writePerLine(strs, table);
	}

	/**
	 * 特别处理如:CPM,CASF 的数据要根据第一个Ni_Count来判断要拆分为多少条数据
	 * 
	 * @param strs
	 * @param define
	 * @throws SQLException
	 * @throws IOException
	 */
	private void specialHandleForNi(String [] strs, Table table) throws IOException, SQLException{
		// 这种类型的数据最少需要有三个字段，并且第二个字段必须为大于0的数字
		if(strs.length < 3 || strs[1] == null || "".equals(strs[1])){
			writePerLine(strs, table);
			return;
		}
		int count = Integer.parseInt(strs[1]);
		if(count <= 0){
			writePerLine(strs, table);
			return;
		}
		// 定义一个二维数组，存入拆分的数据
		String [][] strss = new String[count][strs.length + 1];
		// 要循环的字段数 +1 是为人追加一个计数
		for(int j = 0; j < strs.length + 1; j++){
			String [] items = null;
			if(j != 0 && j != 1 && j != strs.length){
				items = CdmaUtil.split(strs[j], " ");
			}
			// 通过 Ni_Count(对应第二字符串) 得到要分拆的对象数
			for(int i = 0; i < count; i++){
				if(j == 0 || j == 1){
					strss[i][j] = strs[j];
					continue;
				}
				if(j != strs.length){
					if(items.length == count && items[i] != null)
						strss[i][j] = items[i];
					else
						strss[i][j] = "";
				}else
					strss[i][j] = String.valueOf(i + 1);
			}
		}
		// 把每一行数据写入文件中
		for(int i = 0; i < strss.length; i++){
			String [] items = strss[i];
			writePerLine(items, table);
		}
	}

	/**
	 * 特别处理如:CPM,数据为空，需按组继承<br>
	 * 只要有一个字段有数据，就无需继承
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	private void specialHandleForCPM(String [] strs, Table table) throws IOException, SQLException{
		// 写数据前，先缓存,放后会增加缓存
		// 定义最后一个记录是否继承的字段
		StringBuilder inherit = new StringBuilder();
		boolean isInherit = true;
		String inheritFlag = "1";
		for(int i = 1; i < strs.length; i++){
			// 只要有一个字段有数据，就无需继承
			if(strs[i] != null || !strs[i].trim().equals("")){
				isInherit = false;
				break;
			}
		}
		if(isInherit){
			// 从CPM 缓存中取最近的数据
			String [] rs = getCpmValues();
			if(rs != null)
				strs = rs;
		}else{
			// 缓存，有效的数据
			cpmCacher.put(Integer.parseInt(this.time), strs);
			inheritFlag = "0";
		}
		int len = strs.length;
		for(int i = 1; i < len; i++){
			inherit.append(inheritFlag).append(",");
		}
		int count = 1;
		if(strs[1] != null && !strs[1].trim().equals("")){
			count = Integer.parseInt(strs[1]);
		}
		String inheritStr = inherit.toString().substring(0, inherit.toString().length() - 1);
		StringBuilder inherits = new StringBuilder();
		for(int i = 0; i < count; i++){
			if(i == (count - 1)){
				inherits.append(inheritStr);
			}else{
				inherits.append(inheritStr).append(" ");
			}
		}
		// 给数组添加一个字段
		strs = Arrays.copyOf(strs, len + 1);
		strs[len] = inherits.toString();
		specialHandleForNi(strs, table);
	}

	/**
	 * 从CPM 缓存中取最近的数据
	 */
	private String [] getCpmValues(){
		if(null == cpmCacher || cpmCacher.size() == 0){
			return null;
		}
		List<Integer> columnTimes = new ArrayList<Integer>(cpmCacher.keySet());
		// 先排序
		Collections.sort(columnTimes, CdmaUtil.getComparatorGpsLinkSeq());
		for(int len = columnTimes.size(), i = len - 1; i >= 0; i--){
			Integer ti = columnTimes.get(i);
			// 如果本次时间比缓存的最后一个时间还小，则代表为当前最父级的数据
			if(Integer.parseInt(this.time) < ti){
				return null;
			}
			if(cpmCacher.get(ti) != null){
				return cpmCacher.get(ti);
			}
		}
		return null;
	}

	/**
	 * 校验同一时间点该数据是否已存在，同一时间点只会有一条相同表的数据
	 * 
	 * @param define 要校验的定义数据 也就是一条记录
	 * @return false 为不存在可以入库, true 代表已存在不需要入库。
	 */
	private boolean validIsExistAtSameTime(Table table){
		// 先取出对应的表的所有读取标示
		Map<String,Short> hasData = validDatas.get(table.getName());
		if(null == hasData || null == hasData.get(this.time) || hasData.get(this.time) != 1){
			return false;
		}else{
			return true;
		}
	}

	/**
	 * 将每一行数据逐行写入到一个相应的表文件名命名的文件中。
	 * 
	 * @param strs 每一行数据
	 * @param table 数据对应的表结构
	 * @param path 要与入的文件的路径
	 * @throws IOException
	 * @throws SQLException
	 */
	private void writePerLine(String [] strs, Table table) throws IOException, SQLException{
		// 从缓存中取出对应表，记录每一行的长度，用于追加数据时定位
		Map<Integer,PerLineState> plsm = plsMap.get(table.getName());
		// 对应的表，然后对读取的每一行按时间加标示
		Map<String,Short> hasData = validDatas.get(table.getName());
		// 是否为第1行数据
		boolean isfirstline = false;
		if(null == plsm){
			isfirstline = true;
			plsm = new HashMap<Integer,PerLineState>();
			plsMap.put(table.getName(), plsm);
		}
		if(null == hasData){
			hasData = new HashMap<String,Short>();
			validDatas.put(table.getName(), hasData);
		}
		// 存一标示此时此刻此表有数据
		hasData.put(this.time, (short)1);
		// 从缓存中取出对应的Sqlldr信息
		SqlldrForHwDtImpl sqlldr = sqlldrMap.get(table.getName());
		if(sqlldr == null){
			sqlldr = new SqlldrForHwDtImpl(this.collectObjInfo.getTaskID(),
					this.collectObjInfo.getDevInfo().getOmcID(), this.collectObjInfo.getLastCollectTime(), "HwDtCdma");
			sqlldrMap.put(table.getName(), sqlldr);
			// 第一次写入数据时，先把表字段定义先写好
			writeTableHead(table, sqlldr, plsm, strs);
		}
		StringBuffer sbf = new StringBuffer();
		for(int i = 1; i < strs.length; i++){
			sbf.append(strs[i]);
			sbf.append(SQLLDER_SEPARATOR);
		}
		// 预留空间写数据
		sbf.append(sqlldr.getSpaceStr());
		RandomAccessFile rf = sqlldr.getInfo().getRafForTxt();
		// 每一行数据预先写入Sqlldr文件中
		rf.write(sbf.append("\n").toString().getBytes());
		// 记录当前每一行的长度
		if(!isfirstline){
			PerLineState pls = new PerLineState();
			pls.setLength(sbf.toString().length());
			pls.setTime(time);
			recodePldForSpecial(strs, pls, table);
			plsm.put(plsm.size() + 1, pls);
		}
	}

	/**
	 * 一些特殊表要做回填的，先把用于回填的信息缓存起来
	 * 
	 * @param strs 原始文件中每一行数据
	 * @param pls 每一行的状态实现类
	 * @param table 分发模板中对应的一个表
	 */
	private void recodePldForSpecial(String [] strs, PerLineState pls, Table table){
		if(table.getName().equals("CLT_DT_CPM")){
			if(null != strs[2] && !strs[2].equals(""))
				pls.setPn(Integer.parseInt(strs[2]));
		}else if(table.getName().equals("CLT_DT_CSC")){
			if(null != strs[3] && !strs[3].equals(""))
				pls.setSid(Integer.parseInt(strs[3]));
			if(null != strs[4] && !strs[4].equals(""))
				pls.setNid(Integer.parseInt(strs[4]));
			if(null != strs[7] && !strs[7].equals(""))
				pls.setCarr(Integer.parseInt(strs[7]));
			if(null != strs[8] && !strs[8].equals(""))
				pls.setPn(Integer.parseInt(strs[8]));
		}
	}

	/**
	 * 写Sqlldr相关的头文件，以及第一行字段信息
	 * 
	 * @param table 表结构
	 * @param rf RandomAccessFile 文件随机防问流
	 * @return 返回每一行要预留的空间空格字符串
	 */
	private void writeTableHead(Table table, SqlldrForHwDtImpl sqlldr, Map<Integer,PerLineState> plsm, String [] strs){
		// 重组数据 转成Map<String, List<String>> tableCols
		Map<String,List<String>> tableCols = new HashMap<String,List<String>>();
		List<String> colmunList = new ArrayList<String>();
		// 找出字段名
		Map<Integer,Column> columnsMap = table.getColumns();
		Iterator<Integer> iterateCol = columnsMap.keySet().iterator();
		while(iterateCol.hasNext()){
			colmunList.add(columnsMap.get(iterateCol.next()).getName());
		}
		// 设置固定字段 的字段定义 并取得要预留的空间空格字符串
		String spaceStr = setFixFieldColumns(table.getName(), colmunList);
		sqlldr.setSpaceStr(spaceStr);
		tableCols.put(table.getName(), colmunList);
		sqlldr.setTableCols(tableCols);
		// 预处理Sqlldr
		int firstLength = sqlldr.initSqlldr();
		// 这里记录第一行的长度
		for(int i = 1; i < strs.length; i++){
			firstLength += strs[i].length() + 1;
		}
		// 记录当前每一行的长度
		PerLineState pls = new PerLineState();
		pls.setLength(firstLength);
		pls.setTime(time);
		recodePldForSpecial(strs, pls, table);
		plsm.put(plsm.size() + 1, pls);
	}

	/**
	 * 追加固定字段的值
	 * 
	 * @throws IOException
	 * @throws SQLException
	 */
	private void appendFixValue() throws IOException, SQLException{
		// 把用于关gpsLinkSeqList 先排序
		Collections.sort(gpsLinkSeqList, CdmaUtil.getComparatorGpsLinkSeq());
		// 遍历所有待处理的数据
		Iterator<String> sqlldrIterate = sqlldrMap.keySet().iterator();
		while(sqlldrIterate.hasNext()){
			String tableName = sqlldrIterate.next();
			// 对每一个文件追加回定字段信息处理
			SqlldrForHwDtImpl sqlldr = hanldeFixColumns(tableName);
			doSqlldr(sqlldr);
		}
	}

	/**
	 * 入库
	 * 
	 * @param table
	 * @throws SQLException
	 */
	private void doSqlldr(SqlldrForHwDtImpl sqlldr) throws SQLException{
		// 开始执行入库
		LOGGER.debug(this.collectObjInfo.getTaskID() + " - 开始执行入库 " + this.collectObjInfo.getLastCollectTime());
		sqlldr.runSqlldr();
	}

	/**
	 * 对每一个文件追加固定字段信息处理
	 * 
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	private SqlldrForHwDtImpl hanldeFixColumns(String tableName) throws IOException{
		SqlldrForHwDtImpl sqlldr = sqlldrMap.get(tableName);
		RandomAccessFile rf = sqlldr.getInfo().getRafForTxt();
		Map<Integer,PerLineState> plsm = plsMap.get(tableName);
		// 累积行长度
		int length = 0;
		// 因为plsm Map的Key是从1开始，所以是i=1,且 i <= size
		int size = plsm.size();
		// 追加数据 一个长度对应一行
		for(int i = 1; i <= size; i++){
			PerLineState pls = plsm.get(i);
			length += pls.getLength();
			String str = getFixFieldValues(tableName, pls);
			rf.seek(length);
			rf.write(str.getBytes());
		}
		rf.close();
		return sqlldr;
	}

	/**
	 * 设置固定字段的值
	 * 
	 * @param defineHeader
	 * @param colmunList
	 */
	private String getFixFieldValues(String tableName, PerLineState pls){
		StringBuffer sb = new StringBuffer();
		// 采集时间为当前时间
		String collectTime = util.Util.getDateString(new Date());
		String fileName = getPureFileName();
		String startTime = CdmaUtil.getFixDateTime(String.valueOf(pls.getTime()), dateStr);
		String milliSecond = null;
		if(String.valueOf(pls.getTime()).length() <= 6)
			milliSecond = "000";
		else
			milliSecond = String.valueOf(pls.getTime()).substring(6);
		if(tableName.equals("CLT_DT_FILE")){
			startTime = CdmaUtil.getFixDateTime(String.valueOf(startime), dateStr);
			String endTime = CdmaUtil.getFixDateTime(String.valueOf(endtime), dateStr);
			String deviceCity = fileName.substring(3, 6);
			String deviceNumber = fileName.substring(6, 10);
			String coverType = CdmaUtil.getCoverType(fileName);
			String serviceTypeSv = CdmaUtil.getServiceTypeSV(fileName);
			String fileVendor = CdmaUtil.getFileVendor(fileName);
			String fileDeviceType = CdmaUtil.getFileDeviceType(fileName);
			String fileNetworkType = CdmaUtil.getFileNetWorkType(fileName);
			String evtProblemNumber = String.valueOf(eventProblemNumber);
			sb.append(collectTime).append(SQLLDER_SEPARATOR).append(startTime).append(SQLLDER_SEPARATOR)
					.append(endTime).append(SQLLDER_SEPARATOR).append(fileName).append(SQLLDER_SEPARATOR)
					.append(deviceCity).append(SQLLDER_SEPARATOR).append(deviceNumber).append(SQLLDER_SEPARATOR)
					.append(coverType).append(SQLLDER_SEPARATOR).append(serviceTypeSv).append(SQLLDER_SEPARATOR)
					.append(fileVendor).append(SQLLDER_SEPARATOR).append(fileDeviceType).append(SQLLDER_SEPARATOR)
					.append(fileNetworkType).append(SQLLDER_SEPARATOR).append(evtProblemNumber)
					.append(SQLLDER_SEPARATOR);
		}else if(CdmaUtil.isThreeFieldTables(tableName)){
			sb.append(startTime).append(SQLLDER_SEPARATOR).append(milliSecond).append(SQLLDER_SEPARATOR)
					.append(fileName).append(SQLLDER_SEPARATOR);
		}else{
			Integer gpsLink = getGpsLindSeqByTime(Integer.parseInt(pls.getTime()));
			sb.append(collectTime).append(SQLLDER_SEPARATOR).append(startTime).append(SQLLDER_SEPARATOR)
					.append(milliSecond).append(SQLLDER_SEPARATOR).append(fileName).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(gpsLink), 9)).append(SQLLDER_SEPARATOR);
			if(tableName.equals("CLT_DT_CSC")){
				// 如果是CSC，要从缓存的网元数据里面，按照SID、NID、PN、载频相同且扇区经纬度与接口文件中测试经纬度最近原则进行关联。
				// 关联出小区CI和小区中文名等字段
				setCscValues(sb, pls, gpsLink);
			}else if(tableName.equals("CLT_DT_CPM")){
				// 如果是CPM，要从缓存的网元数据里面，按照PN且扇区经纬度与接口文件中测试经纬度最近原则进行关联。
				// 关联出小区CI和小区中文名等字段
				setCpmValues(sb, pls, gpsLink);
			}
		}
		return sb.toString();
	}

	private void setCpmValues(StringBuffer sb, PerLineState pls, Integer gpsLink){
		int pn = pls.getPn();
		LongiLatitude longilat = longiLatMap.get(gpsLink);
		LoadConfMapDevToNe.Sector sector = null;
		// 从缓存中得到网元信息
		if(pn != 0 && longilat != null)
			sector = LoadConfMapDevToNe.getSectorByPn(pn, longilat.getLongitude(), longilat.getLatitude());
		if(null != sector){
			sb.append(CdmaUtil.supplementLength(String.valueOf(sector.getCell()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getCarr()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getCellName()), CELL_NAME_LENGTH))
					.append(SQLLDER_SEPARATOR);
		}else{
			sb.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(CELL_NAME_LENGTH)).append(SQLLDER_SEPARATOR);
		}
	}

	private void setCscValues(StringBuffer sb, PerLineState pls, Integer gpsLink){
		// 从缓存中得到网元信息
		String key = new StringBuffer().append(pls.getPn()).append(pls.getCarr()).toString();
		LongiLatitude longilat = longiLatMap.get(gpsLink);
		LoadConfMapDevToNe.Sector sector = null;
		if(!key.equals("") && longilat != null)
			sector = LoadConfMapDevToNe.getSectorByKey(key, longilat.getLongitude(), longilat.getLatitude());
		if(null != sector){
			vendor = sector.getVendor();
			sb.append(CdmaUtil.supplementLength(String.valueOf(sector.getNeBscId()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getNeBtsId()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getNeCellId()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getNeCarrId()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getBsc()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getBts()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getCell()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getCarr()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getCellName()), CELL_NAME_LENGTH))
					.append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.supplementLength(String.valueOf(sector.getBscName()), NI_CELL_LENGTH))
					.append(SQLLDER_SEPARATOR);
		}else{
			// 每一个字段如果为空也要补空格与“|”，否则sqlldr会报错
			sb.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(CELL_NAME_LENGTH)).append(SQLLDER_SEPARATOR)
					.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH)).append(SQLLDER_SEPARATOR);
		}
	}

	/**
	 * 设置固定字段 的字段定义
	 * 
	 * @param defineHeader
	 * @param colmunList
	 */
	private String setFixFieldColumns(String defineHeader, List<String> colmunList){
		// 用于记录每一行前面要预留的空间
		StringBuffer sb = new StringBuffer();
		if(defineHeader.equals("CLT_DT_FILE")){
			colmunList.add("COLLECT_TIME"); // 19+1
			colmunList.add("START_TIME"); // 19+1
			colmunList.add("END_TIME"); // 19+1
			colmunList.add("FILE_NAME");
			colmunList.add("DEVICE_CITY"); // 3+1
			colmunList.add("DEVICE_NUMBER"); // 4+1
			colmunList.add("COVER_TYPE"); // 2+1
			colmunList.add("SERVICETYPE_SV");
			colmunList.add("FILE_VENDOR"); // 2+1
			colmunList.add("FILE_DEVICETYPE"); // 1+1
			colmunList.add("FILE_NETWORKTYPE"); // 1+1
			colmunList.add("PROBLEM_EVT_NUM"); // 8+1
			sb.append(CdmaUtil.getSpaceStr(88 + getPureFileName().length()
					+ CdmaUtil.getServiceTypeSV(getPureFileName()).length() + 1));
		}else if(CdmaUtil.isThreeFieldTables(defineHeader)){
			colmunList.add("START_TIME");
			colmunList.add("MILLISECOND");
			colmunList.add("FILE_NAME");
			sb.append(CdmaUtil.getSpaceStr(25 + getPureFileName().length()));
		}else{
			colmunList.add("COLLECT_TIME");
			colmunList.add("START_TIME");
			colmunList.add("MILLISECOND");
			colmunList.add("FILE_NAME");
			colmunList.add("GPS_LINK_SEQ");
			sb.append(CdmaUtil.getSpaceStr(55 + getPureFileName().length()));
			if(defineHeader.equals("CLT_DT_CSC")){
				colmunList.add("NE_BSC_ID");
				colmunList.add("NE_BTS_ID");
				colmunList.add("NE_CELL_ID");
				colmunList.add("NE_CARR_ID");
				colmunList.add("BSC_ID");
				colmunList.add("BTS_ID");
				colmunList.add("CELL_ID");
				colmunList.add("CARR");
				colmunList.add("CELL_NAME");
				colmunList.add("BSC_NAME");
				sb.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH * 9 + CELL_NAME_LENGTH + 10));
			}else if(defineHeader.equals("CLT_DT_CPM")){
				colmunList.add("NI_NE_CELL_ID");
				colmunList.add("NI_NE_CARR_ID");
				colmunList.add("NI_CELL_NAME");
				sb.append(CdmaUtil.getSpaceStr(NI_CELL_LENGTH * 2 + CELL_NAME_LENGTH + 3));
			}
		}
		return sb.toString();
	}

	/**
	 * 处理原始文件的Time值
	 * 
	 * @param strline
	 * @return
	 */
	private void setTime(String [] strs) throws Exception{
		StringBuffer bf = new StringBuffer();
		for(int i = 1; i < strs.length; i++){
			if(i != (strs.length - 1)){
				bf.append(CdmaUtil.supplyZero(strs[i], 2));
			}else{
				bf.append(CdmaUtil.supplyZero(strs[i], 3));
			}
		}
		time = bf.toString();
		// 第一个出现的时间作为文件开始时间
		if(startime == null){
			startime = bf.toString();
		}
		endtime = time;
	}

	/**
	 * 设置全局的日期 一个文件一个日期
	 * 
	 * @param strline
	 * @return yyyy-MM-dd
	 */
	private void setDateStr(String [] strs) throws Exception{
		StringBuffer bf = new StringBuffer();
		for(int i = 1; i < strs.length; i++){
			bf.append(strs[i]);
			if((i + 1) < strs.length)
				bf.append("-");
		}
		dateStr = bf.toString();
	}

	/**
	 * 通过获取gpsLinkSeqList 中与所转时间相比最小的一个，做为 gpsLinkSeq ID
	 * 
	 * @param time
	 * @return
	 */
	private Integer getGpsLindSeqByTime(int time){
		if(gpsLinkSeqList.size() <= 0)
			return 0;
		int rsint = 0;
		for(Integer i : gpsLinkSeqList){
			rsint = i;
			if(time <= i)
				return i;
		}
		return rsint;
	}
}
