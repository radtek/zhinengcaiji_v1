package tools.templet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 模板生成 抽象类 AbstractTempletMaker
 * 
 * @author litp
 * @since 1.0
 */
public abstract class AbstractTempletMaker implements TempletMaker {

	protected static final String PARSE_FILE_NAME_SUFFIX = "_parse.xml";

	protected static final String DIST_FILE_NAME_SUFFIX = "_dist.xml";

	protected static final String TB_FILE_NAME_SUFFIX = "_createTable.sql";

	// 所有的表的信息map<原表名称,<所有的列信息>>
	protected Map<String, List<ColumnInfo>> tables = null;

	public AbstractTempletMaker() {
		tables = new LinkedHashMap<String, List<ColumnInfo>>();
	}

	protected void put(String desttn, ColumnInfo t) {
		if (tables.containsKey(desttn)) {
			List<ColumnInfo> list = tables.get(desttn);
			list.add(t);
		} else {
			List<ColumnInfo> list = new ArrayList<ColumnInfo>();
			list.add(t);
			tables.put(desttn, list);
		}
	}

	/**
	 * 把内容写入文件
	 * 
	 * @param content
	 *            内容
	 * @param fileName
	 *            文件路径
	 * @throws IOException
	 */
	protected void makeFile(String content, String fileName) throws Exception {
		FileWriter fw = null;
		try {
			fw = new FileWriter(new File(fileName), false);
			fw.write(content);
			fw.flush();
			fw.close();
		} finally {
			if (fw != null)
				fw.close();
		}
	}

	@Override
	public void make(String refFileName, String oFileName) throws Exception {
		this.parse(refFileName);
		this.generate(oFileName);
	}

	/**
	 * 根据解析后的数据生成模板和建表语句
	 * 
	 * @throws IOException
	 */
	protected abstract void generate(String oFileName) throws Exception;

	/**
	 * 解析参考文件
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	protected void parse(String fileName) throws Exception {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File(fileName)));
			String line = null;
			br.readLine(); // 跳过首行(标题行)
			while ((line = br.readLine()) != null) {
				if (line.trim().equals(""))
					continue;
				String[] datas = line.split(",");
				String colName = datas[3].trim(); // 采集字段名
				// 系统字段，不予以处理
				if (colName.equalsIgnoreCase("OMCID") || colName.equalsIgnoreCase("COLLECTTIME") || colName.equalsIgnoreCase("STAMPTIME"))
					continue;

				// 厂家文件名称;采集表名
				String keyName = datas[0] + ";" + datas[2];
				ColumnInfo t = new ColumnInfo();
				t.setDestColumn(colName);
				if (datas[4] == null || datas[4].trim().length() == 0)
					throw new Exception("参考文件配置有误,原因:有一单元格为空,请检查参考文件的配置.");
				t.setDestType(datas[4].trim());
				t.setSrcColumn(datas[1].trim());
				if (datas.length >= 6)
					t.setLength(datas[5].trim());
				if (datas.length >= 7)
					t.setAllowNull(datas[6].trim());
				if (datas.length >= 8)
					t.setPrimaryKeyFlag(datas[7]);
				put(keyName, t);
			}
		} finally {
			if (br != null)
				br.close();
		}
	}

}
