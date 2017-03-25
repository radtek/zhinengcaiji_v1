package tools.templet;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * 数据库方式 模板生成 DatabaseMaker
 * 
 * @author litp
 * @since 1.0
 */
public class DatabaseMaker extends AbstractTempletMaker {

	public void generate(String oFileName) throws Exception {
		Set<String> sets = tables.keySet();
		// 建表语句
		StringBuilder createTbSQL = new StringBuilder();
		// 解析模板
		StringBuilder pTemp = new StringBuilder("<?xml version=\"1.0\" encoding=\"gb2312\" ?>\n");
		pTemp.append("<templets>\n");

		// 分发模板
		StringBuilder dTemp = new StringBuilder("<?xml version=\"1.0\" encoding=\"gb2312\" ?>\n");
		dTemp.append("<templets>\n");

		int j = 0;
		for (String names : sets) {
			String[] name = names.split(";");
			String srctn = name[0]; // 厂家的
			String desttn = name[1]; // 采集表

			// 表
			createTbSQL.append("create table ").append(desttn).append("\n(\n");
			createTbSQL.append("\tOMCID NUMBER,\n");
			createTbSQL.append("\tCOLLECTTIME DATE,\n");
			createTbSQL.append("\tSTAMPTIME DATE,\n");

			// 解析模板
			pTemp.append("\t").append("<templet id=\"" + j + "\" ");
			pTemp.append("table=\"" + srctn + "\"");
			pTemp.append(" used=\"1\" condition=\"\">\n");

			// 分发模板
			dTemp.append("\t").append("<templet id=\"" + j + "\">\n");
			dTemp.append("\t\t<table id=\"0\" ");
			dTemp.append("name=\"" + desttn + "\" split=\";\" >\n");

			List<ColumnInfo> list = tables.get(names);
			for (int i = 0; i < list.size(); i++) {
				ColumnInfo c = list.get(i);
				String destColumn = c.getDestColumn();
				String destType = c.getDestType().trim();
				String len = c.getLength();
				int iLen = -1;
				try {
					iLen = Integer.parseInt(len);
				} catch (Exception e) {
				}

				String primaryKey = "";
				if (c.getPrimaryKeyFlag().equalsIgnoreCase("Y"))
					primaryKey = " primary key";

				String allowNullStr = "";
				if (c.getAllowNull().equalsIgnoreCase("N"))
					allowNullStr = " not null";

				String lenStr = "";
				if (destType.equalsIgnoreCase("number") && iLen > 0)
					lenStr = "(" + len + ")";

				if (destType.toLowerCase().equalsIgnoreCase("varchar2") && iLen <= 0)
					lenStr = "(200)";
				else if (destType.toLowerCase().equalsIgnoreCase("varchar2") && iLen > 0)
					lenStr = "(" + len + ")";

				createTbSQL.append("\t" + destColumn).append(" " + destType);
				createTbSQL.append(lenStr).append(primaryKey).append(allowNullStr).append(",\n");

				pTemp.append("\t\t<field name=\"" + destColumn + "\" index=\"" + i + "\"/>\n");

				dTemp.append("\t\t\t<column name=\"" + destColumn + "\" index=\"" + i + "\" ");
				if (destType.equalsIgnoreCase("date"))
					dTemp.append("type=\"3\" format=\"YYYY-MM-DD HH24:MI:SS\" />\n");
				else if (destType.equalsIgnoreCase("varchar2") && iLen >= 255)
					dTemp.append("type=\"2\" format=\"" + iLen + "\" />\n");
				else
					dTemp.append("/>\n");
			}
			createTbSQL.deleteCharAt(createTbSQL.length() - 2).append(");\n\n");

			pTemp.append("\t</templet>\n\n");
			dTemp.append("\t\t</table>\n").append("\t</templet>\n\n");

			j++;
		}
		pTemp.append("</templets>");
		dTemp.append("</templets>");

		String prefPath = System.getProperty("user.dir") + File.separator + oFileName;

		String tbFilePath = prefPath + TB_FILE_NAME_SUFFIX;
		makeFile(createTbSQL.toString(), tbFilePath);
		System.out.println("建表语句文件(" + tbFilePath + ")已生成");

		String pFilePath = prefPath + PARSE_FILE_NAME_SUFFIX;
		makeFile(pTemp.toString(), pFilePath);
		System.out.println("解析模板文件(" + pFilePath + ")已生成");

		String dFilePath = prefPath + DIST_FILE_NAME_SUFFIX;
		makeFile(dTemp.toString(), dFilePath);
		System.out.println("分发模板文件(" + dFilePath + ")已生成");
	}

}
