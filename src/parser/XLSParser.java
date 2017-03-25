package parser;

import java.io.File;

import jxl.Sheet;
import jxl.Workbook;
import task.CollectObjInfo;
import templet.XLSTempletP;

public class XLSParser extends Parser {

	public XLSParser() {
	}

	public XLSParser(CollectObjInfo collectInfo) {
		super(collectInfo);
	}

	@Override
	public boolean parseData() throws Exception {
		// 读取要解析的excel文件
		Workbook book = Workbook.getWorkbook(new File(this.fileName));
		// 取得解析模板
		XLSTempletP templet = (XLSTempletP) collectObjInfo.getParseTemplet();
		for (int i = 0; i < templet.m_mapSheet.size(); ++i) {
			XLSTempletP.SheetInfo shtInfo = templet.m_mapSheet.get(Integer.valueOf(i));
			Sheet sheet = book.getSheet(shtInfo.m_strSheetName);

			// 逐行处理
			int r = 0;
			if (shtInfo.m_bHasTitle)
				r = 1;
			for (; r < sheet.getRows(); ++r) {
				StringBuffer strLine = new StringBuffer();
				for (int c = 0; c < sheet.getColumns(); ++c) {
					// 行号跟列号是反的，注意！！
					strLine.append(sheet.getCell(c, r).getContents().trim());
					strLine.append(";");
				}
				strLine.append("\n");
				distribute.DistributeData(strLine.toString().getBytes(), i);
			}
		}
		return true;
	}
}
