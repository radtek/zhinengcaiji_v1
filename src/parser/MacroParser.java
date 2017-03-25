package parser;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import templet.GenericSectionHeadP.Field;

/**
 * 宏解析，是{@link GenericSectionHeadParser}扩展用于,解析{@link Field}的macro属性。 目前，只支持文件名解析日期，但这可以扩展
 * 
 * 
 * @author liangww
 * @version 1.0
 * @create 2012-3-30 上午10:35:50
 */
public class MacroParser extends GenericSectionHeadParser {

	private final static String FILE_PARSE_MACRO = "@fileNamePattern_";

	@Override
	protected void beforDistribute(Collection<Field> cFields) {
		// TODO Auto-generated method stub
		Iterator<Field> itr = cFields.iterator();

		while (itr.hasNext()) {
			Field field = itr.next();
			String macro = field.getMacro();

			// 如果是文件
			if (macro != null && macro.toLowerCase().startsWith(FILE_PARSE_MACRO.toLowerCase())) {
				// Pattern pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{6}");
				Pattern pattern = Pattern.compile(macro.substring(FILE_PARSE_MACRO.length()));
				Matcher matcher = pattern.matcher(getFileName());
				if (matcher.find()) {
					field.setValue(matcher.group());
				}
			}// end of if(macro != null &&
				// macro.toLowerCase().startsWith(FILE_PARSE_MACRO))
		}// end of if while(itr.hasNext())
	}

	@Override
	public String getFileName() {
		// TODO Auto-generated method stub
		File file = new File(super.getFileName());

		return file.getName();
	}

}
