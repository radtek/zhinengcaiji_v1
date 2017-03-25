package templet.lucent;

import org.dom4j.Element;

import formator.Formator;

/**
 * 域， 对应 索引号、源索引号和对应的格式化器
 * 
 * @author liangww
 * @version 1.0
 * @create 2012-7-14 下午04:11:19
 * @see Formator
 * @see FieldGroup
 * 
 */
public class Field {

	int index = 0;		//

	int formatType = 0;	// 格式化类型

	int srcIndex = 0;	// 源index

	/**
	 * 
	 * @param element
	 */
	public void build(Element element) {
		// <field index="1" srcIndex="1" formatType="1" />
		this.index = Integer.valueOf(element.attributeValue("index"));
		this.srcIndex = Integer.valueOf(element.attributeValue("srcIndex"));
		String value = element.attributeValue("formatType");
		if (value != null) {
			this.formatType = Integer.valueOf(value);
		}
	}

}
