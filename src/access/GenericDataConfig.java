package access;

import java.util.ArrayList;
import java.util.List;

import util.Util;

/**
 * 接入数据源关于数据源的描述 一般性类
 * <p>
 * 描述规则为: 文件或者SQL语句,当有多个文件或者SQL语句时使用;号间隔
 * </p>
 * 
 * @author YangJian
 * @since 3.0
 */
public class GenericDataConfig extends DataConfig {

	private String[] dataCfg;

	public GenericDataConfig() {
		super();
	}

	public String[] getDatas() {
		return dataCfg;
	}

	public void setDatas(String[] dataCfg) {
		this.dataCfg = dataCfg;
	}

	/**
	 * 根据传入的配置解析成对应的描述对象
	 * 
	 * @param strCfg
	 * @return
	 */
	public static GenericDataConfig wrap(String strCfg) {
		GenericDataConfig cfg = null;

		if (strCfg != null && strCfg.length() > 0) {
			String[] strFields = strCfg.split(";");

			List<String> tmp = new ArrayList<String>();
			for (String s : strFields) {
				if (Util.isNotNull(s)) {
					tmp.add(s.trim());
				}
			}

			cfg = new GenericDataConfig();
			cfg.setDatas(tmp.toArray(new String[0]));
		}
		return cfg;
	}

}
