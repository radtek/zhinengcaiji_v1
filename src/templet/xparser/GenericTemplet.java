package templet.xparser;

import templet.AbstractTempletBase;

/**
 * XParser解析模板
 * 
 * @author YangJian
 * @since 1.0
 */
public class GenericTemplet extends AbstractTempletBase {

	public String getTempLocation() {
		return this.tmpFileName;
	}

	@Override
	public void parseTemp(String TempContent) throws Exception {

	}

}
