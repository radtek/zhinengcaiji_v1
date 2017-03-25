package templet;

/**
 * 空模板，什么都不做
 * 
 * @author sunxg
 * @since 1.0
 */
public class EmptyTempletP implements TempletBase {

	public void buildTmp(int tmpID) {
		// do nothing
	}

	public void parseTemp(String TempContent) {
		// do nothing
	}

	@Override
	public void buildTmp(TempletRecord record) {
		// do nothing
	}

}
