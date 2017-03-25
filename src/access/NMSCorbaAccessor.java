package access;

/**
 * 网管corba接口采集方式。
 * 
 * @author ChenSijiang 2012-6-8
 */
public class NMSCorbaAccessor extends AbstractAccessor {

	@Override
	public void configure() throws Exception {
		return;
	}

	@Override
	public boolean access() throws Exception {
		return parser.parseData();
	}

}
