package distributor;

import task.CollectObjInfo;

public class SmartLucTelnetDistributor extends Distribute {

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		super.commit();
	}

	@Override
	public void Distribute_Insert(byte[] bData, int tableIndex) {
		// TODO Auto-generated method stub
		super.Distribute_Insert(bData, tableIndex);
	}

	@Override
	public boolean Distribute_Sqlldr(byte[] bData, int tableIndex) {
		// TODO Auto-generated method stub
		return super.Distribute_Sqlldr(bData, tableIndex);
	}

	@Override
	public void distribute(Object wParam, Object lParam) throws Exception {
		// TODO Auto-generated method stub
		super.distribute(wParam, lParam);
	}

	@Override
	public boolean DistributeData(byte[] bData, int tableIndex) {
		// TODO Auto-generated method stub
		return super.DistributeData(bData, tableIndex);
	}

	@Override
	public CollectObjInfo getCollectInfo() {
		// TODO Auto-generated method stub
		return super.getCollectInfo();
	}

	@Override
	protected void init() {
		// TODO Auto-generated method stub
		super.init();
	}

	@Override
	public void init(CollectObjInfo TaskInfo) {
		// TODO Auto-generated method stub
		super.init(TaskInfo);
	}

	@Override
	public void setCollectInfo(CollectObjInfo collectInfo) {
		// TODO Auto-generated method stub
		super.setCollectInfo(collectInfo);
	}

}
