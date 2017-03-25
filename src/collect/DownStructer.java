package collect;

import java.util.ArrayList;
import java.util.List;

public class DownStructer {

	private List<String> suc;

	private List<String> fail;

	private List<String> localFail;

	public DownStructer() {
		suc = new ArrayList<String>();
		fail = new ArrayList<String>();
		localFail = new ArrayList<String>();
	}

	public List<String> getSuc() {
		return suc;
	}

	public void setSuc(List<String> suc) {
		this.suc = suc;
	}

	public List<String> getFail() {
		return fail;
	}

	public void setFail(List<String> fail) {
		this.fail = fail;
	}

	public List<String> getLocalFail() {
		return localFail;
	}

	public void setLocalFail(List<String> localFail) {
		this.localFail = localFail;
	}

}
