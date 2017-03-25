package distributor;

import java.io.FileWriter;
import java.util.List;

public class TableItem {

	public int tableIndex = 0;

	public FileWriter fileWriter = null;

	public String fileName = "";

	public String sql = "";

	public int recordCounts = 0;

	public List<String> head;
}
