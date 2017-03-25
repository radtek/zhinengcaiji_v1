import java.awt.Desktop;

public class DesktopTest
{
	public static void main(String[] args) throws Exception
	{
		System.out.println(Desktop.getDesktop().isSupported(Desktop.Action.BROWSE));;
	}
}
