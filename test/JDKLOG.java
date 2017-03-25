import java.util.logging.Level;
import java.util.logging.Logger;


public class JDKLOG
{
	public static void main(String[] args)
	{
		Logger log = Logger.getLogger(JDKLOG.class.getName());
		log.log(Level.INFO, "adsf");
		log.log(Level.INFO, "adsf");
		log.log(Level.INFO, "adsf");
	}
}
