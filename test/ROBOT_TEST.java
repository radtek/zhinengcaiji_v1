import java.awt.Rectangle;
import java.awt.Robot;
import java.awt.image.BufferedImage;

public class ROBOT_TEST
{
	public static void main(String[] args) throws Exception
	{
		Robot rb = new Robot();
		BufferedImage img = rb.createScreenCapture(new Rectangle(22, 22));
		System.out.println(img);
	}
}
