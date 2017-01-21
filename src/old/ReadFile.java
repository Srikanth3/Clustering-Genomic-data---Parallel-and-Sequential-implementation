package old;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class ReadFile {
	
	public static float[][] readFile() throws FileNotFoundException
	//public static void main(String[] args) throws FileNotFoundException
	{
		File file=new File("inputs/cho.txt");
        Scanner scan=new Scanner(file);
        
        float[][] data = new float[386][18];
        int row=0,col=0;
        
        while(scan.hasNextLine())
        {
        	StringTokenizer st = new StringTokenizer(scan.nextLine());
        	col=0;
        	while(st.hasMoreTokens()) 
        	{
        		String token = st.nextToken();
        		data[row][col] = Float.parseFloat(token);
        	        	
        		col++;
        	}
        	row++;
        }
        
        return data;
	}

}