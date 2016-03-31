package gash.router.persistence;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class fileHashing {
	public fileHashing(){
		
	}
	
	public static void main(String[] args) throws FileNotFoundException, InterruptedException{
		int i=0;
		File file = new File("/Users/Student/Pictures/fluffyImages/LifeForceProjectReport.pdf");
		 
        FileInputStream fis = new FileInputStream(file);
       
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        try {
            for (int readNum; (readNum = fis.read(buf)) != -1;) {
                bos.write(buf, 0, readNum);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        	
        }
        byte[] bytes = bos.toByteArray();
        
       
        int hash = 0;
 	    while(i<15){
 	    		hash = getHash(bytes);
 	    		
 	    		
 	           Fact(hash);
 	    		 
 	           i++;
 	    }
	}
 	
	
	
	public static int getHash(byte[] bytes){
		Checksum checksum = new CRC32();
		checksum.update(bytes, 0, bytes.length);
		long checksumValue = checksum.getValue();
		return (int) checksumValue;

	}
	
	public static void Fact(int hash){
		int tmp = (int) Math.sqrt(hash);
		while(tmp > 0){   
           
                    hash = hash * tmp;
                    tmp = (int) Math.sqrt(tmp);
       }
	}
	
}
