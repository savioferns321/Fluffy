package gash.router.mongodb;


import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;





public class Dbhandler {
	
	public MongoClient getConnection(){
		MongoClient client = null;
		try{
		client = new MongoClient("localhost", 27017);
		System.out.println("connection established!");
		}catch(Exception e){
			System.out.println("Couldnot establish connection !!");
	}
		return client;
	}
	
	public void addFile(byte[] input){
	MongoClient client = getConnection();
	MongoDatabase db = client.getDatabase("Fluffy");
	
	}
	
						
		//GridFS gfs = new GridFS(db,"images");
				
		//to read files from a folder
		/*File folder = new File("/Users/Student/Pictures/fluffyImages");
		File[] list = folder.listFiles();
		for(File file: list){
		GridFSInputFile gridFsInputFile = gfs.createFile(new File("/Users/Student/Pictures/fluffyImages/"+file.getName()));
		System.out.println(gridFsInputFile);
		gridFsInputFile.setFilename(file.getName());
		gridFsInputFile.save();
		}
		for(File file : list){
		GridFSDBFile outputImageFile = gfs.findOne(file.getName());
		System.out.println("Total Chunks: " + outputImageFile.numChunks());
		String imageLocation = "/Users/Student/Pictures/"+file.getName()+".jpeg";
		outputImageFile.writeTo(imageLocation);
		}*/	
	}

