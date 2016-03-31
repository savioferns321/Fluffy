package gash.router.persistence;

import org.bson.Document;
import org.bson.types.Binary;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Dbhandler {
	
	public Dbhandler(){
	
	}
	//establish connection with MongoDB
	public static MongoClient getConnection() throws Exception{
		MongoClient client = null;
		try{
		client = new MongoClient("localhost", 27017);
		System.out.println("connection established!");
		}catch(Exception e){
			System.out.println("Couldnot establish connection !!");
			throw e;
	}
		return client;
	}
	
	//add file into collection Fluffy
	public static void addFile(String fileName,byte[] input) throws Exception{
	MongoClient client = getConnection();
	MongoDatabase db = client.getDatabase("Fluffy");
	MongoCollection<Document> collection = db.getCollection("Fluffy");
	
	Document doc = new Document().append("fileName", fileName ).append("bytes", input);
	collection.insertOne(doc);

	if(collection.count()!=0){
		System.out.println("sucessful");
		System.out.println(doc);
	}else{
		System.out.println("something went wrong");
	}
	client.close();
	}
	
	//get file by name from collection Fluffy and return the bytearray representation of file
	public static byte[] getFile(String fileName) throws Exception{
		Binary bData= null;
		byte[] byteData = {};
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		MongoCollection<Document> collection = db.getCollection("Fluffy");
		FindIterable<Document> doc = collection.find(new Document("fileName", fileName));
		for(Document docs: doc){
			bData = (Binary) docs.get("bytes");
			byteData = bData.getData();
		}
		client.close();
		return byteData;
	}					
		
}
	

