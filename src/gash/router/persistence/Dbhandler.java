package gash.router.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.MongoClient;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Dbhandler {

	/*public Dbhandler(){

	}*/

	public static void main(String[] args){
		//System.out.println(getFilewithChunckId("HoliVideo.mov",2));
		Map<String,ArrayList<MessageDetails>> map = getAllFilesForReplication();
		for(String filename : map.keySet()){
			System.out.print(filename);
			System.out.println(" --> "+map.get(filename).size());
			for(MessageDetails details : map.get(filename)){
				System.out.print(filename+"  ");
				System.out.println("\t"+details.getChunckId() );
			}
		}
	}

	//Getting all files for replication
	public static synchronized Map<String,ArrayList<MessageDetails>> getAllFilesForReplication(){
		Map<String,ArrayList<MessageDetails>> messageMap = new HashMap<String,ArrayList<MessageDetails>>();
		
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		DistinctIterable<String> distintColl = db.getCollection("Fluffy").distinct("fileName", String.class);
		for(String file : distintColl){
			int n = getChuncks(file);
			ArrayList<MessageDetails> msgDetails = new ArrayList<MessageDetails>();
			for (int i = 1; i <= n; i++) {
				msgDetails.add(getFilewithChunckId(file, i));
			}
			messageMap.put(file, msgDetails);
		}
		
		client.close();
		return messageMap;
	}


	//establish connection with MongoDB
	public static MongoClient getConnection(){
		MongoClient client = null;
		try{
			client = new MongoClient("localhost", 27017);
			System.out.println("connection established!");
		}catch(Exception e){
			System.out.println("Couldnot establish connection !!");
		}
		return client;
	}

	//add file into collection Fluffy
	public static boolean addFile(String fileName,byte[] input){
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		MongoCollection<Document> collection = db.getCollection("Fluffy");

		Document doc = new Document().append("fileName", fileName ).append("bytes", input);
		collection.insertOne(doc);

		if(collection.count()!=0){
			client.close();
			return true;
		}else{
			client.close();
			return false;
		}

	}

	//adding file that has been chuncked
	public static boolean addFile(String fileName,byte[] input, int noOfChuncks, int chunckId){
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		MongoCollection<Document> collection = db.getCollection("Fluffy");

		Document doc = new Document().append("fileName", fileName ).append("bytes", input).append("noOfChuncks", noOfChuncks).append("chunckId", chunckId);
		collection.insertOne(doc);

		if(collection.count()!=0){
			client.close();
			return true;
		}else{
			client.close();
			return false;
		}

	}
	//get file by name from collection Fluffy and return the bytearray representation of file
	public byte[] getFile(String fileName){
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

	public static int getChuncks(String fileName){
		int numchuncks = 0;
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		MongoCollection<Document> collection = db.getCollection("Fluffy");
		FindIterable<Document> doc = collection.find(new Document("fileName", fileName));
		for(Document docs: doc){
			numchuncks++;
		}
		client.close();
		return numchuncks;

	}

	//TODO changes should be made
	//to get file that has been chuncked
	public Map getFilewithChuncks(String fileName, int chunckId){
		Map<String,ArrayList<MessageDetails>> messageMap = new HashMap<String,ArrayList<MessageDetails>>();
		ArrayList<MessageDetails> data = new ArrayList<MessageDetails>();
		Binary bData= null;
		byte[] byteData = {};
		int numchuncks = 0;
		int chunckid =0;
		String filename = "";
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		MongoCollection<Document> collection = db.getCollection("Fluffy");
		FindIterable<Document> doc = collection.find(new Document("fileName", fileName));
		for(Document docs: doc){
			bData = (Binary) docs.get("bytes");
			byteData = bData.getData();
			numchuncks = (Integer) docs.get("noOfChuncks");
			chunckid = (Integer) docs.get("chunckId");
			MessageDetails msgdata = new MessageDetails(fileName, byteData,numchuncks,chunckid);
			data.add(msgdata);
			filename = docs.getString("fileName");
			messageMap.put(filename, data);
			data.clear();

		}
		client.close();
		return messageMap;
	}





	//TODO changes should be made
	//to get file that has been chuncked
	public static  MessageDetails getFilewithChunckId(String fileName, int chunckId){
		Binary bData= null;
		byte[] byteData = {};
		int numchuncks = 0;
		int chunckid =0;
		String filename = "";
		MessageDetails msgdata = null;
		MongoClient client = getConnection();
		MongoDatabase db = client.getDatabase("Fluffy");
		MongoCollection<Document> collection = db.getCollection("Fluffy");
		FindIterable<Document> doc = collection.find(new Document("fileName" , fileName).append("chunckId", chunckId));
		for(Document docs: doc){
			bData = (Binary) docs.get("bytes");
			byteData = bData.getData();
			numchuncks = (Integer) docs.get("noOfChuncks");
			chunckid = (Integer) docs.get("chunckId");
			msgdata = new MessageDetails(fileName,byteData,numchuncks,chunckid);

		}
		client.close();
		/*System.out.println(msgdata.getChunckId());
		System.out.println(msgdata.getNoOfChuncks());
		System.out.println(msgdata.getFileName());
		System.out.println(msgdata.getByteData());*/
		return msgdata;
	}

}
