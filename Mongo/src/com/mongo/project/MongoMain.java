package com.mongo.project;

import com.mongodb.BasicDBObject;

import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

import java.util.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class MongoMain {
	
	static ArrayList<String> bookList = new ArrayList<String>();

	public static void showFiles(File[] files) {
	    for (File file : files) {
	        if (file.isDirectory()) {
	            //System.out.println("Directory: " + file.getName());
	            showFiles(file.listFiles()); // Calls same method again.
	        } else {
	            //System.out.println("File: " + file.getName());
	            //System.out.println("Path:"+file.getPath() );
	            bookList.add(file.getPath());
	        }
	    }
	}

	public static String readFile(String fileName) throws IOException {
	    try {
	    	BufferedReader br = new BufferedReader(new FileReader(fileName));
	    	StringBuilder sb = new StringBuilder();
	        String  line = br.readLine();
	        while (line != null) {
	            sb.append(line);
	            sb.append(" ");
	            line = br.readLine();
	        }
	        
	        br.close();
	        return sb.toString();
	    } finally {
	     
	    }
	}
	
	
	public static void insertmongo(File []file)throws Exception
	{
		

		// Mongo Connectivity Code---------------------------------------------------------------------------
		MongoClient mongoClient = new MongoClient( "localhost" , 27022 );
	
	//	MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017),
		//                                      new ServerAddress("localhost", 27018),
		  //                                    new ServerAddress("localhost", 27019)));

		DB db = mongoClient.getDB( "database1" );
		DBCollection coll = db.getCollection("books");
		//Insert Into mongo
		int y=1;
		for (File fi : file)
		{
			//System.out.println("File Name:"+fi.getName());
		    
			String content = readFile(fi.getPath());
		//	Path path = Paths.get(fi.getPath());
	      //  byte[] data = Files.readAllBytes(path);
			System.out.println(content);
			//char[] ch=content.toCharArray();
			BasicDBObject doc = new BasicDBObject("name", fi.getName() )
	        .append("type", "Book")
	        .append("count", y)
	        .append("Data", content);
	        coll.insert(doc);
	        //.append("info", new BasicDBObject("x", 203).append("y", 102));
	        y++;
		}				
	}
	
	public void insert()throws Exception
	{
		
		  // Read From File And insert into Mongo--------------------------------------------------------------------
		File[] file=new File("/home/viraj/project/MongoDB/Book_Data").listFiles();
	
		insertmongo(file);
	}
	
	public void fetchAuthorUnshardedDb(String author)throws Exception
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27040 );
		DB db = mongoClient.getDB( "unshardeddb" );
		DBCollection coll = db.getCollection("books");	
		BasicDBObject query = new BasicDBObject("author",author);
		BasicDBObject field1= new BasicDBObject("content",0);
		long startTime = System.currentTimeMillis();
		DBCursor cursor = coll.find(query,field1);
        long endTime = System.currentTimeMillis();
		int recordCount = 0;
		try {
			if(cursor.hasNext()){
				System.out.println("Records exist");
			}else{
				System.out.println("No Records exist");				
			}
		   while(cursor.hasNext()) {
		       String str = ((cursor.next()).toString());
		       //System.out.println(str+"\n");
		       recordCount ++;
		   }
		} finally {
		   cursor.close();
		}	
        long displayEndTime = System.currentTimeMillis();
		System.out.println("Total record count for author without sharding:"+recordCount);
		System.out.println("Total fetch time:"+(endTime-startTime));
		System.out.println("Total fetch + display time:"+(displayEndTime-startTime));
		
	}
	
	public void fetchAuthorFromShards(String author)throws Exception
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27022 );
		DB db = mongoClient.getDB( "database1" );
		DBCollection coll = db.getCollection("books");	
		BasicDBObject query = new BasicDBObject("author",author);
		BasicDBObject field1= new BasicDBObject("content",0);
		long startTime = System.currentTimeMillis();
		DBCursor cursor = coll.find(query,field1);
        long endTime = System.currentTimeMillis();
		int recordCount = 0;
		try {
			if(cursor.hasNext()){
				System.out.println("Records exist");
			}else{
				System.out.println("No Records exist");				
			}
		   while(cursor.hasNext()) {
		       String str = ((cursor.next()).toString());
		       //System.out.println(str+"\n");
		       recordCount ++;
		   }
		} finally {
		   cursor.close();
		}	
        long displayEndTime = System.currentTimeMillis();
		System.out.println("Total record count for autor with sharding:"+recordCount);
		System.out.println("Total fetch time:"+(endTime-startTime));
		System.out.println("Total fetch + display time:"+(displayEndTime-startTime));
		
	}

	public void fetchAuthorAndTitleFromShards(String author, String title)throws Exception
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27022 );
		DB db = mongoClient.getDB( "database1" );
		DBCollection coll = db.getCollection("books");	
		//BasicDBObject query = new BasicDBObject("author",author);
		//BasicDBObject field1= new BasicDBObject("content",0);
		DBObject clause1 = new BasicDBObject("author", author);  
		DBObject clause2 = new BasicDBObject("title", title);    
		BasicDBList and = new BasicDBList();
		and.add(clause1);
		and.add(clause2);
		DBObject query = new BasicDBObject("$and", and);
		long startTime = System.currentTimeMillis();
		DBCursor cursor = coll.find(query);
        long endTime = System.currentTimeMillis();
		int recordCount = 0;
		try {
			if(cursor.hasNext()){
				System.out.println("Records exist");
			}else{
				System.out.println("No Records exist");				
			}
		   while(cursor.hasNext()) {
		       String str = ((cursor.next()).toString());
		       //System.out.println(str+"\n");
		       recordCount ++;
		   }
		} finally {
		   cursor.close();
		}			
        long displayEndTime = System.currentTimeMillis();
		System.out.println("Total record count for author and title with sharding:"+recordCount);
		System.out.println("Total fetch time:"+(endTime-startTime));
		System.out.println("Total fetch + display time:"+(displayEndTime-startTime));
		
	}

	public void fetchAuthorAndTitleWithoutShards(String author, String title)throws Exception
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27040 );
		DB db = mongoClient.getDB( "unshardeddb" );
		DBCollection coll = db.getCollection("books");	
		//BasicDBObject query = new BasicDBObject("author",author);
		//BasicDBObject field1= new BasicDBObject("content",0);
		DBObject clause1 = new BasicDBObject("author", author);  
		DBObject clause2 = new BasicDBObject("title", title);    
		BasicDBList and = new BasicDBList();
		and.add(clause1);
		and.add(clause2);
		DBObject query = new BasicDBObject("$and", and);
		long startTime = System.currentTimeMillis();
		DBCursor cursor = coll.find(query);
        long endTime = System.currentTimeMillis();
		int recordCount = 0;
		try {
			if(cursor.hasNext()){
				System.out.println("Records exist");
			}else{
				System.out.println("No Records exist");				
			}
		   while(cursor.hasNext()) {
		       String str = ((cursor.next()).toString());
		       //System.out.println(str+"\n");
		       recordCount ++;
		   }
		} finally {
		   cursor.close();
		}			
        long displayEndTime = System.currentTimeMillis();
		System.out.println("Total record count for author and title without sharding:"+recordCount);
		System.out.println("Total fetch time:"+(endTime-startTime));
		System.out.println("Total fetch + display time:"+(displayEndTime-startTime));
		
	}

	public void fetchBookContentWithoutShards(String string1, String string2)throws Exception
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27040 );
		DB db = mongoClient.getDB( "unshardeddb" );
		DBCollection coll = db.getCollection("books");	
		long startTime = System.currentTimeMillis();
		BasicDBObject search = new BasicDBObject("$search", "pirates shakespeare");
		BasicDBObject textSearch = new BasicDBObject("$text", search);
		int matchCount = coll.find(textSearch).count();
		System.out.println("Text search matches: "+ matchCount);

		DBCursor cursor = coll.find(textSearch);
        long endTime = System.currentTimeMillis();
		int recordCount = 0;
		try {
			if(cursor.hasNext()){
				System.out.println("Records exist");
			}else{
				System.out.println("No Records exist");				
			}
		   while(cursor.hasNext()) {
		       String str = ((cursor.next()).toString());
		       //System.out.println(str+"\n");
		       recordCount ++;
		   }
		} finally {
		   cursor.close();
		}			
        long displayEndTime = System.currentTimeMillis();
		System.out.println("Total record count for author and title without sharding:"+recordCount);
		System.out.println("Total fetch time:"+(endTime-startTime));
		System.out.println("Total fetch + display time:"+(displayEndTime-startTime));
		
	}

	public void fetchBookContentWithShards(String string1, String string2)throws Exception
	{
		MongoClient mongoClient = new MongoClient( "localhost" , 27022 );
		DB db = mongoClient.getDB( "database1" );
		DBCollection coll = db.getCollection("books");	
		long startTime = System.currentTimeMillis();
		BasicDBObject search = new BasicDBObject("$search", "pirates shakespeare");
		BasicDBObject textSearch = new BasicDBObject("$text", search);
		int matchCount = coll.find(textSearch).count();
		System.out.println("Text search matches: "+ matchCount);

		DBCursor cursor = coll.find(textSearch);
        long endTime = System.currentTimeMillis();
		int recordCount = 0;
		try {
			if(cursor.hasNext()){
				System.out.println("Records exist");
			}else{
				System.out.println("No Records exist");				
			}
		   while(cursor.hasNext()) {
		       String str = ((cursor.next()).toString());
		       //System.out.println(str+"\n");
		       recordCount ++;
		   }
		} finally {
		   cursor.close();
		}			
        long displayEndTime = System.currentTimeMillis();
		System.out.println("Total record count for author and title with sharding:"+recordCount);
		System.out.println("Total fetch time:"+(endTime-startTime));
		System.out.println("Total fetch + display time:"+(displayEndTime-startTime));
		
	}
	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		
		
		MongoMain m=new MongoMain();
		
		
		//m.loadData();
		m.fetchAuthorUnshardedDb("Victor Appleton");
	}
	
	public void loadData() throws UnknownHostException{
		String filePath = "/Users/Rayan/Desktop/College/226-JohnGash/Volumes/PATRIOT/EBooks";
	    File[] files = new File(filePath).listFiles();
	    showFiles(files);
	    String[] authorArray= {"William Shakespeare","Brandon Stanton","James Patterson", "John Grisham", "Rob Elliot", "Stephen King","David Baldacci", "Rick Riordan", "Gillian Flynn"};
	    int id = 1;
	    int countLargeFiles = 0;
	    long totalTimeInMillis=0;
	    long totalTimeForLargeFiles=0;
		MongoClient mongoClient = new MongoClient( "localhost" , 27022 ); // for sharded cluster
	    //MongoClient mongoClient = new MongoClient( "localhost" , 27040 );
	//	MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017),
		//                                      new ServerAddress("localhost", 27018),
		  //                                    new ServerAddress("localhost", 27019)));

		//DB db = mongoClient.getDB( "unshardeddb" );
	    DB db = mongoClient.getDB( "database1" );
		DBCollection coll = db.getCollection("books");
		
       for (Iterator<String> it = bookList.iterator(); it.hasNext();) {
            String currentBook = it.next();
            String line=null, title=null, author=null, postingDate=null, releaseDate=null, lastUpdate =null,language=null;
			BufferedReader br = null;
			StringBuffer content = new StringBuffer();
			try {
				br = new BufferedReader(new FileReader(currentBook));
				while ((line = br.readLine()) != null){
					//System.out.println(line);
					content.append(line);
					line.toLowerCase();
					if(line.toLowerCase().contains("title:")){
						String[] titleArr = line.split(":");
						if(titleArr.length==2)
							title=line.split(":")[1].trim();						
					}else if(line.toLowerCase().contains("author:")){
						String[] authorArr = line.split(":");
						if(authorArr.length==2)
							author=line.split(":")[1].trim();						
					}else if(line.toLowerCase().contains("posting date:")){
						String[] postingDateArr = line.split(":");
						if(postingDateArr.length==2)
							postingDate=line.split(":")[1].trim();						
					}else if(line.toLowerCase().contains("release date:")){
						String[] releaseDateArr = line.split(":");
						if(releaseDateArr.length==2)
							releaseDate=line.split(":")[1].trim();						
					}else if(line.toLowerCase().contains("last updated:")){
						String[] lastUpdateArr = line.split(":");
						if(lastUpdateArr.length==2)
							lastUpdate=line.split(":")[1].trim();						
					}else if(line.toLowerCase().contains("language:")){
						String[] languageArr = line.split(":");
						if(languageArr.length==2)
							language=line.split(":")[1].trim();						
					}
				}
				br.close();
//				System.out.println("Current Book"+currentBook);
//				System.out.println("Title:"+title);
//				System.out.println("Author:"+author);
//				System.out.println("posting date:"+postingDate);
//				System.out.println("release date:"+releaseDate);
//				System.out.println("Last Update:"+lastUpdate);
//				System.out.println("language:"+language);
//				System.out.println("content:"+content);				
				id++;

				if(author == null){
					author = authorArray[id%9];
				}
				if(title == null){
					title = "This is a dummy title "+id;
				}
				
				System.out.println("Current Book"+currentBook);
				System.out.println("Author:"+author);
				
				BasicDBObject doc = new BasicDBObject("filepath", currentBook)
		        .append("title", title)
		        .append("author", author)
		        .append("postingDate", postingDate)
		        .append("language", language)
		        .append("releasedate", releaseDate)
		        .append("content", content.toString());
				
				long startTime = System.currentTimeMillis();
		        coll.insert(doc);
		        long endTime = System.currentTimeMillis();
				
		        totalTimeInMillis += (endTime - startTime);

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e){
				e.printStackTrace();				
			} catch(Exception e){
				e.printStackTrace();
				System.out.println("line"+line);
				if(e.getMessage().contains("16777216")){
					File file = new File(currentBook);
					GridFS gridfs = new GridFS(db, "books");
					GridFSInputFile gfsFile=null;
					try {
						gfsFile = gridfs.createFile(file);
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					gfsFile.setFilename(title);
					countLargeFiles ++;
					long startTime = System.currentTimeMillis();
					gfsFile.save();
			        long endTime = System.currentTimeMillis();
			        System.out.println("Time taken in millis for long insert: "+(endTime-startTime));
			        totalTimeForLargeFiles += (endTime-startTime); 
				}
				continue;
			}
        }
       System.out.println("Time taken for inserting "+id+" files into sharded cluster");
       System.out.println("Total time taken to insert "+(id-countLargeFiles)+" records is "+totalTimeInMillis+" ms.");
       System.out.println("Total time taken to insert large "+countLargeFiles+" records is "+totalTimeForLargeFiles+" ms.");
	}
}
