package com.cassandra.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class genre_cassandra {
	private Session session;
	private Cluster cluster;
	static ArrayList<String> bookList = new ArrayList<String>();
	
	 public void close() {
	      cluster.close();
	   }
	public static void showFiles(File[] files) {
	    for (File file : files) {
	        if (file.isDirectory()) {
	            System.out.println("Directory: " + file.getName());
	            showFiles(file.listFiles()); // Calls same method again.
	        } else {
	            System.out.println("File: " + file.getName());
	            System.out.println("Path:"+file.getPath() );
	            bookList.add(file.getPath());
	        }
	    }
	}
	public static void main (String args[]){
		genre_cassandra solr = new genre_cassandra();
		solr.connect("127.0.0.1");
		solr.loadData();
		solr.close();
	}
	 public void connect(String node) {
	      cluster = Cluster.builder()
	            .addContactPoint(node).build();
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", 
	            metadata.getClusterName());
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	               host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	   }
	
	public void loadData(){
		long totaltime = 0;
		session = cluster.connect();
		 session.execute("CREATE KEYSPACE action WITH replication " +"= {'class':'SimpleStrategy', 'replication_factor':1};");
		 session.execute("CREATE KEYSPACE horror WITH replication " +"= {'class':'SimpleStrategy', 'replication_factor':1};");
		 session.execute("CREATE KEYSPACE drama WITH replication " +"= {'class':'SimpleStrategy', 'replication_factor':1};");
		 session.execute("CREATE KEYSPACE romance WITH replication " +"= {'class':'SimpleStrategy', 'replication_factor':1};");
		 session.execute("CREATE KEYSPACE comedy WITH replication " +"= {'class':'SimpleStrategy', 'replication_factor':1};");
		 session.execute(
	    	      "CREATE TABLE action.books (" +
	    	            "id int PRIMARY KEY," + 
	    	            "title text," + 
	    	            "author text," + 
	    	            "postingDate text," + 
	    	            "releaseDate text," + 
	    	            "language text," + 
	    	            "content text"+
	    	            ");");
		 session.execute(
	    	      "CREATE TABLE horror.books (" +
	    	            "id int PRIMARY KEY," + 
	    	            "title text," + 
	    	            "author text," + 
	    	            "postingDate text," + 
	    	            "releaseDate text," + 
	    	            "language text," + 
	    	            "content text"+
	    	            ");");   
		 session.execute(
	    	      "CREATE TABLE drama.books (" +
	    	            "id int PRIMARY KEY," + 
	    	            "title text," + 
	    	            "author text," + 
	    	            "postingDate text," + 
	    	            "releaseDate text," + 
	    	            "language text," + 
	    	            "content text"+
	    	            ");");   
		 session.execute(
	    	      "CREATE TABLE romance.books (" +
	    	            "id int PRIMARY KEY," + 
	    	            "title text," + 
	    	            "author text," + 
	    	            "postingDate text," + 
	    	            "releaseDate text," + 
	    	            "language text," + 
	    	            "content text"+
	    	            ");");   
		 session.execute(
	    	      "CREATE TABLE comedy.books (" +
	    	            "id int PRIMARY KEY," + 
	    	            "title text," + 
	    	            "author text," + 
	    	            "postingDate text," + 
	    	            "releaseDate text," + 
	    	            "language text," + 
	    	            "content text"+
	    	            ");");   
		String filePath = "/Users/sharanya/Desktop/EBooks";
		 String[] authorArray= {"William Shakespeare","Brandon Stanton","James Patterson", "John Grisham", "Rob Elliot", "Stephen King","David Baldacci", "Rick Riordan", "Gillian Flynn"};
		 
	    File[] files = new File(filePath).listFiles();
	    showFiles(files);
	    int id = 1;
        for (Iterator<String> it = bookList.iterator(); it.hasNext();) {
            String currentBook = it.next();
            String line=null, title=null, author=null, postingDate=null, releaseDate=null, language=null;
			BufferedReader br = null;
			StringBuffer content = new StringBuffer();
			try {
				br = new BufferedReader(new FileReader(currentBook));
				while ((line = br.readLine()) != null){
					line = line.replaceAll("'", "");
					content.append(line);
					line.toLowerCase();
					if(line.toLowerCase().contains("title:")){
						String[] titleArr = line.split(":");
						if(titleArr.length==2) {
							title=line.split(":")[1];
						}
					}else if(line.toLowerCase().contains("author:")){
						String[] authorArr = line.split(":");
						if(authorArr.length==2)	{
							author=line.split(":")[1];
						}
					}else if(line.toLowerCase().contains("posting date:")){
						String[] postingDateArr = line.split(":");
						if(postingDateArr.length==2)	{
							postingDate=line.split(":")[1];	
						}
					}else if(line.toLowerCase().contains("release date:")){
						String[] releaseDateArr = line.split(":");
						if(releaseDateArr.length==2)	{
							releaseDate=line.split(":")[1];	
						}
					}else if(line.toLowerCase().contains("language:")){
						String[] languageArr = line.split(":");
						if(languageArr.length==2)	{
							language=line.split(":")[1];
						}
					}
				}
				if(author == null){
					author = authorArray[id%9];
				}
				if(title == null){
					title = "This is a dummy title "+id;
				}
				System.out.println("inserting "+id +author+releaseDate+title);
				br.close();
				long starttime = System.currentTimeMillis();
				int k = id%5;
				if(k==0)
					session.execute("INSERT INTO action.books (id,title,author,postingdate,releasedate,language,content) VALUES ("+id+",'"+title+"','"+author+"','"+postingDate+"','"+releaseDate+"','"+language+"','"+content+"')");	
				else if(k==1) 
					session.execute("INSERT INTO horror.books (id,title,author,postingdate,releasedate,language,content) VALUES ("+id+",'"+title+"','"+author+"','"+postingDate+"','"+releaseDate+"','"+language+"','"+content+"')");	
				else if(k==2) 
					session.execute("INSERT INTO drama.books (id,title,author,postingdate,releasedate,language,content) VALUES ("+id+",'"+title+"','"+author+"','"+postingDate+"','"+releaseDate+"','"+language+"','"+content+"')");	
				else if(k==3) 
					session.execute("INSERT INTO romance.books (id,title,author,postingdate,releasedate,language,content) VALUES ("+id+",'"+title+"','"+author+"','"+postingDate+"','"+releaseDate+"','"+language+"','"+content+"')");	
				else 
					session.execute("INSERT INTO comedy.books (id,title,author,postingdate,releasedate,language,content) VALUES ("+id+",'"+title+"','"+author+"','"+postingDate+"','"+releaseDate+"','"+language+"','"+content+"')");	
				id++;
				long endtime = System.currentTimeMillis();
				totaltime = totaltime+(endtime - starttime);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e){
				e.printStackTrace();				
			} 
			  catch(Exception e){
				e.printStackTrace();
				System.out.println("line"+line);
			}
        }
		System.out.println("Total time "+totaltime);
	}
	
}