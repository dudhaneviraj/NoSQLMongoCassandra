package com.cassandra.project;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class RetrieveData {
	String node = "127.0.0.1";
	private Cluster cluster;
	private Session session;
	 public void close() {
	      cluster.close();
	   }
	
	 public void fetchbyauthor(String a_name, String title)	{
		 cluster = Cluster.builder()
         .addContactPoint(node).build();
   Metadata metadata = cluster.getMetadata();
   System.out.printf("Connected to cluster: %s\n", 
         metadata.getClusterName());
   for ( Host host : metadata.getAllHosts() ) {
      System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
            host.getDatacenter(), host.getAddress(), host.getRack());
   }
		 session = cluster.connect();
		 long starttime = System.currentTimeMillis();
		 session.execute("SELECT title FROM " +a_name+".books where title='"+title+"';");
		 long endtime = System.currentTimeMillis();
		 System.out.println("Time taken for Records is "+(endtime - starttime));
		 ResultSet rs = session.execute("SELECT count(*) FROM " +a_name+".books where title='"+title+"';");
		 for (Row row : rs) {
			    System.out.println(row.getLong("count"));
			}
		 cluster.close();
	 }
	 
	 public void fetchbygenre(String g_name)	{
		 cluster = Cluster.builder()
		            .addContactPoint(node).build();
		      Metadata metadata = cluster.getMetadata();
		      System.out.printf("Connected to cluster: %s\n", 
		            metadata.getClusterName());
		      for ( Host host : metadata.getAllHosts() ) {
		         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
		               host.getDatacenter(), host.getAddress(), host.getRack());
		      }
		 session = cluster.connect();
		 long starttime = System.currentTimeMillis();
		
		  session.execute("SELECT title FROM " +g_name+".books;");
		 long endtime = System.currentTimeMillis();
		 System.out.println("Time taken for Records is "+(endtime - starttime));
		 ResultSet rs = session.execute("SELECT count(*) FROM " +g_name+".books;");
		 for (Row row : rs) {
			    System.out.println(row.getLong("count"));
			}
		 cluster.close();
	 }
	 
	 public void fetchbyage(String age)	{
		 cluster = Cluster.builder()
         		.addContactPoint(node).build();
		 Metadata metadata = cluster.getMetadata();
		 System.out.printf("Connected to cluster: %s\n", 
         metadata.getClusterName());
		 for ( Host host : metadata.getAllHosts() ) {
			 System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
           host.getDatacenter(), host.getAddress(), host.getRack());
		 }
		 session = cluster.connect();
		 long starttime = System.currentTimeMillis();
		 session.execute("SELECT title FROM " +age+".books;");
		 long endtime = System.currentTimeMillis();
		 System.out.println("Time taken for Records is "+(endtime - starttime));
		 ResultSet rs = session.execute("SELECT count(*) FROM " +age+".books;");
		 for (Row row : rs) {
			    System.out.println(row.getLong("count"));
			}
		 cluster.close();
	 	}
	 
	 public void fetchbooksfromloaddata(String author, String title)	{
		 cluster = Cluster.builder()
		            .addContactPoint(node).build();
		      Metadata metadata = cluster.getMetadata();
		      System.out.printf("Connected to cluster: %s\n", 
		            metadata.getClusterName());
		      for ( Host host : metadata.getAllHosts() ) {
		         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
		               host.getDatacenter(), host.getAddress(), host.getRack());
		      }
		 session = cluster.connect();
		 long starttime = System.currentTimeMillis();
		 session.execute("SELECT title FROM demo.books where author = '"+author+"' and title ='"+title+"' ALLOW FILTERING;");
		 long endtime = System.currentTimeMillis();
		 System.out.println("Time taken for Records is "+(endtime - starttime));
		 ResultSet rs = session.execute("SELECT count(*) FROM demo.books where author = '"+author+"' and title ='"+title+"' ALLOW FILTERING;");
		 for (Row row : rs) {
			    System.out.println(row.getLong("count"));
			}
	     cluster.close();
	 }
	 
	 public void fetchtextfromloaddata1(String text)	{
			 cluster = Cluster.builder()
	         .addContactPoint(node).build();
	   Metadata metadata = cluster.getMetadata();
	   System.out.printf("Connected to cluster: %s\n", 
	         metadata.getClusterName());
	   for ( Host host : metadata.getAllHosts() ) {
	      System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	            host.getDatacenter(), host.getAddress(), host.getRack());
	   }
		 session = cluster.connect();
		 long starttime = System.currentTimeMillis();
		 session.execute("SELECT * FROM wordlist.doclist where word ='"+text+"';");
		 long endtime = System.currentTimeMillis();
		 ResultSet rs = session.execute("SELECT count(*) FROM wordlist.doclist where word ='"+text+"';");
		 for (Row row : rs) {
			    System.out.println(row.getLong("count"));
			}
		 System.out.println("Time taken for Records is "+(endtime - starttime));
		 cluster.close();
	 }
}