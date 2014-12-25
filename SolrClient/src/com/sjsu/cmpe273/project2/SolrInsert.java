package com.sjsu.cmpe273.project2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SolrInsert {

	static ArrayList<String> bookList = new ArrayList<String>();

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
		SolrInsert solr = new SolrInsert();
		//solr.loadData();
		solr.querySolr();
	}
	
	public void querySolr(){
		
		String urlString = "http://localhost:8983/solr";
		SolrServer solr = new HttpSolrServer(urlString);
		SolrQuery parameters = new SolrQuery();
		parameters.set("q", "text:shakespeare&fl=title");
		parameters.setRows(10000);
		try {
			QueryResponse response = solr.query(parameters);
			SolrDocumentList list = response.getResults();
			System.out.println("The number of documents fetched:"+list.size());
			/*for (int i=0; i<list.size(); i++){
				System.out.println(list.get(i).toString());
			}*/
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	public void loadData(){
		String filePath = "/Users/Rayan/Desktop/College/226-JohnGash/Volumes/PATRIOT/EBooks";
	    String[] authorArray= {"William Shakespeare","Brandon Stanton","James Patterson", "John Grisham", "Rob Elliot", "Stephen King","David Baldacci", "Rick Riordan", "Gillian Flynn"};
	    File[] files = new File(filePath).listFiles();
	    showFiles(files);
	    long totalTimeInMillis=0;
		String urlString = "http://localhost:8983/solr";
		SolrServer solr = new HttpSolrServer(urlString);
	    int id = 1;
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
//					System.out.println("Current Book"+currentBook);
//					System.out.println("Title:"+title);
//					System.out.println("Author:"+author);
//					System.out.println("posting date:"+postingDate);
//					System.out.println("release date:"+releaseDate);
//					System.out.println("Last Update:"+lastUpdate);
//					System.out.println("language:"+language);
//					System.out.println("content:"+content);				

					if(author == null){
						author = authorArray[id%9];
					}
					if(title == null){
						title = "This is a dummy title "+id;
					}
					
					System.out.println("Current Book"+currentBook);
					System.out.println("Author:"+author);
					
					long startTime = System.currentTimeMillis();

					SolrInputDocument document = new SolrInputDocument();
					document.addField("id", id);
					document.addField("title", title);
					document.addField("author", author);
					document.addField("postingdate", postingDate);
					document.addField("releasedate", releaseDate);
					document.addField("lastupdate", lastUpdate);
					document.addField("language", language);
					document.addField("content", content);
					
					UpdateResponse response = solr.add(document);
					System.out.println("response is"+response.toString());
					solr.commit();				
					id++;
					
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
					continue;
				}
	        }		
	       
	       System.out.println("Time taken for inserting "+id+" files into SOLR");
	       System.out.println("Total time taken to insert "+id+" records is "+totalTimeInMillis+" ms.");
	       //System.out.println("Total time taken to insert large "+countLargeFiles+" records is "+totalTimeForLargeFiles+" ms.");

	}
}
