package com.mongo.tests;

import org.junit.Test;

import com.mongo.project.*;

public class MongoTestCase {
	/*@Test
	public void testAuthorWithoutShards() {
		MongoMain mongoQuery = new MongoMain();
		
		try {
			mongoQuery.fetchAuthorUnshardedDb("Victor Appleton");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	@Test
	public void fetchAuthorWithShards(){
		MongoMain mongoQuery = new MongoMain();

		try {
			mongoQuery.fetchAuthorFromShards("Victor Appleton");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	

	@Test
	public void fetchAuthorAndTitleFromShards(){
		MongoMain mongoQuery = new MongoMain();

		try {
			mongoQuery.fetchAuthorAndTitleFromShards("Victor Appleton", "Tom Swift and His Motor-Cycle");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}*/
	
	@Test
	public void fetchAuthorAndTitleWithoutShards(){
		MongoMain mongoQuery = new MongoMain();

		try {
			mongoQuery.fetchAuthorAndTitleWithoutShards("Victor Appleton", "Tom Swift and His Motor-Cycle");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	/*
	@Test
	public void fetchBookContentWithoutShards(){
		MongoMain mongoQuery = new MongoMain();

		try {
			mongoQuery.fetchBookContentWithoutShards("Victor Appleton", "Tom Swift and His Motor-Cycle");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
/*
	@Test
	public void fetchBookContentWithShards(){
		MongoMain mongoQuery = new MongoMain();

		try {
			mongoQuery.fetchBookContentWithShards("Victor Appleton", "Tom Swift and His Motor-Cycle");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	*/

}
