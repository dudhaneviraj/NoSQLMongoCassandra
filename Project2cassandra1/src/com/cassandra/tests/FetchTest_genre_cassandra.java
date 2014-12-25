package com.cassandra.tests;

import org.junit.Test;

import com.cassandra.project.RetrieveData;

public class FetchTest_genre_cassandra {
	@Test
	public void test() {
		RetrieveData ppl = new RetrieveData ();
		
			 ppl.fetchbygenre("action");	
	}
}