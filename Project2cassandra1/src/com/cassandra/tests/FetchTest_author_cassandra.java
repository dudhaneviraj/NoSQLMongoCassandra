package com.cassandra.tests;

import org.junit.Test;

import com.cassandra.project.RetrieveData;

public class FetchTest_author_cassandra {
	@Test
	public void test() {
		RetrieveData ppl = new RetrieveData ();
			 ppl.fetchbyauthor("oscarwilde"," The Picture of Dorian Gray");	
	}
}
