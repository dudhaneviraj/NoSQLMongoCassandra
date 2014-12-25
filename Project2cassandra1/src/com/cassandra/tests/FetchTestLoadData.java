package com.cassandra.tests;

import org.junit.Test;
import com.cassandra.project.RetrieveData;

public class FetchTestLoadData {
	@Test
	public void test() {
		RetrieveData ppl = new RetrieveData ();
		
			 ppl.fetchbooksfromloaddata(" Oscar Wilde"," The Picture of Dorian Gray");	
	}
}