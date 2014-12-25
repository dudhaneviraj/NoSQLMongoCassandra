package com.cassandra.tests;

import org.junit.Test;

import com.cassandra.project.RetrieveData;

public class FetchTestage_cassandra {
	@Test
	public void test() {
		RetrieveData ppl = new RetrieveData ();
			 ppl.fetchbyage("onetonine");	
	}
}