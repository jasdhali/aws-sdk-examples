package com.example.aws.dynamodb;

import java.util.List;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;

/**
 *  https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/java_dynamodb_code_examples.html
 * 
 */
public class ListTables {
	public static void main(String[] args) {
		System.out.println("Listing your amazon ");
		ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
		Region region = Region.US_EAST_1;
		DynamoDbClient ddb = DynamoDbClient.builder()
				.credentialsProvider(credentialsProvider)
				.region(region)
				.build();
		listTables(ddb);
		ddb.close();
	}
	public static void listTables(DynamoDbClient ddb) {
		boolean moreTables = true;
		String lastName = null;
		while(moreTables) {
			try {
				ListTablesResponse response = null;
				if(lastName == null ) {
					ListTablesRequest request = ListTablesRequest.builder().build();
					response = ddb.listTables(request);
				} else {
					ListTablesRequest request = ListTablesRequest.builder().exclusiveStartTableName(lastName).build();
					response = ddb.listTables(request);
				}
				List<String> tableNames = response.tableNames();
				
				if( tableNames.size() > 0 ) {
					for(String curName : tableNames) {
						System.out.format( "* %s\n", curName);
					}
				} else {
					System.out.println("No tables found");
					System.exit(0);
				}
				
				lastName = response.lastEvaluatedTableName();
				
				if( lastName == null ) {
					moreTables = false;
				}
				
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println( e.getMessage() );
			}
		}
		
	}
}
