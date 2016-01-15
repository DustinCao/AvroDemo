package com.infobird.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.infobird.data.entity.User;


public class TestMain {

public static void main(String[] args) {
		
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setAge("20");
		user1.setGender("Man");
		
		User user2 = new User();
		user2.setName("cs");
		user2.setAge("25");
		user2.setGender("Man");
		
		User user3 = new User();
		user3.setName("LiLy");
		user3.setAge("24");
		user3.setGender("Women");
		
		File file = new File("users1.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = 
				new DataFileWriter<User>(userDatumWriter); 
		try {
			dataFileWriter.create(user1.getSchema(), file);
			dataFileWriter.append(user1);
			dataFileWriter.append(user2);
			dataFileWriter.append(user3);
			dataFileWriter.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
		DataFileReader<User> dataFileReader = null;
		
		try {
			dataFileReader = new DataFileReader(file, userDatumReader);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		User user = null;
		try {
			while(dataFileReader.hasNext()) {
				user = dataFileReader.next(user);
				System.out.println("user:" + user);
			}
		} catch (IOException e) {
			// TODO: handle exception
		}
}
}
