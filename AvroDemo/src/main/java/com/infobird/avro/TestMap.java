package com.infobird.avro;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.infobird.data.entity.User;

import example.avro.UserMap;


public class TestMap {

	public static void main(String[] args) {
		
		UserMap user1 = new UserMap();
		Map<CharSequence, Long> map = new HashMap<CharSequence, Long>();
		map.put("key1", 10L);
		user1.setName(map);
		
		/*User user2 = new User();
		user2.setName("cs");
		user2.setAge("25");
		user2.setGender("Man");
		
		User user3 = new User();
		user3.setName("LiLy");
		user3.setAge("24");
		user3.setGender("Women");*/
		
		File file = new File("users1.avro");
		DatumWriter<UserMap> userDatumWriter = new SpecificDatumWriter<UserMap>(UserMap.class);
		DataFileWriter<UserMap> dataFileWriter = 
				new DataFileWriter<UserMap>(userDatumWriter); 
		
		try {
			dataFileWriter.create(user1.getSchema(), file);
			dataFileWriter.append(user1);
			//dataFileWriter.append(user2);
			//dataFileWriter.append(user3);
			dataFileWriter.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		DatumReader<UserMap> userDatumReader = new SpecificDatumReader<UserMap>(UserMap.class);
		DataFileReader<UserMap> dataFileReader = null;
		
		try {
			dataFileReader = new DataFileReader<UserMap>(file, userDatumReader);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		UserMap user = null;
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
