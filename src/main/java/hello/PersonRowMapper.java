/*package hello;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.boot.autoconfigure.security.SecurityProperties.User;
import org.springframework.jdbc.core.RowMapper;

public class PersonRowMapper implements RowMapper<Person>{

	  @Override
	  public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
		  
	   Person person = new Person();
	   person.setFirstName(rs.getString("firstName"));
	   person.setLastName(rs.getString("lastName"));
	   
	   return person;
	  }
	  
	 }
*/