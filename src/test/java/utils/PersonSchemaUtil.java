package utils;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PersonSchemaUtil {

  public static SchemaPlus getRootSchema() throws Exception {
    CalciteConnection calciteConnection = createCalciteConnection();
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    PersonsSchema personsSchema = createPeople();
    ReflectiveSchema reflectiveSchema = new ReflectiveSchema(personsSchema);
    rootSchema.add("people", reflectiveSchema);
    return rootSchema;
  }

  public static CalciteConnection createCalciteConnection() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    return connection.unwrap(CalciteConnection.class);
  }

  public static PersonsSchema createPeople() {
    PersonsSchema personsSchema = new PersonsSchema();
    Person p1 = new Person("Toto", "toto");
    Person p2 = new Person("Tata", "tata");
    Person p3 = new Person("Test", "test");
    personsSchema.setPersons(new Person[]{p1, p2, p3});
    return personsSchema;
  }


}
