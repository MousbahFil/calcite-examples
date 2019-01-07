package utils;

import java.util.Arrays;

public class PersonsSchema {

  public Person[] persons;

  public Person[] getPersons() {
    return persons;
  }

  public void setPersons(final Person[] persons) {
    this.persons = persons;
  }

  @Override public String toString() {
    return "PersonsSchema{" +
        "persons=" + Arrays.toString(persons) +
        '}';
  }
}
