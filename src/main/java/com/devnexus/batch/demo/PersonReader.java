package com.devnexus.batch.demo;

import com.devnexus.batch.demo.domain.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.FlatFileItemReader;

public class PersonReader extends FlatFileItemReader<Person> {

  private static final Logger log = LoggerFactory.getLogger(PersonReader.class);

  @Override
  protected Person doRead() throws Exception {

    Person person = super.doRead();
    if (null != person)
      log.warn("Will read person " + person.toString());

    if (Math.random() > 0.95D) {
      throw new Exception("Oooops");
    }

    return person;
  }
}
