package com.devnexus.batch.demo.processor;

import com.devnexus.batch.demo.domain.Race;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class RaceItemProcessor implements ItemProcessor<Race, Race> {

  private static final Logger log = LoggerFactory.getLogger(RaceItemProcessor.class);

/*
  @Override
  public Race process(final Race race) throws Exception {

    log.warn("Processing the race result {}", race);
    final int position = race.position();

    if (position > 3) {//Filtering elements
      return null;
    }

    final String pilot = race.pilot().toUpperCase();//Transforming the name to uppercase

    final Race processedRace = new Race(position, pilot);
    log.warn("Converting ({}) into ({})",race, processedRace);

    return processedRace;
  }
*/



  @Override
  public Race process(final Race race) throws Exception {

    log.warn("Processing the race result {}", race);
    final int position = race.position();
    if (position > 3) {
      return null;
    }
    final String pilot = race.pilot().toUpperCase();

    if (position == 2) {
      log.warn("Exception while reading {}", race);
      throw new Exception("Exception while reading");
    }

    final Race processedRace = new Race(position, pilot);
    //log.warn("Converting ({}) into ({})",race, processedRace);

    return processedRace;
  }

}