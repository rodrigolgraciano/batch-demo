package com.rh.batch.demo.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rh.batch.demo.domain.Rider;
import org.springframework.batch.item.ItemProcessor;

/**
 * Simple processor to demonstrate
 * <li>Filtering</li>
 * <li>Exception with retry on the step</li>
 */
public class RiderItemProcessor implements ItemProcessor<Rider, Rider> {

  private static final Logger log = LoggerFactory.getLogger( RiderItemProcessor.class);

  @Override
  public Rider process(final Rider rider){
    log.info("Processing the race result {}", rider);
    return new Rider(rider.position(), rider.name().toUpperCase(), rider.team(), rider.times());
  }
}
