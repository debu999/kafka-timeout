package org.doogle.kafka.producer;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import org.doogle.kafka.model.Quote;

public class QuoteDeserializer extends ObjectMapperDeserializer<Quote> {

  public QuoteDeserializer() {
    super(Quote.class);
  }
}