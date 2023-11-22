package org.doogle.kafka.processor;

import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Random;
import org.doogle.kafka.model.Quote;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

/**
 * A bean consuming data from the "quote-requests" Kafka topic (mapped to "requests" channel) and
 * giving out a random quote. The result is pushed to the "quotes" Kafka topic.
 */
@ApplicationScoped
public class QuotesProcessor {

  private final Random random = new Random();

  @Incoming("requests-in")
  @Outgoing("quotes-out")
  @Blocking
  public Quote process(String quoteRequest) throws InterruptedException {
    // simulate some hard working task
    Thread.sleep(200);
    return new Quote(quoteRequest, random.nextInt(100));
  }
}