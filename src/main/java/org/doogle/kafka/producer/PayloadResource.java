package org.doogle.kafka.producer;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.random.RandomGenerator;
import org.doogle.kafka.model.Payload;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

@Path("/payload")
public class PayloadResource {

  public static RandomGenerator r;

  @Channel("timeout-out")
  Emitter<Payload> payloadEmitter;

  public static synchronized int randomInt() //lol thread safety
  {
    if (r == null) {
      r = RandomGenerator.of("L128X1024MixRandom");
    }
    return r.nextInt(1, 101);
  }

  @POST
  @Path("/send")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Uni<Payload> createRequest() {
    Uni<UUID> uuid = Uni.createFrom().item(UUID.randomUUID());

    return uuid.map(this::getPayload).log("payloadtosend").onFailure().recoverWithUni(
            uuid.map(u -> Payload.builder().type("Failure").amount(0).id(u.toString()).build()))
        .ifNoItem().after(Duration.ofMillis(1000))
        .recoverWithUni(
            uuid.map(u -> Payload.builder().type("Timeout").amount(-1).id(u.toString()).build()))
        .invoke(p -> payloadEmitter.send(p)).log("payloadsent");

  }

  private Payload getPayload(UUID uuid) {
    int amount = randomInt();
    String type = "Normal";
    Log.infov("Amount is {0}", amount);
    if (amount % 2 == 0) {
      introduceDelay(amount);
      type = "Timeout";
    }
    return Payload.builder().id(uuid.toString()).amount(amount).type(type).build();
  }

  public void introduceDelay(int amount) {
    try {
      Log.infov("Introducing delay of {0} millis", amount * 100);
      Thread.sleep(amount * 100L);
    } catch (InterruptedException e) {
      Log.errorv(e, "Introducing delay of {0} failed", amount * 100);
    }
  }

  @Incoming("timeout-in")
  @Acknowledgment(Strategy.MANUAL)
  public Uni<CompletionStage<Void>> process(Message<Payload> payload) {
    Log.infov("Received payload - {0}", payload.getPayload());
    Uni<UUID> uuid = Uni.createFrom().item(UUID.randomUUID());
    return Uni.createFrom()
        .item(payload)
        .log("msg1")
        .invoke(p -> introduceDelay(p.getPayload().getAmount()))
        .log("delaycompleted")
        .onFailure()
        .recoverWithUni(
            uuid.map(u -> payload.withPayload(
                Payload.builder().type("Failure").amount(0).id(u.toString()).build())))
        .log("msg2")
        .map(pl -> payload.ack())
        .ifNoItem().after(Duration.ofMillis(1000))
        .recoverWithUni(
            uuid.map(u -> Payload.builder().type("Timeout").amount(10000).id(u.toString()).build())
                .log("msg3")
                .map(pll -> payload.withPayload(pll))
                .map(newpl ->
                    payload.nack(new Exception(
                        "no message received in given time duration. new payload used."))))
        .log("payloadsent");
  }

}