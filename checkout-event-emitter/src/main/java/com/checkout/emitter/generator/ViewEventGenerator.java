package com.checkout.emitter.generator;

import com.checkout.schema.ViewEvent;
import java.time.Instant;
import java.util.List;
import java.util.Random;

public final class ViewEventGenerator {

  private static final List<String> POSTCODE_AREAS =
      List.of("SW", "E", "N", "M", "B", "L", "G", "EH", "CF", "LS", "BS", "OX", "CB", "NE", "BT");
  private static final String INWARD_LETTERS = "ABDEFGHJLNPQRSTUWXYZ";
  private static final List<String> WEBPAGES =
      List.of("/checkout/basket", "/checkout/delivery", "/checkout/payment", "/checkout/confirm", "/products");
  private static final String BASE_URL = "https://www.checkout.com";
  private static final int MAX_USER_ID = 100_000;
  private static final int MAX_AGE_SECONDS = 600;

  private final Random random;

  public ViewEventGenerator() {
    this.random = new Random();
  }

  public ViewEvent next() {
    var eventId = "evt-" + this.random.nextInt();
    return new ViewEvent(
        eventId,
        random.nextInt(1, MAX_USER_ID),
        randomPostcode(),
        BASE_URL + WEBPAGES.get(random.nextInt(WEBPAGES.size())),
        Instant.now().getEpochSecond() - random.nextInt(MAX_AGE_SECONDS));
  }

  private String randomPostcode() {
    String area = POSTCODE_AREAS.get(random.nextInt(POSTCODE_AREAS.size()));
    int district = random.nextInt(1, 21);
    int sector = random.nextInt(10);
    return area + district + " " + sector + randomInwardLetter() + randomInwardLetter();
  }

  private char randomInwardLetter() {
    return INWARD_LETTERS.charAt(random.nextInt(INWARD_LETTERS.length()));
  }
}
