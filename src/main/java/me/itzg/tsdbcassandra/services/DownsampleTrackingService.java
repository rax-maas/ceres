package me.itzg.tsdbcassandra.services;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import me.itzg.tsdbcassandra.config.DownsampleProperties;
import me.itzg.tsdbcassandra.downsample.TemporalNormalizer;
import me.itzg.tsdbcassandra.model.PendingDownsampleSet;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final Charset HASHING_CHARSET = StandardCharsets.UTF_8;
  private static final String DELIM = "|";
  private static final String PREFIX_INGESTING = "ingesting";
  private static final String PREFIX_PENDING = "pending";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final DownsampleProperties properties;
  private final TemporalNormalizer timeSlotNormalizer;
  private final HashFunction hashFunction;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   DownsampleProperties properties) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    timeSlotNormalizer = new TemporalNormalizer(properties.getTimeSlotWidth());
    hashFunction = Hashing.murmur3_32();
  }

  public Publisher<?> track(String tenant, String seriesSet, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }

    final HashCode hashCode = hashFunction.newHasher()
        .putString(tenant, HASHING_CHARSET)
        .putString(seriesSet, HASHING_CHARSET)
        .hash();
    final int partition = Hashing.consistentHash(hashCode, properties.getPartitions());
    final Instant normalizedTimeSlot = timestamp.with(timeSlotNormalizer);

    final String ingestingKey = PREFIX_INGESTING
        + DELIM + partition
        + DELIM + normalizedTimeSlot.getEpochSecond();

    final String pendingKey = PREFIX_PENDING
        + DELIM + partition
        + DELIM + normalizedTimeSlot.getEpochSecond();
    final String pendingValue = tenant
        + DELIM + seriesSet;

    return redisTemplate.opsForValue()
        .set(ingestingKey, "", properties.getLastTouchDelay())
        .and(
            redisTemplate.opsForSet()
            .add(pendingKey, pendingValue)
        );
  }

  private static PendingDownsampleSet buildPending(String pendingKey, String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSet(pendingValue.substring(1+splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(
            Long.parseLong(pendingKey.substring(1 + pendingKey.lastIndexOf(DELIM)))
        ));
  }

  public Flux<PendingDownsampleSet> retrieveReadyOnes(int partition) {
    return redisTemplate
        // scan over pending timeslots
        .scan(
            ScanOptions.scanOptions()
                .match(PREFIX_PENDING + DELIM + partition + DELIM + "*")
                .build()
        )
        // expand each of those
        .flatMap(pendingKey ->
            // ...first see if the ingestion key
            redisTemplate.hasKey(
                PREFIX_INGESTING + pendingKey.substring(PREFIX_PENDING.length())
            )
                // ...has gone idle and been expired away
                .filter(stillIngesting -> !stillIngesting)
                // ...otherwise, expand by popping the downsample sets from that timeslot
                .flatMapMany(stillIngesting -> popMany(pendingKey))
        );
  }

  private Flux<PendingDownsampleSet> popMany(String pendingKey) {
    return Flux.from(
        redisTemplate.opsForSet()
            .pop(pendingKey, properties.getPendingRetrievalLimit())
            // repeat the
            .repeatWhen(DownsampleTrackingService::notEmpty)
            .map(pendingValue -> buildPending(pendingKey, pendingValue))
    );
  }

  private static Publisher<?> notEmpty(Flux<Long> amountFlux) {
    return amountFlux.handle((amount, sink) -> {
      log.info("amount={}", amount);
      if (amount > 0) {
        sink.next(true);
      } else {
        sink.complete();
      }
    });
  }
}
