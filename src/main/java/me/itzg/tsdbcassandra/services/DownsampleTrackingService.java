package me.itzg.tsdbcassandra.services;

import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import me.itzg.tsdbcassandra.config.DownsampleProperties;
import me.itzg.tsdbcassandra.downsample.TemporalNormalizer;
import me.itzg.tsdbcassandra.entities.PendingDownsampleSet;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final Charset HASHING_CHARSET = StandardCharsets.UTF_8;

  private final ReactiveCassandraTemplate cassandraTemplate;
  private final TimestampProvider timestampProvider;
  private final DownsampleProperties properties;
  private final TemporalNormalizer timeSlotNormalizer;
  private final HashFunction hashFunction;

  @Autowired
  public DownsampleTrackingService(ReactiveCassandraTemplate cassandraTemplate,
                                   TimestampProvider timestampProvider,
                                   DownsampleProperties properties) {
    this.cassandraTemplate = cassandraTemplate;
    this.timestampProvider = timestampProvider;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isEnabled() ? "enabled" : "disabled");
    timeSlotNormalizer = new TemporalNormalizer(properties.getTimeSlotWidth());
    hashFunction = Hashing.murmur3_32();
  }

  public Publisher<?> track(String tenant, String seriesSet, Instant timestamp) {
    if (!properties.isEnabled()) {
      return Mono.empty();
    }

    final HashCode hashCode = hashFunction.newHasher()
        .putString(tenant, HASHING_CHARSET)
        .putString(seriesSet, HASHING_CHARSET)
        .hash();
    final int partition = Hashing.consistentHash(hashCode, properties.getPartitions());

    return cassandraTemplate.update(
        new PendingDownsampleSet()
            .setPartition(partition)
            .setTimeSlot(timestamp.with(timeSlotNormalizer))
            .setTenant(tenant)
            .setSeriesSet(seriesSet)
            .setLastTouch(timestampProvider.now())
    );
  }

  public Flux<PendingDownsampleSet> retrieveReadyOnes(int partition) {
    final Instant now = timestampProvider.now();

    return cassandraTemplate.select(
        query(
            where("partition").is(partition),
            // make sure the whole slot has elapsed
            where("timeSlot").lt(now.minus(properties.getTimeSlotWidth())),
            // make sure slot isn't busy with updates
            where("lastTouch").lt(now.minus(properties.getLastTouchDelay()))
        )
            // for lastTouch filtering
            .withAllowFiltering(),
        PendingDownsampleSet.class
    );
  }

  public Mono<?> complete(PendingDownsampleSet entry) {
    return cassandraTemplate.delete(entry);
  }

}
