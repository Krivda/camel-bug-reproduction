package camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWithRouteBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.endpoint.dsl.DirectEndpointBuilderFactory.DirectEndpointBuilder;
import org.apache.camel.builder.endpoint.dsl.KafkaEndpointBuilderFactory.KafkaEndpointBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.kafka;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("DefaultAnnotationParam") // need exactly 2 partitions
@CamelSpringBootTest
@EnableAutoConfiguration
@UseAdviceWith
@EmbeddedKafka(controlledShutdown = true, partitions = 2) // Exactly 2 partitions
public class BreakOnFirstErrorTest {


    @Autowired
    protected CamelContext camelContext;

    @Autowired
    protected ProducerTemplate kafkaProducer;

    private final String kafkaGroupId = "test_group_id";
    private final String kafkaTopicName = "test_topic";
    private final List<String> consumedRecords = new ArrayList<>();
    @Value("${spring.embedded.kafka.brokers}")
    private String kafkaBrokerAddress;

    @BeforeEach
    public void setupTestRoutes() throws Exception {
        AdviceWithRouteBuilder.addRoutes(camelContext, builder -> {
            createProducerRoute(builder);
            createConsumerRoute(builder);
        });
        camelContext.start();
    }

    @Test
    public void shouldOnlyReconsumeFailedMessageOnError() {
        // partitions are consumed backwards :)
        final List<String> producedRecordsPartition0 = List.of("5", "6", "7", "8", "9", "10", "11"); // <- Error is thrown once on record "5"
        final List<String> producedRecordsPartition1 = List.of("1", "2", "3", "4");

        final List<String> expectedConsumedRecords = List.of("1", "2", "3", "4"); // only records form part_0 should be consumed


        produceRecords(producedRecordsPartition0, producedRecordsPartition1);

        await().untilAsserted(() ->
                // Assertion fails as all records on topic are reconsumed on error.
                assertThat(consumedRecords).isEqualTo(expectedConsumedRecords));
        //actual results:
        //expected: ["1", "2", "3", "4"]
        //but was: ["1", "2", "3", "4", "9", "10", "11", "1", "2", "3", "4"]

        // Detailed description here. Warning: a huge wall of text
        /*
         Preface: Many thanks to Karen Lease, for this test case mockup, which is very convenient to use and to adapt.

         TL;DR:
         This happens because of commits are getting totally messed up when the poll returns records from multiple
         partitions and when the exception is thrown in a route while processing the very first record of a second
         (any but the first) partition.
         The root cause is the lastResult variable isn't cleared/reinitialized neither between partitions processing nor
         between polls

         Detailed bug description:
         It all happens because of the two bugs introduced in https://issues.apache.org/jira/browse/CAMEL-18350. Though
         it was a great fix, which shifted breakOnFirstError behaviour from "totally broken" to "breaks in specific case".
         But nevertheless it's a very critical bug.

         Bug1. lastResult variable is not getting initialized before the next poll, and it still contains an old offset
          from the previous polling
         Bug2. lastResult variable is not getting initialized before proceeding to the next partition, and it still
          contains an old offset from the previous partition

         let's dive into:
         this mock example (again, many thanks to Karen Lease) setups quite a specific situation and a specific poll.
         Due to .maxPollRecords(8) - we will happen to get exactly 8 records in a single poll.
          4 of them will be from part_0 and 4 from part_1.
         The example is set up to produce an exception exactly if message body == "5",
         which is (and that's crucial) the very first record in part_0.
         route will start consuming all records from part_1 (partitions are consumed backwards by the iterator).
         We will successfully process record "1", "2", "3", "4" from it, and then we will start with part_0.
         The first record is "5" which will break the route, causing a processing exception. And here all the bugs
         come into play :(

         let's dive into the code of KafkaRecordProcessorFacade.processPolledRecords(..)

        //       while (partitionIterator.hasNext() && !isStopping()) {
        // [1]        TopicPartition partition = partitionIterator.next();
        //
        //           List<ConsumerRecord<Object, Object>> partitionRecords = allRecords.records(partition);
        //           Iterator<ConsumerRecord<Object, Object>> recordIterator = partitionRecords.iterator();
        //
        //           while (!lastResult.isBreakOnErrorHit() && recordIterator.hasNext() && !isStopping()) {
        //               ConsumerRecord<Object, Object> record = recordIterator.next();
        //
        // [2]            lastResult = processRecord(partition, partitionIterator.hasNext(), recordIterator.hasNext(), lastResult,
        //                       kafkaRecordProcessor, record);
        //
        //               // stripped, as consumerListener == null in the case
        //           }
        //           // stripped, as breakOnFirstError is enabled
        //       }

         Here come the two loops, "outer", the partitionIterator loop and "inner", the recordIterator loop.
         Note the place [1]: partition is initialized immediately at the outer loop start, but lastResult is set
         with new value much, much later at [2]. It means, that at every new iteration of outer loop up to the point [2]
         we have lastResult value got from previous iteration: either from prev partition processing (Bug1) or even prev poll
         (as  lastResult is passed as parameter from outer scope of KafkaFetchRecords.startPolling(), where it's not cleared
         before new poll) and still holds the __offset__ value from prev iteration (Bug2).
         Everything goes smooth if point [2] executes without exception and lastResult is getting updated set successfully.
         Everything becomes messy if there's an exception.
         Note, that "partition" variable is reassigned at the outer loop start is being passed to processRecord(...) method.

         The actual exception is caught in later in KafkaRecordProcessor.processExchange()

        //     try {
        //         processor.process(exchange);
        //     } catch (Exception e) {
        //         exchange.setException(e);
        //     }
        //     if (exchange.getException() != null) {
        // [3]    boolean breakOnErrorExit = processException(exchange, partition, lastResult.getPartitionLastOffset(),
        //                 exceptionHandler);
        //         return new ProcessingResult(breakOnErrorExit, lastResult.getPartitionLastOffset(), true);
        //     } else {
        //         return new ProcessingResult(false, record.offset(), exchange.getException() != null);
        //     }

          Note: lastResult variable was not changed at this point.
          Note point [3]: here processException()'s parameter "partitionLastOffset" is calculated as lastResult.getPartitionLastOffset()

         As we do know now, lastResult here has some "dirty" value got from some previous iteration,
         which has nothing to do with the current partition. But in processException() code it's presumed to be a valid offset
         for the __current__ partition, and it gets committed to it at point [4] of KafkaRecordProcessor.processException()

        //      if (configuration.isBreakOnFirstError()) {
        //          // we are failing and we should break out
        //          if (LOG.isWarnEnabled()) {
        //              LOG.warn("Error during processing {} from topic: {}", exchange, partition.topic(), exchange.getException());
        //              LOG.warn("Will seek consumer to offset {} and start polling again.", partitionLastOffset);
        //          }
        //
        //          // force commit, so we resume on next poll where we failed except when the failure happened
        //          // at the first message in a poll
        //          if (partitionLastOffset != AbstractCommitManager.START_OFFSET) {
        // [4]           commitManager.forceCommit(partition, partitionLastOffset);
        //          }
        //
        //          // continue to next partition
        //          return true;
        //      }

         Note: here we have committed a completely "random" (unrelated to the partition) offset to the current partition
         causing component to ether loosing or re-consuming records.
         In this setup, we commit invalid offset=3 to part_0, and thus have lost records "5", "6", "7", "8"
         it doesn't stop here, as the valid commit to part_1 also was not made, which causes component to re-consume
         records "1", "2", "3", "4"

         Actually, I don't have any quick fix idea for these bugs, as the fix surely will be very intrusive. Somehow,
         lastOffset should be cleared before the start of polling loop (that's easy) and at the start of the partitions
         processing loop (which isn't that straightforward, as it might affect different implementations of
         CommitManager which may come with
        </editor-fold>
        */

    }

    private void produceRecords(final List<String> producedRecordsPartition0, List<String> producedRecordsPartition1) {
        // Producing in two batches to ensure application has a committed offset
        //final int size = producedRecordsPartition1.size();
        //final int middleIndex = (size + 1) / 2;
        //final List<String> firstHalf = new ArrayList<>(producedRecordsPartition1.subList(0, middleIndex));
        //final List<String> secondHalf = new ArrayList<>(producedRecordsPartition1.subList(middleIndex, size));

        producedRecordsPartition0.forEach(v -> kafkaProducer.sendBodyAndHeader(v, KafkaConstants.PARTITION_KEY, 0));
        producedRecordsPartition1.forEach(v -> kafkaProducer.sendBodyAndHeader(v, KafkaConstants.PARTITION_KEY, 1));


//        await().until(() -> getCurrentOffset() > 0);
//        System.out.println("Offset committed: " + getCurrentOffset());
//        secondHalf.forEach(kafkaProducer::sendBody);
    }

    private Long getCurrentOffset() {
        try {
            return Optional.ofNullable(KafkaTestUtils.getCurrentOffset(kafkaBrokerAddress, kafkaGroupId, kafkaTopicName, 0)).map(OffsetAndMetadata::offset).orElse(0L);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createConsumerRoute(RouteBuilder builder) {
        builder.from(kafkaTestTopic().groupId(kafkaGroupId).autoOffsetReset("earliest").breakOnFirstError(true)
                //  get 8 recs total: 4 from part_0 and 4 from part_1 in first poll. recs "9" and "10" won't be in first poll
                .maxPollRecords(8)).log(">> Partition:${header.kafka.PARTITION} offset:${header.kafka.OFFSET} Body:${body}").process(this::ifIsFifthRecordThrowException).process().body(String.class, body -> consumedRecords.add(body)).log("consumed<< ");
    }

    private void ifIsFifthRecordThrowException(Exchange e) {
        if (e.getMessage().getBody().equals("5")) {
            throw new RuntimeException("ERROR_TRIGGERED_BY_TEST");
        }
    }

    private void createProducerRoute(RouteBuilder builder) {
        final DirectEndpointBuilder mockKafkaProducer = direct("mockKafkaProducer");
        kafkaProducer.setDefaultEndpoint(mockKafkaProducer.resolve(camelContext));

        builder.from(mockKafkaProducer).to(kafkaTestTopic());
    }

    private KafkaEndpointBuilder kafkaTestTopic() {
        return kafka(kafkaTopicName).brokers(kafkaBrokerAddress);
    }
}
