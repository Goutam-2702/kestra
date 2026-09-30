package io.kestra.core.services;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.kestra.core.async.AsyncOperationProcessedEvent;
import io.kestra.core.async.AsyncOperationProcessedEvent.Outcome;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.lock.LockService;
import io.kestra.core.models.notifications.CoreNotificationType;
import io.kestra.core.models.notifications.Notification;
import io.kestra.core.queues.BroadcastQueueInterface;
import io.kestra.core.repositories.NotificationRepositoryInterface;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@KestraTest
class AsyncOperationAggregatorTest {

    @Inject
    private AsyncOperationAggregator aggregator;

    @Inject
    private BroadcastQueueInterface<AsyncOperationProcessedEvent> asyncOperationQueue;

    @Inject
    private NotificationService notificationService;

    @Inject
    private NotificationRepositoryInterface notificationRepository;

    @Inject
    private LockService lockService;

    @Test
    void shouldIncrementCountersWhenProcessedEventsArrive() throws Exception {
        assertThat(aggregator).isNotNull();

        String userId = TestsUtils.randomString(this.getClass().getSimpleName());
        String operationId = TestsUtils.randomString(this.getClass().getSimpleName());
        notificationService.notify(userId, "tenantA", CoreNotificationType.ASYNC_OPERATION, "title", operationId, 3);

        asyncOperationQueue.emit(new AsyncOperationProcessedEvent(operationId, "tenantA", "item-1", Outcome.SUCCEEDED, null, Instant.now()));
        asyncOperationQueue.emit(new AsyncOperationProcessedEvent(operationId, "tenantA", "item-2", Outcome.SUCCEEDED, null, Instant.now()));
        asyncOperationQueue.emit(new AsyncOperationProcessedEvent(operationId, "tenantA", "item-3", Outcome.FAILED, "boom", Instant.now()));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        {
            Notification reloaded = notificationRepository.findByOperationId(operationId).orElseThrow();
            assertThat(reloaded.getSucceededItems()).isEqualTo(2);
            assertThat(reloaded.getFailedItems()).isEqualTo(1);
        });
    }

    @Test
    void shouldProcessEventOnlyOnceWhenAnotherAggregatorInstanceCompetesForLeadership() throws Exception {
        // Simulates a second Executor instance in the cluster: it shares the same lock table and
        // queue, so at most one of the two can win the leader lock and subscribe.
        AsyncOperationAggregator competingInstance = new AsyncOperationAggregator(notificationService, asyncOperationQueue, lockService);
        try {
            competingInstance.tryBecomeLeaderAndSubscribe();

            String userId = TestsUtils.randomString(this.getClass().getSimpleName());
            String operationId = TestsUtils.randomString(this.getClass().getSimpleName());
            notificationService.notify(userId, "tenantA", CoreNotificationType.ASYNC_OPERATION, "title", operationId, 1);

            asyncOperationQueue.emit(new AsyncOperationProcessedEvent(operationId, "tenantA", "item-1", Outcome.SUCCEEDED, null, Instant.now()));

            // pollDelay gives a losing subscriber time to (wrongly) double-process before asserting
            // the count stayed at exactly one.
            await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() ->
            {
                Notification reloaded = notificationRepository.findByOperationId(operationId).orElseThrow();
                assertThat(reloaded.getSucceededItems()).isEqualTo(1);
            });
        } finally {
            competingInstance.shutdown();
            // Restore the shared singleton as leader so later tests are not left without a subscriber.
            aggregator.tryBecomeLeaderAndSubscribe();
        }
    }
}
