package io.kestra.core.services;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kestra.core.exceptions.NotFoundException;
import io.kestra.core.models.notifications.CoreNotificationType;
import io.kestra.core.models.notifications.Notification;
import io.kestra.core.models.notifications.NotificationEvent;
import io.kestra.core.models.notifications.NotificationEventType;
import io.kestra.core.models.notifications.NotificationItem;
import io.kestra.core.models.notifications.NotificationItemOutcome;
import io.kestra.core.models.notifications.NotificationType;
import io.kestra.core.queues.BroadcastQueueInterface;
import io.kestra.core.queues.QueueException;
import io.kestra.core.repositories.NotificationItemRepositoryInterface;
import io.kestra.core.repositories.NotificationRepositoryInterface;
import io.kestra.core.repositories.NotificationRepositoryInterface.NotificationCursor;
import io.kestra.core.server.AsyncOperationType;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import io.micronaut.http.sse.Event;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.util.function.Tuples;

/**
 * Write-path facade for {@link Notification}s. This is the seam producers (an EE case-management
 * feature, async-operation progress, ...) target: it writes synchronously to the repository today,
 * but callers never see the persistence mechanism, so the write path can move behind a durable
 * queue later with zero caller changes.
 * <p>
 * Every create/update also emits a {@link NotificationEvent} on {@link #notificationQueue}, so
 * consumers (e.g. a live UI bell) don't have to poll — {@link #follow(String)} is the read-path
 * counterpart, building the SSE stream on top of {@link NotificationStreamingService}.
 * <p>
 * Retention is flat and identical for every {@link NotificationType} (see {@link #purge()}) — read
 * notifications are purged after 7 days, unread after 30 days.
 */
@Slf4j
@Singleton
public class NotificationService {

    private static final int READ_RETENTION_DAYS = 7;
    private static final int UNREAD_RETENTION_DAYS = 30;
    private static final Duration ACCESSIBLE_TENANT_IDS_REFRESH_INTERVAL = Duration.ofSeconds(1);
    private static final Duration NOTIFICATION_SAMPLE_INTERVAL = Duration.ofMillis(250);

    private final NotificationRepositoryInterface notificationRepository;
    private final NotificationItemRepositoryInterface notificationItemRepository;
    private final BroadcastQueueInterface<NotificationEvent> notificationQueue;
    private final NotificationStreamingService notificationStreamingService;
    private final AccessibleTenantsProvider accessibleTenantsProvider;
    private final CurrentUserProvider currentUserProvider;
    private final TenantService tenantService;

    @Inject
    public NotificationService(
        NotificationRepositoryInterface notificationRepository,
        NotificationItemRepositoryInterface notificationItemRepository,
        BroadcastQueueInterface<NotificationEvent> notificationQueue,
        NotificationStreamingService notificationStreamingService,
        AccessibleTenantsProvider accessibleTenantsProvider,
        CurrentUserProvider currentUserProvider,
        TenantService tenantService) {
        this.notificationRepository = Objects.requireNonNull(notificationRepository, "notificationRepository must not be null");
        this.notificationItemRepository = Objects.requireNonNull(notificationItemRepository, "notificationItemRepository must not be null");
        this.notificationQueue = Objects.requireNonNull(notificationQueue, "notificationQueue must not be null");
        this.notificationStreamingService = Objects.requireNonNull(notificationStreamingService, "notificationStreamingService must not be null");
        this.accessibleTenantsProvider = Objects.requireNonNull(accessibleTenantsProvider, "accessibleTenantsProvider must not be null");
        this.currentUserProvider = Objects.requireNonNull(currentUserProvider, "currentUserProvider must not be null");
        this.tenantService = Objects.requireNonNull(tenantService, "tenantService must not be null");
    }

    /**
     * Creates a new, unread notification.
     */
    public Notification notify(String userId, @Nullable String tenantId, NotificationType type, String title, @Nullable String referenceId) {
        Notification created = createNotification(userId, tenantId, type, null, title, referenceId, false);
        emit(NotificationEventType.CREATED, created);
        return created;
    }

    /**
     * Notifies the user who submitted an async operation that it was accepted, so they can track
     * its progress from their notifications, and records one {@link NotificationItemOutcome#PENDING}
     * {@link NotificationItem} per targeted resource.
     */
    public void notifyAsyncOperation(String operationId, AsyncOperationType operationType, List<String> resourceIds) {
        currentUserProvider.currentUserId().ifPresent(userId ->
        {
            String tenantId = tenantService.resolveTenant();
            Notification created = createNotification(
                userId,
                tenantId,
                CoreNotificationType.ASYNC_OPERATION,
                operationType,
                "%s requested for %d item%s".formatted(humanize(operationType), resourceIds.size(), resourceIds.size() == 1 ? "" : "s"),
                operationId,
                !resourceIds.isEmpty()
            );

            if (!resourceIds.isEmpty()) {
                Instant now = Instant.now();
                notificationItemRepository.create(
                    resourceIds.stream()
                        .map(
                            resourceId -> NotificationItem.builder()
                                .notificationId(created.getId())
                                .operationId(operationId)
                                .tenantId(tenantId)
                                .resourceId(resourceId)
                                .created(now)
                                .build()
                        )
                        .toList()
                );
            }

            emit(NotificationEventType.CREATED, created);
        });
    }

    private static String humanize(AsyncOperationType operationType) {
        String words = operationType.name().toLowerCase().replace('_', ' ');
        return Character.toUpperCase(words.charAt(0)) + words.substring(1);
    }

    private Notification createNotification(
        String userId,
        @Nullable String tenantId,
        NotificationType type,
        @Nullable AsyncOperationType asyncOperationType,
        String title,
        @Nullable String referenceId,
        boolean read) {
        Instant now = Instant.now();

        return notificationRepository.create(
            Notification.builder()
                .id(IdUtils.create())
                .userId(userId)
                .tenantId(tenantId)
                .type(type.key())
                .asyncOperationType(asyncOperationType)
                .title(title)
                .referenceId(referenceId)
                .read(read)
                .createdDate(now)
                .updatedDate(now)
                .build()
        );
    }

    /**
     * Flips the {@link NotificationItem} tracking {@code (operationId, resourceId)} to {@code outcome}
     * and pushes an update to SSE followers, who hydrate its progress themselves (see {@link #follow(String)}).
     *
     * @throws NotFoundException if no notification matches {@code operationId}.
     */
    public void recordAsyncOperationItemOutcome(String operationId, String resourceId, NotificationItemOutcome outcome) {
        Notification notification = notificationRepository.findByOperationId(operationId)
            .orElseThrow(() -> new NotFoundException("No notification found for operationId '" + operationId + "'"));

        if (!notificationItemRepository.updateOutcome(operationId, resourceId, outcome)) {
            log.warn("No pending notification item found for operationId '{}', resourceId '{}'", operationId, resourceId);
            return;
        }

        emit(NotificationEventType.UPDATED, notification);
    }

    /**
     * Projects {@code succeededItems}/{@code failedItems}/{@code totalItems} onto the notification
     * by aggregating its {@link NotificationItem}s — see the Javadoc on those fields. A no-op for
     * anything but an {@link CoreNotificationType#ASYNC_OPERATION}.
     */
    private Notification withProgress(Notification notification) {
        if (notification.getAsyncOperationType() == null) {
            return notification;
        }

        Map<NotificationItemOutcome, Long> counts = notificationItemRepository.countByOperationId(notification.getTenantId(), notification.getReferenceId());
        long succeeded = counts.getOrDefault(NotificationItemOutcome.SUCCEEDED, 0L);
        long failed = counts.getOrDefault(NotificationItemOutcome.FAILED, 0L);
        long pending = counts.getOrDefault(NotificationItemOutcome.PENDING, 0L);

        return notification.toBuilder()
            .succeededItems((int) succeeded)
            .failedItems((int) failed)
            .totalItems((int) (succeeded + failed + pending))
            .build();
    }

    private List<Notification> withProgress(List<Notification> notifications) {
        return notifications.stream().map(this::withProgress).toList();
    }

    private void emit(NotificationEventType eventType, Notification notification) {
        try {
            notificationQueue.emit(NotificationEvent.of(eventType, notification));
        } catch (QueueException e) {
            log.error("Failed to emit NotificationEvent for notification '{}'", notification.getId(), e);
        }
    }

    public boolean markRead(String userId, String id) {
        boolean updated = notificationRepository.markRead(userId, id);
        if (updated) {
            notificationRepository.findById(userId, id).ifPresent(notification -> emit(NotificationEventType.UPDATED, notification));
        }
        return updated;
    }

    public boolean markUnread(String userId, String id) {
        boolean updated = notificationRepository.markUnread(userId, id);
        if (updated) {
            notificationRepository.findById(userId, id).ifPresent(notification -> emit(NotificationEventType.UPDATED, notification));
        }
        return updated;
    }

    public int markAllRead(String userId, Set<String> accessibleTenantIds) {
        List<Notification> updated = notificationRepository.markAllRead(userId, accessibleTenantIds);
        emit(NotificationEventType.UPDATED, updated);
        return updated.size();
    }

    /**
     * History, cursor-based, hydrated with progress (see {@link #withProgress(Notification)}).
     */
    public List<Notification> findByUser(String userId, Set<String> accessibleTenantIds, @Nullable NotificationCursor cursor, int limit) {
        return withProgress(notificationRepository.findByUser(userId, accessibleTenantIds, cursor, limit));
    }

    /**
     * Polling delta, hydrated with progress (see {@link #withProgress(Notification)}).
     */
    public List<Notification> findByUserSince(String userId, Set<String> accessibleTenantIds, Instant since) {
        return withProgress(notificationRepository.findByUserSince(userId, accessibleTenantIds, since));
    }

    /**
     * Follows live notification updates for {@code userId}, restricted to their currently
     * accessible tenants. Each event is checked against the latest value of {@link #accessibleTenantIds}.
     * Updates are sampled per notification id, so a fast-changing notification (e.g. progress ticks)
     * can't flood the stream at the expense of others.
     * <p>
     * Callers must invoke {@link FollowSubscription#unregister()} once the stream terminates.
     */
    public FollowSubscription follow(String userId) {
        String subscriberId = IdUtils.create();
        Set<String> initialAccessibleTenantIds = accessibleTenantIds(userId);

        Flux<Event<Notification>> flux = Flux.<NotificationEvent> create(
            emitter -> notificationStreamingService.registerSubscriber(userId, subscriberId, emitter),
            FluxSink.OverflowStrategy.LATEST
        )
            .doFinally(_ -> notificationStreamingService.unregisterSubscriber(userId, subscriberId))
            .withLatestFrom(upToDateAccessibleTenantsIds(userId, initialAccessibleTenantIds), Tuples::of)
            .filter(notificationAndAllowedTenants -> isAccessible(notificationAndAllowedTenants.getT2(), notificationAndAllowedTenants.getT1().notification()))
            .map(tuple -> Event.of(tuple.getT1().notification()).id(tuple.getT1().eventType().name().toLowerCase()))
            .buffer(NOTIFICATION_SAMPLE_INTERVAL)
            .flatMapIterable(this::latestNotificationUpdateById)
            .map(this::withProgress)
            .timeout(Duration.ofHours(1));

        return new FollowSubscription(flux, () -> notificationStreamingService.unregisterSubscriber(userId, subscriberId));
    }

    private Event<Notification> withProgress(Event<Notification> event) {
        return Event.of(event, withProgress(event.getData()));
    }

    private List<Event<Notification>> latestNotificationUpdateById(List<Event<Notification>> events) {
        return events.stream()
            .collect(
                Collectors.toMap(
                    event -> event.getData().getId(),
                    Function.identity(),
                    (first, last) -> last,
                    LinkedHashMap::new
                )
            )
            .values()
            .stream()
            .toList();
    }

    private Flux<Set<String>> upToDateAccessibleTenantsIds(String userId, Set<String> initialAccessibleTenantIds) {
        return Flux.interval(ACCESSIBLE_TENANT_IDS_REFRESH_INTERVAL)
            .map(_ -> accessibleTenantIds(userId))
            .startWith(initialAccessibleTenantIds);
    }

    private static boolean isAccessible(Set<String> accessibleTenantIds, Notification notification) {
        String tenantId = notification.getTenantId();
        return tenantId == null || accessibleTenantIds.contains(tenantId);
    }

    /**
     * Gets the accessible tenants for the given user.
     */
    public Set<String> accessibleTenantIds(String userId) {
        return accessibleTenantsProvider.accessibleTenantIds(userId);
    }

    /**
     * Flat-TTL retention purge: read notifications older than {@value READ_RETENTION_DAYS} days,
     * and any notification older than {@value UNREAD_RETENTION_DAYS} days regardless of read state.
     * Cascades to the purged notifications' {@link NotificationItem}s, via each async-operation
     * notification's {@code (tenantId, referenceId)}.
     *
     * @return the number of deleted notification rows.
     */
    public int purge() {
        Instant now = Instant.now();
        Instant readOlderThan = now.minus(READ_RETENTION_DAYS, ChronoUnit.DAYS);
        Instant createdOlderThan = now.minus(UNREAD_RETENTION_DAYS, ChronoUnit.DAYS);

        List<NotificationItemRepositoryInterface.TenantOperationId> operationIds = notificationRepository.findToPurge(readOlderThan, createdOlderThan).stream()
            .filter(notification -> notification.getAsyncOperationType() != null)
            .map(notification -> new NotificationItemRepositoryInterface.TenantOperationId(notification.getTenantId(), notification.getReferenceId()))
            .toList();
        if (!operationIds.isEmpty()) {
            notificationItemRepository.deleteByOperationIds(operationIds);
        }

        return notificationRepository.deleteByQuery(readOlderThan, createdOlderThan);
    }

    @SneakyThrows(QueueException.class)
    private void emit(NotificationEventType eventType, List<Notification> notifications) {
        if (notifications.isEmpty()) {
            return;
        }
        notificationQueue.emit(notifications.stream().map(notification -> NotificationEvent.of(eventType, notification)).toList());
    }

    /**
     * A live notification stream and the cleanup its caller must run once the stream terminates
     * (complete / error / cancel), to unregister the subscriber from {@link NotificationStreamingService}.
     */
    public record FollowSubscription(Flux<Event<Notification>> flux, Runnable unregister) {
    }
}
