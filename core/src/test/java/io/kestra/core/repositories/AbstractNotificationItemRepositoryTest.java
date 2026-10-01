package io.kestra.core.repositories;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.notifications.NotificationItem;
import io.kestra.core.models.notifications.NotificationItemOutcome;
import io.kestra.core.repositories.NotificationItemRepositoryInterface.TenantOperationId;
import io.kestra.core.utils.TestsUtils;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public abstract class AbstractNotificationItemRepositoryTest {
    @Inject
    private NotificationItemRepositoryInterface notificationItemRepository;

    private static NotificationItem item(String operationId, String resourceId, String tenantId) {
        return NotificationItem.builder()
            .notificationId("notif-" + operationId)
            .operationId(operationId)
            .tenantId(tenantId)
            .resourceId(resourceId)
            .created(Instant.now())
            .build();
    }

    @Test
    void create() {
        String operationId = TestsUtils.randomString(this.getClass().getSimpleName());
        List<NotificationItem> created = notificationItemRepository.create(
            List.of(item(operationId, "res-1", "tenantA"), item(operationId, "res-2", "tenantA"))
        );

        assertThat(created).hasSize(2);
        assertThat(notificationItemRepository.countByOperationId("tenantA", operationId).get(NotificationItemOutcome.PENDING)).isEqualTo(2L);
    }

    @Test
    void create_reinsertingSameNotificationAndResourceOverwritesRatherThanDuplicates() {
        String operationId = TestsUtils.randomString(this.getClass().getSimpleName());
        notificationItemRepository.create(List.of(item(operationId, "res-1", "tenantA")));
        notificationItemRepository.create(List.of(item(operationId, "res-1", "tenantA")));

        assertThat(notificationItemRepository.countByOperationId("tenantA", operationId).get(NotificationItemOutcome.PENDING)).isEqualTo(1L);
    }

    @Test
    void updateOutcome() {
        String operationId = TestsUtils.randomString(this.getClass().getSimpleName());
        NotificationItem created = item(operationId, "res-1", "tenantA");
        notificationItemRepository.create(List.of(created));

        assertThat(notificationItemRepository.updateOutcome(operationId, "res-1", NotificationItemOutcome.SUCCEEDED)).isTrue();

        Map<NotificationItemOutcome, Long> counts = notificationItemRepository.countByOperationId("tenantA", operationId);
        assertThat(counts.get(NotificationItemOutcome.SUCCEEDED)).isEqualTo(1L);
        assertThat(counts.getOrDefault(NotificationItemOutcome.PENDING, 0L)).isEqualTo(0L);

        // unknown (operationId, resourceId): no-op, returns false
        assertThat(notificationItemRepository.updateOutcome(operationId, "unknown-resource", NotificationItemOutcome.FAILED)).isFalse();
    }

    @Test
    void countByOperationId_talliesByOutcome() {
        String operationId = TestsUtils.randomString(this.getClass().getSimpleName());
        notificationItemRepository.create(
            List.of(
                item(operationId, "res-1", "tenantA"),
                item(operationId, "res-2", "tenantA"),
                item(operationId, "res-3", "tenantA")
            )
        );

        notificationItemRepository.updateOutcome(operationId, "res-1", NotificationItemOutcome.SUCCEEDED);
        notificationItemRepository.updateOutcome(operationId, "res-2", NotificationItemOutcome.FAILED);

        Map<NotificationItemOutcome, Long> counts = notificationItemRepository.countByOperationId("tenantA", operationId);
        assertThat(counts.get(NotificationItemOutcome.SUCCEEDED)).isEqualTo(1L);
        assertThat(counts.get(NotificationItemOutcome.FAILED)).isEqualTo(1L);
        assertThat(counts.get(NotificationItemOutcome.PENDING)).isEqualTo(1L);
    }

    @Test
    void deleteByOperationIds() {
        String keep = TestsUtils.randomString(this.getClass().getSimpleName());
        String purge1 = TestsUtils.randomString(this.getClass().getSimpleName());
        String purge2 = TestsUtils.randomString(this.getClass().getSimpleName());
        notificationItemRepository.create(List.of(item(keep, "res-1", "tenantA")));
        notificationItemRepository.create(List.of(item(purge1, "res-1", "tenantA")));
        notificationItemRepository.create(List.of(item(purge2, "res-1", "tenantA")));

        int deleted = notificationItemRepository.deleteByOperationIds(
            List.of(
                new TenantOperationId("tenantA", purge1),
                new TenantOperationId("tenantA", purge2)
            )
        );

        assertThat(deleted).isEqualTo(2);
        assertThat(notificationItemRepository.countByOperationId("tenantA", keep)).isNotEmpty();
        assertThat(notificationItemRepository.countByOperationId("tenantA", purge1)).isEmpty();
        assertThat(notificationItemRepository.countByOperationId("tenantA", purge2)).isEmpty();
    }

    @Test
    void deleteByOperationIds_emptyListIsNoOp() {
        assertThat(notificationItemRepository.deleteByOperationIds(List.of())).isEqualTo(0);
    }
}
