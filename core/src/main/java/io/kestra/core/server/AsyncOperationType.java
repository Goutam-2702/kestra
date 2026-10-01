package io.kestra.core.server;

/**
 * The kind of operation {@link io.kestra.core.services.NotificationService#notifyAsyncOperation} is notified about.
 */
public enum AsyncOperationType {
    EXECUTION_KILL(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_PAUSE(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_RESUME(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_RESTART(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_REPLAY(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_FORCE_RUN(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_UNQUEUE(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_CHANGE_STATUS(AsyncOperationType.ResourceType.EXECUTION),
    EXECUTION_SET_LABELS(AsyncOperationType.ResourceType.EXECUTION),
    TRIGGER_UNLOCK(AsyncOperationType.ResourceType.TRIGGER),
    TRIGGER_DELETE(AsyncOperationType.ResourceType.TRIGGER),
    TRIGGER_DISABLE(AsyncOperationType.ResourceType.TRIGGER),
    TRIGGER_ENABLE(AsyncOperationType.ResourceType.TRIGGER),
    BACKFILL_PAUSE(AsyncOperationType.ResourceType.TRIGGER),
    BACKFILL_RESUME(AsyncOperationType.ResourceType.TRIGGER),
    BACKFILL_DELETE(AsyncOperationType.ResourceType.TRIGGER),
    ;

    private final ResourceType resourceType;

    AsyncOperationType(ResourceType resourceType) {
        this.resourceType = resourceType;
    }

    /**
     * The kind of resource this operation type targets — an execution id or a trigger uid. A
     * backfill is addressed by the trigger uid it belongs to, hence {@code TRIGGER}.
     */
    public ResourceType resourceType() {
        return resourceType;
    }

    public enum ResourceType {
        EXECUTION,
        TRIGGER
    }
}
