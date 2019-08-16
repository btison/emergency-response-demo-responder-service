package com.redhat.cajun.navy.responder.message;

public class RespondersDeletedEvent {

    private RespondersDeletedEvent() {};

    private int deleted;

    private Long[] responders;

    public int getDeleted() {
        return deleted;
    }

    public Long[] getResponders() {
        return responders;
    }

    public static class Builder {

        private final RespondersDeletedEvent event;

        public Builder(Long[] responders) {
            event = new RespondersDeletedEvent();
            event.responders = responders;
            event.deleted = responders.length;
        }

        public RespondersDeletedEvent build() {
            return event;
        }
    }

}
