package com.redhat.cajun.navy.responder.message;

public class RespondersCreatedEvent {

    private RespondersCreatedEvent() {};

    private int created;

    private Long[] responders;

    public int getCreated() {
        return created;
    }

    public Long[] getResponders() {
        return responders;
    }

    public static class Builder {

        private final RespondersCreatedEvent event;

        public Builder(Long[] responders) {
            event = new RespondersCreatedEvent();
            event.responders = responders;
            event.created = responders.length;
        }

        public RespondersCreatedEvent build() {
            return event;
        }
    }

}
