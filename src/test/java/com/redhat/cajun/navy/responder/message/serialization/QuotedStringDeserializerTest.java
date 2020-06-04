package com.redhat.cajun.navy.responder.message.serialization;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class QuotedStringDeserializerTest {

    @Test
    public void testUnquoted() {

        String json = "{\"messageType\":\"IncidentReportedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\": {\"id\":\"incident123\"," +
                "\"lat\": \"34.14338\"," +
                "\"lon\": \"-77.86569\"," +
                "\"numberOfPeople\": 3," +
                "\"medicalNeeded\": true," +
                "\"timestamp\": 1521148332350" +
                "}}";

        QuotedStringDeserializer deserializer = new QuotedStringDeserializer();
        byte[] data = json.getBytes();
        String deserialized = deserializer.deserialize("test", data);
        MatcherAssert.assertThat(deserialized, CoreMatchers.equalTo(json));

    }

    @Test
    public void testQuoted() {
        String json = "\"{\\\"messageType\\\":\\\"IncidentReportedEvent\\\"," +
                "\\\"id\\\":\\\"messageId\\\"," +
                "\\\"invokingService\\\":\\\"messageSender\\\"," +
                "\\\"timestamp\\\":1521148332397," +
                "\\\"body\\\": {\\\"id\":\\\"incident123\\\"," +
                "\\\"lat\\\": \\\"34.14338\\\"," +
                "\\\"lon\\\": \\\"-77.86569\\\"," +
                "\\\"numberOfPeople\\\": 3," +
                "\\\"medicalNeeded\\\": true," +
                "\\\"timestamp\\\": 1521148332350" +
                "}}\"";

        String expected = "{\"messageType\":\"IncidentReportedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\": {\"id\":\"incident123\"," +
                "\"lat\": \"34.14338\"," +
                "\"lon\": \"-77.86569\"," +
                "\"numberOfPeople\": 3," +
                "\"medicalNeeded\": true," +
                "\"timestamp\": 1521148332350" +
                "}}";

        QuotedStringDeserializer deserializer = new QuotedStringDeserializer();
        byte[] data = json.getBytes();
        String deserialized = deserializer.deserialize("test", data);
        MatcherAssert.assertThat(deserialized, CoreMatchers.equalTo(expected));
    }

    @Test
    public void testQuotedUnEscaped() {

        String json = "\"{\"messageType\":\"IncidentReportedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\": {\"id\":\"incident123\"," +
                "\"lat\": \"34.14338\"," +
                "\"lon\": \"-77.86569\"," +
                "\"numberOfPeople\": 3," +
                "\"medicalNeeded\": true," +
                "\"timestamp\": 1521148332350" +
                "}}\"";

        String expected = "{\"messageType\":\"IncidentReportedEvent\"," +
                "\"id\":\"messageId\"," +
                "\"invokingService\":\"messageSender\"," +
                "\"timestamp\":1521148332397," +
                "\"body\": {\"id\":\"incident123\"," +
                "\"lat\": \"34.14338\"," +
                "\"lon\": \"-77.86569\"," +
                "\"numberOfPeople\": 3," +
                "\"medicalNeeded\": true," +
                "\"timestamp\": 1521148332350" +
                "}}";

        QuotedStringDeserializer deserializer = new QuotedStringDeserializer();
        byte[] data = json.getBytes();
        String deserialized = deserializer.deserialize("test", data);
        MatcherAssert.assertThat(deserialized, CoreMatchers.equalTo(expected));

    }

}
