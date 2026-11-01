package com.example.faxdelivery.rightfax;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

class RightFaxResponseReaderTest {

    private final RightFaxResponseReader reader = new RightFaxResponseReader(JsonMapper.builder().build());

    @Test
    void readsPointersArraysAndBareStrings() {
        String upload = "[\"https://rf/Attachments/abc\"]";
        assertThat(reader.text(upload, reader.parse(upload), "/0")).isEqualTo("https://rf/Attachments/abc");

        String job = "{\"Id\":\"123\",\"Condition\":\"Succeeded\"}";
        assertThat(reader.text(job, reader.parse(job), "/Id")).isEqualTo("123");

        String bare = "\"https://rf/Attachments/xyz\"";
        assertThat(reader.text(bare, reader.parse(bare), "")).isEqualTo("https://rf/Attachments/xyz");

        String docs = "{\"Items\":[{\"Id\":\"d1\",\"Condition\":\"Failed\"},{\"Id\":\"d2\"}]}";
        assertThat(reader.items(reader.parse(docs), "/Items")).hasSize(2);
        assertThat(RightFaxResponseReader.field(reader.items(reader.parse(docs), "/Items").get(0), "Condition"))
                .isEqualTo("Failed");
    }

    @Test
    void missingValuesAreNull() {
        String body = "{\"Other\":1}";
        assertThat(reader.text(body, reader.parse(body), "/Id")).isNull();
        assertThat(reader.items(reader.parse("not json"), "/Items")).isEmpty();
    }
}
