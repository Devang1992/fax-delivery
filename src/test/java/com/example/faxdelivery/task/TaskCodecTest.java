package com.example.faxdelivery.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

class TaskCodecTest {

    private final TaskCodec codec = new TaskCodec(JsonMapper.builder().build());

    @Test
    void roundTrip() {
        FaxTask task = FaxTask.newSubmit("MID-1", new Origin("src", 2, 99L, "k", 1L), Map.of("MID", "MID-1"), "{}",
                1L, 2L, 3L).retryAt(10L, false, false, "reason=X");
        assertThat(codec.decode(codec.encode(task))).isEqualTo(task);
    }

    @Test
    void rejectsGarbageAndUnknownVersions() {
        assertThatThrownBy(() -> codec.decode("not json")).isInstanceOf(InvalidTaskException.class);
        assertThatThrownBy(() -> codec.decode("{\"version\":99,\"stage\":\"SUBMIT\"}"))
                .isInstanceOf(InvalidTaskException.class);
    }
}
