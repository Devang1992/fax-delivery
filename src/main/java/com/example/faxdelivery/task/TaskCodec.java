package com.example.faxdelivery.task;

import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/** FaxTask <-> JSON (record value). Unknown future fields are ignored; unknown versions are rejected. */
@Component
public class TaskCodec {

    private final JsonMapper jsonMapper;

    public TaskCodec(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public String encode(FaxTask task) {
        return jsonMapper.writeValueAsString(task);
    }

    public FaxTask decode(String value) {
        if (value == null || value.isBlank()) {
            throw new InvalidTaskException("empty task record", null);
        }
        FaxTask task;
        try {
            task = jsonMapper.readValue(value, FaxTask.class);
        }
        catch (JacksonException e) {
            throw new InvalidTaskException("cannot decode task: " + e.getOriginalMessage(), e);
        }
        if (task.version() != FaxTask.CURRENT_VERSION) {
            throw new InvalidTaskException("unsupported task version " + task.version(), null);
        }
        if (task.stage() == null || task.origin() == null) {
            throw new InvalidTaskException("task without stage or origin", null);
        }
        return task;
    }
}
