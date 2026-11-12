package com.javastream.storage.log;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OffsetManagerTest {

    @Test
    void assignUndoAndRecover() {
        OffsetManager offsets = new OffsetManager();
        assertEquals(0, offsets.assignAndIncrement());
        assertEquals(1, offsets.peekNext());
        offsets.undoLastAssign();
        assertEquals(0, offsets.peekNext());

        offsets.resetFromRecovery(100_000, 100_000);
        assertEquals(100_000, offsets.logEndOffset());
        assertEquals(100_000, offsets.assignAndIncrement());
    }
}
