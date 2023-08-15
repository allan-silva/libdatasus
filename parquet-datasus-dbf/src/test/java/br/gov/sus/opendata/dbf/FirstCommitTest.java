package br.gov.sus.opendata.dbf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FirstCommitTest {

    @Test
    void add() {
        FirstCommit f = new FirstCommit();
        assertEquals(42, f.add(40, 2));
    }
}