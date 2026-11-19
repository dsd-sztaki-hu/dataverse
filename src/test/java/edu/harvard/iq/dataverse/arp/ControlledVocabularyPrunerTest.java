package edu.harvard.iq.dataverse.arp;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ControlledVocabularyPrunerTest {

    private static final List<ControlledVocabularyPruner.Value> LANGUAGES = List.of(
            new ControlledVocabularyPruner.Value(1L, "Hungarian"),
            new ControlledVocabularyPruner.Value(2L, "Hungarian Sign Language"),
            new ControlledVocabularyPruner.Value(3L, "Zulu"),
            new ControlledVocabularyPruner.Value(4L, "Ainu (Japan)"),
            new ControlledVocabularyPruner.Value(5L, "Not applicable"));

    @Test
    public void removesTheValuesThatAreNotInTheTemplate() {
        ControlledVocabularyPruner.Plan plan = ControlledVocabularyPruner.plan(
                LANGUAGES, Set.of("Hungarian", "Zulu", "Not applicable"), Set.of());

        assertEquals(List.of(2L, 4L), plan.remove());
        assertTrue(plan.keptInUse().isEmpty());
    }

    @Test
    public void keepsTheUsedValuesThatAreNotInTheTemplate() {
        ControlledVocabularyPruner.Plan plan = ControlledVocabularyPruner.plan(
                LANGUAGES, Set.of("Hungarian", "Zulu", "Not applicable"), Set.of(1L, 4L, 2L));

        assertTrue(plan.remove().isEmpty());
        assertEquals(List.of("Ainu (Japan)", "Hungarian Sign Language"), plan.keptInUse());
    }

    @Test
    public void removesNothingWhenEveryValueIsInTheTemplate() {
        ControlledVocabularyPruner.Plan plan = ControlledVocabularyPruner.plan(
                LANGUAGES,
                Set.of("Hungarian", "Hungarian Sign Language", "Zulu", "Ainu (Japan)", "Not applicable"),
                Set.of());

        assertTrue(plan.remove().isEmpty());
        assertTrue(plan.keptInUse().isEmpty());
    }

    @Test
    public void labelsMatchExactly() {
        ControlledVocabularyPruner.Plan plan = ControlledVocabularyPruner.plan(
                LANGUAGES, Set.of("hungarian", "Hungarian ", "Zulu", "Not applicable", "Hungarian Sign Language", "Ainu (Japan)"),
                Set.of());

        assertEquals(List.of(1L), plan.remove());
    }

    @Test
    public void reportsOnlyFieldsWhereSomethingHappened() {
        assertFalse(new ControlledVocabularyPruner.Result("language", 0, List.of()).changedAnything());
        assertTrue(new ControlledVocabularyPruner.Result("language", 7493, List.of()).changedAnything());
        assertTrue(new ControlledVocabularyPruner.Result("language", 0, List.of("Hungarian Sign Language")).changedAnything());
    }
}
