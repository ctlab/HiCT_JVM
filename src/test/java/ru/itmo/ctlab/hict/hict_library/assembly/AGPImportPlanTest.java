package ru.itmo.ctlab.hict.hict_library.assembly;

import org.junit.jupiter.api.Test;
import ru.itmo.ctlab.hict.hict_library.domain.*;
import java.io.StringReader;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static ru.itmo.ctlab.hict.hict_library.assembly.AGPImportPlan.MissingComponentsPolicy.*;

class AGPImportPlanTest {
  final long[] resolutions = {0, 10, 100};
  ContigDescriptor source() {
    var stripe = new StripeDescriptor(0, 32, new double[32]);
    return new ContigDescriptor(0, "ctg", 95, List.of(10L, 1L),
      List.of(ContigHideType.SHOWN, ContigHideType.SHOWN), List.of(
        List.of(new ATUDescriptor(stripe, 0, 4, ATUDirection.FORWARD), new ATUDescriptor(stripe, 4, 10, ATUDirection.REVERSED)),
        List.of(new ATUDescriptor(stripe, 20, 21, ATUDirection.FORWARD))), null, 0);
  }
  List<Integer> bins(ContigDescriptor d, int order) {
    var result = new ArrayList<Integer>();
    for (var atu : d.getAtus().get(order)) {
      if (atu.getDirection() == ATUDirection.FORWARD)
        for (int i=atu.getStartIndexInStripeIncl(); i<atu.getEndIndexInStripeExcl(); i++) result.add(i);
      else for (int i=atu.getEndIndexInStripeExcl()-1; i>=atu.getStartIndexInStripeIncl(); i--) result.add(i);
    }
    return result;
  }
  @Test void everyCutPreservesAllBinsIncludingReverseAtusAndZeroBinChildren() {
    var original=source();
    for (int cut=1; cut<95; cut++) {
      var left=AGPImportPlan.slice(original,0,cut,1,resolutions);
      var right=AGPImportPlan.slice(original,cut,95,2,resolutions);
      assertEquals(95,left.getLengthBp()+right.getLengthBp());
      assertEquals(cut,right.getOffsetInSourceFASTA());
      for(int r=1;r<3;r++) {
        var actual=new ArrayList<>(bins(left,r));actual.addAll(bins(right,r));
        assertEquals(bins(original,r),actual,"Cut "+cut+" at "+resolutions[r]);
        assertEquals((cut+resolutions[r]-1)/resolutions[r],left.getLengthBinsAtResolution()[r]);
      }
    }
  }
  @Test void slicingAnExistingUnalignedPieceKeepsTheOriginalBinGrid() {
    var original=source();
    var child=AGPImportPlan.slice(original,44,95,1,resolutions);
    var nested=AGPImportPlan.slice(child,11,51,2,resolutions);
    var direct=AGPImportPlan.slice(original,55,95,3,resolutions);
    assertEquals(55,nested.getOffsetInSourceFASTA());
    for(int r=1;r<3;r++) assertEquals(bins(direct,r),bins(nested,r));
  }
  @Test void partialComponentsCanBePlacedOnDifferentScaffoldsInReverse() throws Exception {
    var records=AGPProcessor.parseRecordsFromReader(new StringReader("a 1 44 1 W ctg 1 44 +\nb 1 51 1 W ctg 45 95 -"));
    var source=source();
    var plan=AGPImportPlan.prepare(records,List.of(source),n->source,resolutions,1,REQUIRE_CHOICE);
    assertFalse(plan.report().requiresChoice());assertEquals(1,plan.report().splitSourceContigs());
    assertEquals(0,plan.report().omittedBp());assertEquals(ContigDirection.REVERSED,plan.selected().get(1).direction());
    assertEquals(List.of(0,1,2,3,9),bins(plan.selected().get(0).descriptor(),1));
    assertEquals(List.of(8,7,6,5,4),bins(plan.selected().get(1).descriptor(),1));
    assertTrue(plan.report().warnings().get(0).contains("No contact bin was removed"));
  }
  @Test void missingIntervalsRequireAnExplicitChoiceAndRetainTheComplement() throws Exception {
    var records=AGPProcessor.parseRecordsFromReader(new StringReader("a 1 30 1 W ctg 21 50 +"));var source=source();
    for(var policy:AGPImportPlan.MissingComponentsPolicy.values()) {
      var plan=AGPImportPlan.prepare(records,List.of(source),n->source,resolutions,1,policy);
      assertEquals(65,plan.report().omittedBp());assertEquals(2,plan.omitted().size());
      assertEquals(policy==REQUIRE_CHOICE,plan.report().requiresChoice());
      assertEquals(List.of(20L,45L),plan.omitted().stream().map(c->c.descriptor().getLengthBp()).toList());
    }
  }
  @Test void overlappingAliasedComponentsAndOutOfBoundsIntervalsAreRejected() throws Exception {
    var source=source();
    for(var text:List.of("a 1 50 1 W ctg 1 50 +\nb 1 51 1 W alias 45 95 -", "a 1 96 1 W ctg 1 96 +")) {
      var records=AGPProcessor.parseRecordsFromReader(new StringReader(text));
      assertThrows(IllegalArgumentException.class,()->AGPImportPlan.prepare(records,List.of(source),n->source,resolutions,1,EXCLUDE));
    }
  }
  @Test void explicitUnscaffoldedSingletonsNeedAChoiceButOrdinarySingletonScaffoldsDoNot() throws Exception {
    var source = source();
    for (String name : List.of("scf_ctg", "unscaffolded_ctg")) {
      var records = AGPProcessor.parseRecordsFromReader(new StringReader(name + " 1 95 1 W ctg 1 95 -"));
      var preview = AGPImportPlan.prepare(records,List.of(source),n->source,resolutions,1,REQUIRE_CHOICE);
      assertEquals(name.startsWith("unscaffolded_"),preview.report().requiresChoice());
      assertEquals(0,preview.report().omittedBp());
      var selected = AGPImportPlan.prepare(records,List.of(source),n->source,resolutions,1,SCAFFOLDED_ONLY);
      assertEquals(name.startsWith("unscaffolded_") ? 0 : 1,selected.selected().size());
      var full = AGPImportPlan.prepare(records,List.of(source),n->source,resolutions,1,APPEND_UNPLACED);
      assertEquals(1,full.selected().size());assertEquals(ContigDirection.REVERSED,full.selected().getFirst().direction());
    }
    var invalid=AGPProcessor.parseRecordsFromReader(new StringReader("unscaffolded_a 1 95 1 W ctg 1 95 +\nunscaffolded_b 1 95 1 W ctg 1 95 +"));
    assertThrows(IllegalArgumentException.class,()->AGPImportPlan.prepare(invalid,List.of(source),n->source,resolutions,1,SCAFFOLDED_ONLY), "Excluded records must still be validated");
  }
}
