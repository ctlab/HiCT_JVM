package ru.itmo.ctlab.hict.hict_server.tools;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HictCliApproximateExportTest {
  @Test void explicitEstimateFlagAcceptsTheDocumentedBooleanFormsAndPreservesOrdinaryDefault() {
    for (var literal : List.of("true", "yes", "1", "false", "no", "0")) {
      final var command = parse(List.of("--approximate-rebin=" + literal));
      final var options = command.toOptions(command.agpPath, command.applyAgp);
      assertEquals(List.of("true", "yes", "1").contains(literal), options.approximateRebin());
      assertFalse(options.balanceExportedCoolers());
    }
    final var bare = parse(List.of("--approximate-rebin"));
    assertTrue(bare.toOptions(bare.agpPath, bare.applyAgp).approximateRebin());
    final var ordinary = parse(List.of());
    assertFalse(ordinary.toOptions(ordinary.agpPath, ordinary.applyAgp).approximateRebin());
  }

  @Test void importCommandsCannotSilentlyIgnoreAnApproximateExportFlag() {
    final var command = new HictCli.McoolToHict();
    command.input = Path.of("source.mcool"); command.output = Path.of("result.hict.hdf5"); command.approximateRebin = true;
    final var failure = assertThrows(IllegalArgumentException.class, () -> command.toOptions("", false));
    assertTrue(failure.getMessage().contains("only for hict-to-mcool"));
  }

  private static HictCli.HictToMcool parse(List<String> flag) {
    final var args = new ArrayList<>(List.of("convert", "hict-to-mcool", "--input=source.hict.hdf5",
      "--output=result.mcool", "--agp=edited.agp", "--apply-agp", "--no-balance-exported-coolers"));
    args.addAll(flag);
    final var parsed = new CommandLine(new HictCli()).parseArgs(args.toArray(String[]::new));
    return parsed.subcommand().subcommand().commandSpec().userObject() instanceof HictCli.HictToMcool command
      ? command : null;
  }
}
