package ru.itmo.ctlab.hict.hict_library.domain;

import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.util.List;

public record AssemblyInfo(@NotNull List<ContigTree.@NotNull ContigTuple> contigs,
                           @NotNull List<ScaffoldTree.@NotNull ScaffoldTuple> scaffolds) {

}
