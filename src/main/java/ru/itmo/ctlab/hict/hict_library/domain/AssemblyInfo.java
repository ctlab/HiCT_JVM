package ru.itmo.ctlab.hict.hict_library.domain;

import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.trees.ContigTree;
import ru.itmo.ctlab.hict.hict_library.trees.ScaffoldTree;

import java.util.List;

public record AssemblyInfo(@NotNull @NonNull List<ContigTree.@NotNull @NonNull ContigTuple> contigs,
                           @NotNull @NonNull List<ScaffoldTree.@NotNull @NonNull ScaffoldTuple> scaffolds) {

}
