package ru.itmo.ctlab.hict.hict_server.dto;


import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import ru.itmo.ctlab.hict.hict_library.domain.ScaffoldDescriptor;

import java.util.List;

public record AssemblyInfoDTO(@NotNull @NonNull List<@NotNull @NonNull ContigDescriptorDTO> contigDescriptors,
                              @NotNull @NonNull List<@NotNull @NonNull ScaffoldDescriptor> scaffoldDescriptors) {
}
