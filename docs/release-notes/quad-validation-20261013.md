# Quad application correctness and diagnostics

FASTA lookup now gives exact source identifiers precedence over positional aliases.
Previously, a FASTA with equal-length records in another order could silently map
one contig to the wrong sequence. Positional aliases remain available only when
none of the source identifiers overlap. Duplicate identifiers fail explicitly.
Compatibility checks use the assembly's source intervals, so valid reordered,
reversed, split and subset assemblies no longer emit spurious mismatch warnings.
The response adds `sourceIntervalsCovered`; existing fields are retained.

Display names reject whitespace/control characters before mutation, because AGP
and FASTA use whitespace-delimited identifiers. Contig and scaffold names remain
independent. Invalid HTTP names return 400, without an internal-error stack trace.

1D track indexing is a pending state with a progress message, not an error. NaN
weight summaries are recorded once per map instead of a warning at every resolution.
Console logging uses a bounded asynchronous queue with no severity-based dropping.
It applies backpressure if all 8192 slots fill; it is asynchronous under normal
load, not guaranteed nonblocking under overload. Normal shutdown allows five
seconds to drain; abrupt termination or a permanently blocked output may lose
pending diagnostics. This policy favors preserving errors over silent dropping.

Regression tests cover reordered equal-length FASTA contents, partial source
intervals, invalid and duplicate identifiers, atomic name rejection, and slow
logging sinks. The separate quad integration oracle checks all ten supplied AGPs,
both subset policies, matrix permutations and exported FASTA bases/gaps.

Native-library extraction and loading now run on a Vert.x worker before server
readiness, rather than in a class initializer on the event loop. Initialization
failures fail deployment. The startup regression checks worker execution, event
loop responsiveness during a blocked loader, and propagated failures.
