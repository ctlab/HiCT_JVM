// Exercise the two assignment operators that reference removed BitMagic APIs
// in BLAST 2.16.0. Merely including bm.h does not fail on older compilers.
#include <util/bitset/bm.h>
#include <utility>

int main()
{
    bm::bvector<> bits;
    using iterator = bm::bvector<>::bulk_insert_iterator;
    iterator source(bits);
    source = 7;
    source = 42;
    iterator copy;
    copy = source; // Allocate a previously empty iterator's buffer.
    source.flush();
    copy.flush();

    iterator destination(bits);
    destination = std::move(source); // Release the destination's old buffer.
    destination = 100;
    destination.flush();
    source = copy; // Allocate again after transferring the buffer.
    source = 1000;
    source.flush();

    return bits.count() == 4 && bits.test(7) && bits.test(42)
        && bits.test(100) && bits.test(1000) ? 0 : 1;
}
