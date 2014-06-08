pfind
=====

Parallel (multi-threaded) Unix/Linux 'find' utility

Similar to Linux / Unix 'find', pfind.pl searches for files in a directory hierarchy.
Unlike Linux / Unix 'find', pfind.pl uses multiple threads (8 by default) to parallelize the search.

This may become usefull when searching filesystems with high-latency e.g., NFS or AFS, 
by accelerating the search through hiding the latency of individual I/O operations.

pfind.pl is not 100% compatible with Linux / Unix 'find' utility.
Neither in the flags it supports, nor in the order of the listing it generates. 

But common search options like '-type f' or '-type d' are supported and tested (not verified), 
to generate similar listing to that of 'find.

Type ./pfind.pl --help for usage information.

Edi Shmueli, 2013.