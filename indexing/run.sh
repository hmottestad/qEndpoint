#!/bin/bash

export JAVA_OPTIONS="-Xms16G -Xmx16G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"


#File converted in ..... 3 min 42 sec 284 ms 302 us
#Total Triples ......... 179203844
#Different subjects .... 25554000
#Different predicates .. 2337
#Different objects ..... 42155144
#Common Subject/Object . 24163053
#Index generated and saved in 149 ms 994 us
#[main] . [##########] 100.0 done
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread latest-lexemes.nt latest-lexemes.hdt



#File converted in ..... 3 min 40 sec 281 ms 501 us
#Total Triples ......... 179203844
#Different subjects .... 25554000
#Different predicates .. 2337
#Different objects ..... 42155144
#Common Subject/Object . 24163053
#[main] . [##########] 100.0 done
#Index generated and saved in 151 ms 346 us
#time ./qendpoint-cli-2.4.1-pair/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread latest-lexemes.nt.gz latest-lexemes.hdt




#File converted in ..... 3 min 13 sec 814 ms 582 us
#[main] . [##########] 100.0 done
#Total Triples ......... 179203844
#Different subjects .... 25554000
#Different predicates .. 2337
#Different objects ..... 42155144
#Common Subject/Object . 24163053
#Index generated and saved in 142 ms 792 us
#
#real    3m14.524s
#user    20m13.988s
#sys     0m15.060s
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread latest-lexemes.nt.gz latest-lexemes.hdt



#File converted in ..... 13 hour 40 min 58 sec 707 ms 838 us
#Total Triples ......... 7919157949
#Different subjects .... 235696265
#Different predicates .. 12124
#Different objects ..... 1617604632
#Common Subject/Object . 117929189


#File converted in ..... 2 hour 7 min 41 sec 19 ms 703 us
#Total Triples ......... 7919157949
#Different subjects .... 235696265
#Different predicates .. 12124
#Different objects ..... 1617604632
#Common Subject/Object . 117929189
#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/latest-truthy.nt.gz  latest-truthy.hdt




#File converted in ..... 2 hour 6 min 38 sec 67 ms 762 us
#Total Triples ......... 7919157949
#Different subjects .... 235696265
#Different predicates .. 12124
#Different objects ..... 1617604632
#Common Subject/Object . 117929189
#
#23:24:42.890 [main] INFO  c.t.qendpoint.core.hdt.impl.HDTImpl - Index generated and saved in 34 min 51 sec 53 ms 648 us
#Index generated and saved in 34 min 51 sec 59 ms 561 us#
#real    161m37.845s
#user    649m25.706s
#sys     29m53.606s
#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -disk -index -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/latest-truthy.nt.gz  latest-truthy.hdt


#File converted in ..... 6 min 14 sec 341 ms 86 us
#[main] . [##########] 100.0 done
#Total Triples ......... 499706958
#Different subjects .... 11412855
#Different predicates .. 10681
#Different objects ..... 115723374
#Common Subject/Object . 5709760
#Index generated and saved in 1 min 41 sec 838 ms 901 us
export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+UseCompactObjectHeaders"
time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/first_500M_lines_truthy.nt.gz first_500M_lines_truthy.hdt



#File converted in ..... 5 hour 27 min 57 sec 195 ms 782 us
#Total Triples ......... 20 754 779 933
#Different subjects .... 2 276 125 108
#Different predicates .. 60 661
#Different objects ..... 3 753 832 879
#Common Subject/Object . 2 056 458 644
#export JAVA_OPTIONS="-Xms64G -Xmx64G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+UseCompactObjectHeaders"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/wikidata-20251208-all-BETA.nt.gz wikidata-20251208-all-BETA.hdt
