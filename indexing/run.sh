#!/bin/bash

#export JAVA_OPTIONS="-Xms16G -Xmx16G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread latest-lexemes.nt.gz latest-lexemes.hdt


#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -disk -index -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/latest-truthy.nt.gz  latest-truthy.hdt


#[main] . [##########] 100.0 done
#Total Triples ......... 499706958
#Different subjects .... 11412855
#Different predicates .. 10681
#Different objects ..... 115723374
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#08:33:35.743 [main] INFO  c.t.q.c.triples.impl.BitmapTriples - Count predicates in 1 sec 541 ms 121 us
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#[main] . [##########] 100.0 done
#08:33:45.935 [main] INFO  c.t.qendpoint.core.hdt.impl.HDTImpl - Index generated and saved in 1 min 34 sec 165 ms 41 us
#[main] . [##########] 100.0 done
#
#real    7m29.917s
#user    30m28.923s
#sys     1m9.650s
#export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"
export JAVA_OPTIONS="-Xms32G -Xmx32G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+UseCompactObjectHeaders"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/first_500M_lines_truthy.nt.gz first_500M_lines_truthy.hdt
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -no-recreate -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/first_500M_lines_truthy.nt.gz first_500M_lines_truthy.hdt
#

#[main] . [##########] 100.0 done
#Total Triples ......... 20754779933
#Different subjects .... 2276125108
#Different predicates .. 60661
#Different objects ..... 3753832879
#Common Subject/Object . 2056458644
#
#real    354m53.017s
#user    1272m56.135s
#sys     303m49.361s
export JAVA_OPTIONS="-Xms64G -Xmx64G -XX:+UseParallelGC -XX:+AlwaysPreTouch -XX:+DisableExplicitGC -XX:+UseCompactObjectHeaders"
#time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/wikidata-20251208-all-BETA.nt.gz wikidata-20251208-all-BETA.hdt
time ./qendpoint-cli-2.5.3/bin/rdf2hdt.sh -index -no-recreate -disk -disklocation ./temp/ -multithread /Users/havardottestad/Documents/wikidata/wikidata-20251208-all-BETA.nt.gz wikidata-20251208-all-BETA.hdt

#[main] . [          ] 0.96  object index: count objects 1986002944/20754779933 (43 035 713 items/s)
