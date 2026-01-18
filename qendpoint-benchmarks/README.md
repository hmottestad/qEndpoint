# qendpoint JMH benchmarks

Build the benchmark uber-jar:

```bash
mvn -o -Dmaven.repo.local=.m2_repo -pl qendpoint-benchmarks -DskipTests package
```

Run the NT.GZ → HDT + indexes benchmark (override `ntGzPath` as needed):

```bash
java -jar qendpoint-benchmarks/target/benchmarks.jar NtGzToHdtAndIndexesBenchmark
```

This benchmark supports toggling the pull-based NT/NQ disk pipeline via `parser.ntSimpleParser`:

```bash
java -jar qendpoint-benchmarks/target/benchmarks.jar NtGzToHdtAndIndexesBenchmark \
  -p ntSimpleParser=true
```

By default it uses `indexing/datagovbe-valid.nt.gz`. If that file does not exist but
`indexing/datagovbe-valid.ttl` does, it will generate the `.nt.gz` once during JMH setup.

Override `ntGzPath` as needed:

```bash
java -jar qendpoint-benchmarks/target/benchmarks.jar NtGzToHdtAndIndexesBenchmark \
  -p ntGzPath=/absolute/path/to/dataset.nt.gz
```
