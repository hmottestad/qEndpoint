package com.the_qa_company.qendpoint.benchmarks;

import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.enums.TripleComponentOrder;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.hdt.HDTVersion;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.tools.RDF2HDT;
import com.the_qa_company.qendpoint.core.triples.impl.BitmapTriplesIndexFile;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class NtGzToHdtAndIndexesBenchmark {

	@State(Scope.Benchmark)
	public static class BenchmarkState {
		@Param({ "indexing/datagovbe-valid.nt.gz", "indexing/datagovbe-valid.nt" })
		public String ntGzPath;

		@Param({ "true" })
		public boolean ntSimpleParser;

		@Param({ "http://example.org/#" })
		public String baseUri;

		@Param({ "spo,sop,pso,pos,osp,ops" })
		public String indexOrders;

		Path resolvedNtGzPath;

		Path invocationDir;
		Path outHdt;
		Path genWorkDir;

		@Setup(Level.Trial)
		public void setupTrial() throws IOException {
			this.resolvedNtGzPath = resolveOrPreparePath(ntGzPath);
		}

		@Setup(Level.Invocation)
		public void setupInvocation() throws IOException {
			this.invocationDir = Files.createTempDirectory("qendpoint-jmh-ntgz");
			this.outHdt = invocationDir.resolve("dataset.hdt");
			this.genWorkDir = invocationDir.resolve("gen-workdir");
//			System.out.println("Benchmark invocation working dir: " + invocationDir);
		}

		@TearDown(Level.Invocation)
		public void tearDownInvocation() throws IOException {
			deleteRecursively(invocationDir);
		}

		HDTOptions hdtOptions() {
			HDTOptions spec = HDTOptions.of();
			spec.set(HDTOptionsKeys.LOADER_TYPE_KEY, HDTOptionsKeys.LOADER_TYPE_VALUE_DISK);
			spec.set(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY, genWorkDir);
			spec.set(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY, outHdt);

			// spec.set(HDTOptionsKeys.NT_SIMPLE_PARSER_KEY,
			// Boolean.toString(ntSimpleParser));

			spec.set(HDTOptionsKeys.TRIPLE_ORDER_KEY, TripleComponentOrder.SPO);
			spec.set(HDTOptionsKeys.BITMAPTRIPLES_INDEX_OTHERS, indexOrders);
			spec.set(HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_KEY,
					HDTOptionsKeys.BITMAPTRIPLES_INDEX_METHOD_VALUE_OPTIMIZED);

			return spec;
		}

		private static Path resolveOrPreparePath(String pathString) throws IOException {
			Path input = Path.of(pathString);
			if (input.isAbsolute() && Files.exists(input)) {
				return input.normalize();
			}

			Path cwd = Path.of("").toAbsolutePath();
			Path fromCwd = cwd.resolve(input).normalize();
			if (Files.exists(fromCwd)) {
				return fromCwd;
			}

			Path repoRoot = findRepoRoot(cwd);
			if (repoRoot != null) {
				Path fromRepo = repoRoot.resolve(input).normalize();
				if (Files.exists(fromRepo)) {
					return fromRepo;
				}

				if (input.normalize().equals(Path.of("indexing", "datagovbe-valid.nt.gz"))) {
					Path generated = BenchmarkDatasetFiles.ensureDatagovbeValidNtGz(repoRoot).normalize();
					if (Files.exists(generated)) {
						return generated;
					}
				}
			}

			throw new IllegalArgumentException("Input path does not exist: " + pathString + " (cwd=" + cwd
					+ (repoRoot == null ? "" : ", repoRoot=" + repoRoot) + ")");
		}

		private static Path findRepoRoot(Path start) {
			Path cur = start;
			while (cur != null) {
				if (Files.isDirectory(cur.resolve(".git")) && Files.isRegularFile(cur.resolve("pom.xml"))) {
					return cur;
				}
				cur = cur.getParent();
			}
			return null;
		}

		private static void deleteRecursively(Path root) throws IOException {
			if (root == null || !Files.exists(root)) {
				return;
			}
			Files.walkFileTree(root, new SimpleFileVisitor<>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					Files.deleteIfExists(file);
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					Files.deleteIfExists(dir);
					return FileVisitResult.CONTINUE;
				}
			});
		}
	}

	public static void main(String[] args) throws Exception {
		Options opt = new OptionsBuilder().include(NtGzToHdtAndIndexesBenchmark.class.getName()).forks(0).build();

		new Runner(opt).run();

	}

	@Benchmark
	public long ntGzToHdtAndIndexes(BenchmarkState state) throws Exception {
		HDTOptions spec = state.hdtOptions();

		try (HDT hdt = HDTManager.generateHDT(state.resolvedNtGzPath.toString(), state.baseUri, RDFNotation.NTRIPLES,
				spec, ProgressListener.ignore())) {
			// MapOnCallHDT only maps on first access; index creation assumes a
			// mapped HDT instance.
			hdt.getTriples();
			HDTManager.indexedHDT(hdt, ProgressListener.ignore(), spec);
		}

		long l = totalBytes(state.outHdt);
		if (l != 24585527) {
			throw new IllegalStateException("Unexpected total size: " + l);
		}
		return l;
	}

	@Benchmark
	public long ntGzToHdtAndIndexesRDF2HDT(BenchmarkState state) throws Exception {
		String[] args = { "-index", "-multithread", "-disk", "-quiet", "-base", state.baseUri,
				state.resolvedNtGzPath.toString(), state.outHdt.toString() };
		try {
			RDF2HDT.main(args);
		} catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}

		return 1;
	}

	private static long totalBytes(Path hdtPath) throws IOException {
		long total = Files.size(hdtPath);

		Path foqIndex = hdtPath.resolveSibling(hdtPath.getFileName() + HDTVersion.get_index_suffix("-"));
		if (!Files.exists(foqIndex)) {
			throw new IOException("Missing FOQ index file: " + foqIndex);
		}
		total += Files.size(foqIndex);

		for (TripleComponentOrder order : TripleComponentOrder.values()) {
			if (order == TripleComponentOrder.Unknown || order == TripleComponentOrder.SPO) {
				continue;
			}
			Path idx = BitmapTriplesIndexFile.getIndexPath(hdtPath, order);
			if (!Files.exists(idx)) {
				throw new IOException("Missing index file for order " + order + ": " + idx);
			}
			total += Files.size(idx);
		}

		return total;
	}
}
