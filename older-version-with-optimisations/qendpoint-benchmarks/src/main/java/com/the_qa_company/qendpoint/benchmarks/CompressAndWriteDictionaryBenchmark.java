package com.the_qa_company.qendpoint.benchmarks;

import com.the_qa_company.qendpoint.core.dictionary.impl.CompressFourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.WriteFourSectionDictionary;
import com.the_qa_company.qendpoint.core.dictionary.impl.section.WriteDictionarySection;
import com.the_qa_company.qendpoint.core.hdt.impl.diskimport.CompressionResult;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IndexedNode;
import com.the_qa_company.qendpoint.core.util.string.ByteString;
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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Fork(value = 1, jvmArgs = { "-Xms4G", "-Xmx4G" })
@Measurement(iterations = 10)
public class CompressAndWriteDictionaryBenchmark {

	@State(Scope.Benchmark)
	public static class GeneratedData {
		@Param({ "10000000" })
		public int tripleCount;

		@Param({ "10" })
		public int duplicatesPerNode;

		@Param({ "0.25" })
		public double sharedDistinctRatio;

		@Param({ "8192" })
		public int bufferSize;

		@Param({ "NONE" })
		public String diskCompression;

		@Param({ "16" })
		public int pfcBlockSize;

		@Param({ "75" })
		public long seed;

		private HDTOptions hdtOptions;
		private IndexedNode[] subjects;
		private IndexedNode[] predicates;
		private IndexedNode[] objects;
		private ByteString[] sectionEntriesSorted;

		@Setup(Level.Trial)
		public void setupTrial() {
			int distinctPerRole = Math.max(1, tripleCount / Math.max(1, duplicatesPerNode));
			int sharedDistinct = (int) Math.round(distinctPerRole * Math.max(0.0, Math.min(1.0, sharedDistinctRatio)));
			sharedDistinct = Math.min(sharedDistinct, distinctPerRole);

			int subjectOnlyDistinct = distinctPerRole - sharedDistinct;
			int objectOnlyDistinct = distinctPerRole - sharedDistinct;
			int predicateDistinct = Math.max(1, distinctPerRole / 10);

			ByteString[] sharedPool = generatePool("http://example.org/shared/", sharedDistinct);
			ByteString[] subjectOnlyPool = generatePool("http://example.org/subject/", subjectOnlyDistinct);
			ByteString[] objectOnlyPool = generatePool("http://example.org/object/", objectOnlyDistinct);
			ByteString[] predicatePool = generatePool("http://example.org/predicate/", predicateDistinct);

			ByteString[] subjectPool = concat(sharedPool, subjectOnlyPool);
			ByteString[] objectPool = concat(sharedPool, objectOnlyPool);

			Random random = new Random(seed);

			this.subjects = new IndexedNode[tripleCount];
			this.predicates = new IndexedNode[tripleCount];
			this.objects = new IndexedNode[tripleCount];
			for (int i = 0; i < tripleCount; i++) {
				long tripleId = i + 1L;
				this.subjects[i] = new IndexedNode(subjectPool[random.nextInt(subjectPool.length)], tripleId);
				this.predicates[i] = new IndexedNode(predicatePool[random.nextInt(predicatePool.length)], tripleId);
				this.objects[i] = new IndexedNode(objectPool[random.nextInt(objectPool.length)], tripleId);
			}

			System.out.println("Sorting subjects");
			Arrays.parallelSort(this.subjects);
			System.out.println("Sorting predicates");
			Arrays.parallelSort(this.predicates);
			System.out.println("Sorting objects");
			Arrays.parallelSort(this.objects);

			this.sectionEntriesSorted = Arrays.copyOf(subjectPool, subjectPool.length);
			System.out.println("Sorting section entries");
			Arrays.parallelSort(this.sectionEntriesSorted);

			this.hdtOptions = HDTOptions.of();
			this.hdtOptions.set(HDTOptionsKeys.DISK_COMPRESSION_KEY, diskCompression);
			this.hdtOptions.set("pfc.blocksize", Integer.toString(pfcBlockSize));
		}

		CompressionResult newCompressionResult() {
			return new GeneratedCompressionResult(tripleCount, subjects, predicates, objects);
		}

		IteratorView<CharSequence> newSectionEntriesIterator() {
			return new IteratorView<>(sectionEntriesSorted);
		}

		long sectionEntriesCount() {
			return sectionEntriesSorted.length;
		}

		HDTOptions options() {
			return hdtOptions;
		}

		private static ByteString[] generatePool(String prefix, int size) {
			ByteString[] out = new ByteString[size];
			for (int i = 0; i < size; i++) {
				out[i] = ByteString.of(prefix + String.format("%08d", i));
			}
			return out;
		}

		private static ByteString[] concat(ByteString[] a, ByteString[] b) {
			ByteString[] out = Arrays.copyOf(a, a.length + b.length);
			System.arraycopy(b, 0, out, a.length, b.length);
			return out;
		}
	}

	@State(Scope.Thread)
	public static class TempPaths {
		Path trialDir;
		Path fourSectionBasePath;
		Path singleSectionBasePath;

		@Setup(Level.Trial)
		public void setupTrial() throws IOException {
			this.trialDir = Files.createTempDirectory("qendpoint-jmh-dict");
			long tid = Thread.currentThread().getId();
			this.fourSectionBasePath = trialDir.resolve("four-section-" + tid);
			this.singleSectionBasePath = trialDir.resolve("one-section-" + tid);
			System.out.println("Benchmark trial working dir: " + trialDir);
		}

		@TearDown(Level.Trial)
		public void tearDownTrial() throws IOException {
			deleteRecursively(trialDir);
		}
	}

	@Benchmark
	public long compressFourSectionDictionary_to_writeFourSectionDictionary(GeneratedData data, TempPaths paths)
			throws Exception {
		try (CompressionResult result = data.newCompressionResult();
				CompressFourSectionDictionary tempDict = new CompressFourSectionDictionary(result,
						NoOpNodeConsumer.INSTANCE, ProgressListener.ignore(), false, false);
				WriteFourSectionDictionary dict = new WriteFourSectionDictionary(data.options(),
						paths.fourSectionBasePath, data.bufferSize, false)) {

			dict.loadAsync(tempDict, ProgressListener.ignore());
			return dict.getNumberOfElements();
		}
	}

	@Benchmark
	public long writeDictionarySection_load(GeneratedData data, TempPaths paths) throws IOException {
		try (WriteDictionarySection section = new WriteDictionarySection(data.options(), paths.singleSectionBasePath,
				data.bufferSize)) {
			section.load(data.newSectionEntriesIterator(), data.sectionEntriesCount(), ProgressListener.ignore());
			return section.getNumberOfElements();
		}
	}

	private enum NoOpNodeConsumer implements CompressFourSectionDictionary.NodeConsumer {
		INSTANCE;

		@Override
		public void onSubject(long preMapId, long newMapId) {
		}

		@Override
		public void onPredicate(long preMapId, long newMapId) {
		}

		@Override
		public void onObject(long preMapId, long newMapId) {
		}

		@Override
		public void onGraph(long preMapId, long newMapId) {
		}
	}

	private static final class GeneratedCompressionResult implements CompressionResult {
		private final long tripleCount;
		private final IndexedNode[] subjects;
		private final IndexedNode[] predicates;
		private final IndexedNode[] objects;

		private GeneratedCompressionResult(long tripleCount, IndexedNode[] subjects, IndexedNode[] predicates,
				IndexedNode[] objects) {
			this.tripleCount = tripleCount;
			this.subjects = subjects;
			this.predicates = predicates;
			this.objects = objects;
		}

		@Override
		public long getTripleCount() {
			return tripleCount;
		}

		@Override
		public boolean supportsGraph() {
			return false;
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getSubjects() {
			return new IteratorView<>(subjects);
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getPredicates() {
			return new IteratorView<>(predicates);
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getObjects() {
			return new IteratorView<>(objects);
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getGraph() {
			return ExceptionIterator.empty();
		}

		@Override
		public long getSubjectsCount() {
			return tripleCount;
		}

		@Override
		public long getPredicatesCount() {
			return tripleCount;
		}

		@Override
		public long getObjectsCount() {
			return tripleCount;
		}

		@Override
		public long getGraphCount() {
			return 0;
		}

		@Override
		public long getSharedCount() {
			return tripleCount;
		}

		@Override
		public long getRawSize() {
			return 0;
		}

		@Override
		public void delete() {
		}

		@Override
		public void close() {
		}
	}

	private static final class IteratorView<T> implements ExceptionIterator<T, IOException>, java.util.Iterator<T> {
		private final T[] values;
		private int idx;

		private IteratorView(T[] values) {
			this.values = values;
		}

		@Override
		public boolean hasNext() {
			return idx < values.length;
		}

		@Override
		public T next() {
			return values[idx++];
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove");
		}
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
