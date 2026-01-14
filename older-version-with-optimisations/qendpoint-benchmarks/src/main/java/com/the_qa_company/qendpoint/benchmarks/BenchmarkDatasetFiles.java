package com.the_qa_company.qendpoint.benchmarks;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

public final class BenchmarkDatasetFiles {

	private static final Path DATAGOVBE_VALID_TTL = Path.of("indexing", "datagovbe-valid.ttl");
	private static final Path DATAGOVBE_VALID_NT_GZ = Path.of("indexing", "datagovbe-valid.nt.gz");

	private BenchmarkDatasetFiles() {
	}

	public static Path ensureDatagovbeValidNtGz(Path repoRoot) throws IOException {
		Objects.requireNonNull(repoRoot, "repoRoot");

		Path ttlFile = repoRoot.resolve(DATAGOVBE_VALID_TTL);
		Path ntGzFile = repoRoot.resolve(DATAGOVBE_VALID_NT_GZ);
		ensureTurtleToNtGz(ttlFile, ntGzFile);
		return ntGzFile;
	}

	static void ensureTurtleToNtGz(Path ttlFile, Path ntGzFile) throws IOException {
		if (!Files.exists(ttlFile)) {
			throw new NoSuchFileException(ttlFile.toString());
		}

		if (Files.exists(ntGzFile)
				&& Files.getLastModifiedTime(ntGzFile).compareTo(Files.getLastModifiedTime(ttlFile)) >= 0) {
			return;
		}

		Files.createDirectories(ntGzFile.getParent());

		Path tmpFile = ntGzFile.resolveSibling(ntGzFile.getFileName() + ".tmp");

		try (OutputStream fileOut = Files.newOutputStream(tmpFile, StandardOpenOption.CREATE,
				StandardOpenOption.TRUNCATE_EXISTING);
				OutputStream bufferedOut = new BufferedOutputStream(fileOut);
				GZIPOutputStream gzipOut = new GZIPOutputStream(bufferedOut)) {

			StreamRDF writer = StreamRDFWriter.getWriterStream(gzipOut, Lang.NTRIPLES);

			RDFParser.source(ttlFile.toString()).base(ttlFile.toUri().toString()).lang(Lang.TURTLE).parse(writer);
		} catch (RuntimeException e) {
			Files.deleteIfExists(tmpFile);
			throw e;
		} catch (IOException e) {
			Files.deleteIfExists(tmpFile);
			throw e;
		}

		try {
			Files.move(tmpFile, ntGzFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		} catch (AtomicMoveNotSupportedException e) {
			Files.move(tmpFile, ntGzFile, StandardCopyOption.REPLACE_EXISTING);
		}
	}
}
