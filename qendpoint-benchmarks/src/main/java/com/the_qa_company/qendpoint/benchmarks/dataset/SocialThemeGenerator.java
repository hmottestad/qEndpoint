package com.the_qa_company.qendpoint.benchmarks.dataset;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.RDF_TYPE;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.SOCIAL_NS;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.XSD_DATE_TIME;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.entity;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.iri;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterInt;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.jitterRandom;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.randomSentence;
import static com.the_qa_company.qendpoint.benchmarks.dataset.ThemeDatasetSupport.scaled;

final class SocialThemeGenerator {
	private SocialThemeGenerator() {
	}

	static void generate(ThemeDatasetGenerator.GenerationConfig config, NTriplesWriter writer) throws IOException {
		Random contentRandom = new Random(config.seed());
		Random jitterRandom = jitterRandom(config.seed());

		int userCount = jitterInt(jitterRandom, scaled(300, config.scale()), 2);
		int tagCount = jitterInt(jitterRandom, scaled(40, config.scale()), 2);
		int postsPerUser = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int commentsPerPost = jitterInt(jitterRandom, scaled(2, config.scale()), 1);
		int likesPerPost = jitterInt(jitterRandom, scaled(3, config.scale()), 1);
		int followsPerUser = jitterInt(jitterRandom, scaled(3, config.scale()), 1);
		int tagsPerPost = jitterInt(jitterRandom, scaled(2, config.scale()), 1);

		String userType = iri(SOCIAL_NS, "User");
		String postType = iri(SOCIAL_NS, "Post");
		String commentType = iri(SOCIAL_NS, "Comment");
		String tagType = iri(SOCIAL_NS, "Tag");

		String follows = iri(SOCIAL_NS, "follows");
		String authored = iri(SOCIAL_NS, "authored");
		String likedBy = iri(SOCIAL_NS, "likedBy");
		String hasComment = iri(SOCIAL_NS, "hasComment");
		String hasTag = iri(SOCIAL_NS, "hasTag");
		String createdAt = iri(SOCIAL_NS, "createdAt");
		String hasName = iri(SOCIAL_NS, "name");
		String content = iri(SOCIAL_NS, "content");

		List<String> users = new ArrayList<>(userCount);
		for (int u = 0; u < userCount; u++) {
			String user = entity(SOCIAL_NS, "user", u);
			users.add(user);
			writer.iriStatement(user, RDF_TYPE, userType);
			writer.literalStatement(user, hasName, "user" + u);
		}

		List<String> tags = new ArrayList<>(tagCount);
		for (int t = 0; t < tagCount; t++) {
			String tag = entity(SOCIAL_NS, "tag", t);
			tags.add(tag);
			writer.iriStatement(tag, RDF_TYPE, tagType);
			writer.literalStatement(tag, hasName, "tag" + t);
		}

		int postIndex = 0;
		int commentIndex = 0;
		for (String user : users) {
			for (int f = 0; f < followsPerUser; f++) {
				String target = users.get(contentRandom.nextInt(users.size()));
				if (!user.equals(target)) {
					writer.iriStatement(user, follows, target);
				}
			}

			for (int p = 0; p < postsPerUser; p++) {
				String post = entity(SOCIAL_NS, "post", postIndex++);
				writer.iriStatement(post, RDF_TYPE, postType);
				writer.iriStatement(post, authored, user);
				writer.literalStatement(post, content, randomSentence(contentRandom, 5, 12));
				writer.literalStatement(post, createdAt,
						LocalDateTime.of(2024, 1, 1, 8, 0).plusHours(contentRandom.nextInt(500)).toString(),
						XSD_DATE_TIME);

				for (int t = 0; t < tagsPerPost; t++) {
					String tag = tags.get(contentRandom.nextInt(tags.size()));
					writer.iriStatement(post, hasTag, tag);
				}

				for (int l = 0; l < likesPerPost; l++) {
					String liker = users.get(contentRandom.nextInt(users.size()));
					writer.iriStatement(post, likedBy, liker);
				}

				for (int c = 0; c < commentsPerPost; c++) {
					String comment = entity(SOCIAL_NS, "comment", commentIndex++);
					String commenter = users.get(contentRandom.nextInt(users.size()));
					writer.iriStatement(comment, RDF_TYPE, commentType);
					writer.iriStatement(comment, authored, commenter);
					writer.literalStatement(comment, content, randomSentence(contentRandom, 3, 8));
					writer.iriStatement(post, hasComment, comment);
				}
			}
		}
	}
}
