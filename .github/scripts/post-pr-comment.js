// Shared PR comment posting utility.
// Used by both clang-tidy-comment.yml and claude-review-comment.yml.
//
// Usage from actions/github-script:
//   const post = require('./.github/scripts/post-pr-comment.js');
//   await post({ github, context, core, prNumber, body, marker });
//
// Parameters:
//   github   - octokit instance from actions/github-script
//   context  - GitHub Actions context
//   core     - @actions/core for logging
//   prNumber - PR number to comment on
//   body     - comment body (markdown string)
//   marker   - HTML comment marker for dedup (e.g. '<!-- claude-review -->')
//              If an existing comment with this marker is found, it is updated.
//              If not found, a new comment is created.
//   legacyMarkers - optional list of legacy markers that should be migrated by
//              updating the newest matching legacy comment in place.
//   prunePrefix - optional marker prefix whose older comments should be pruned.
//                 Deletion is best-effort so concurrent runs can race safely.
//   preserveLatest - optional count of comments to keep when prunePrefix is
//                    set.

module.exports = async function postPrComment({
  github,
  context,
  core,
  prNumber,
  body,
  marker,
  legacyMarkers = [],
  prunePrefix = '',
  preserveLatest = 0,
}) {
  if (!prNumber || !body) {
    core.warning('Missing prNumber or body; skipping comment.');
    return;
  }

  const owner = context.repo.owner;
  const repo = context.repo.repo;

  // Ensure marker is embedded in the body
  const markedBody = body.includes(marker) ? body : `${marker}\n${body}`;

  async function listComments() {
    return await github.paginate(github.rest.issues.listComments, {
      owner,
      repo,
      issue_number: prNumber,
      per_page: 100,
    });
  }

  function commentActivityTime(comment) {
    const timestamp =
        Date.parse(comment.updated_at || comment.created_at || '');
    return Number.isNaN(timestamp) ? 0 : timestamp;
  }

  async function deleteCommentIfPresent(comment, reason) {
    try {
      await github.rest.issues.deleteComment({
        owner,
        repo,
        comment_id: comment.id,
      });
      core.info(`${reason} ${comment.id}`);
    } catch (error) {
      if (error.status === 404) {
        core.info(`Comment ${comment.id} was already deleted by another run.`);
        return;
      }
      throw error;
    }
  }

  let comments = await listComments();
  const existing = comments.find(
      comment =>
          typeof comment.body === 'string' && comment.body.includes(marker));
  const legacy = existing ?
      null :
      comments.filter(
                  comment => typeof comment.body === 'string' &&
                      legacyMarkers.some(
                          legacyMarker => comment.body.includes(legacyMarker)))
              .sort(
                  (left, right) => commentActivityTime(right) -
                      commentActivityTime(left))[0] ||
          null;
  const target = existing || legacy;
  let currentCommentId = null;
  let currentCommentTime = 0;

  if (target) {
    try {
      const response = await github.rest.issues.updateComment({
        owner,
        repo,
        comment_id: target.id,
        body: markedBody,
      });
      currentCommentId = target.id;
      currentCommentTime = commentActivityTime(response.data);
      core.info(
          `${existing ? 'Updated' : 'Migrated'} existing comment ${target.id}`);
    } catch (error) {
      if (error.status !== 404) {
        throw error;
      }
      core.info(
          `Comment ${target.id} disappeared before update; ` +
          'creating a fresh comment instead.');
    }
  }

  if (!currentCommentId) {
    const response = await github.rest.issues.createComment({
      owner,
      repo,
      issue_number: prNumber,
      body: markedBody,
    });
    currentCommentId = response.data.id;
    currentCommentTime = commentActivityTime(response.data);
    core.info('Created new PR comment');
  }

  if (prunePrefix || legacyMarkers.length > 0) {
    comments = await listComments();
    if (!currentCommentTime) {
      const currentComment =
          comments.find(comment => comment.id === currentCommentId);
      currentCommentTime =
          currentComment ? commentActivityTime(currentComment) : 0;
    }

    const legacyComments = comments.filter(
        comment => comment.id !== currentCommentId &&
            typeof comment.body === 'string' &&
            legacyMarkers.some(
                legacyMarker => comment.body.includes(legacyMarker)));
    for (const comment of legacyComments) {
      await deleteCommentIfPresent(comment, 'Deleted legacy marker comment');
    }

    if (!prunePrefix || preserveLatest <= 0) {
      return;
    }

    const currentMarkerComments =
        comments
            .filter(
                comment => comment.id !== currentCommentId &&
                    typeof comment.body === 'string' &&
                    comment.body.includes(prunePrefix))
            .sort(
                (left, right) =>
                    commentActivityTime(right) - commentActivityTime(left));

    const newerThanCurrent = currentMarkerComments.filter(
        comment => commentActivityTime(comment) > currentCommentTime);
    const olderOrSame = currentMarkerComments.filter(
        comment => commentActivityTime(comment) <= currentCommentTime);
    const olderSlots =
        Math.max(preserveLatest - 1 - newerThanCurrent.length, 0);
    const keep = new Set([
      currentCommentId,
      ...newerThanCurrent.map(comment => comment.id),
      ...olderOrSame.slice(0, olderSlots).map(comment => comment.id),
    ]);

    for (const comment of olderOrSame) {
      if (keep.has(comment.id)) {
        continue;
      }
      await deleteCommentIfPresent(comment, 'Deleted old comment');
    }
  }
};
