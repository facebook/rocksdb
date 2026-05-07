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
//              being considered part of the same comment family.
//   prunePrefix - optional marker prefix whose older comments should be
//                 superseded. If obsoleteTitle is set, older comments are
//                 collapsed instead of deleted.
//   preserveLatest - optional count of active comments to keep when
//                    prunePrefix is set.
//   obsoleteMarker - optional HTML marker used to detect already-obsolete
//                    comments.
//   obsoleteTitle - optional heading to use when collapsing superseded
//                   comments into a details block.

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
  obsoleteMarker = '',
  obsoleteTitle = '',
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

  function commentSortDescending(left, right) {
    const timeDelta = commentActivityTime(right) - commentActivityTime(left);
    if (timeDelta !== 0) {
      return timeDelta;
    }
    return Number(right.id || 0) - Number(left.id || 0);
  }

  function isObsoleteComment(comment) {
    return !!obsoleteMarker && typeof comment.body === 'string' &&
        comment.body.includes(obsoleteMarker);
  }

  function buildObsoleteBody(comment) {
    const originalBody =
        typeof comment.body === 'string' && comment.body.trim() ?
        comment.body :
        '*No original review body preserved.*';
    const title = obsoleteTitle || 'AI Review - OBSOLETE';
    return `${obsoleteMarker}\n## ${
        title}\n\n<details>\n<summary>Superseded by a newer AI review. Expand to see the original review.</summary>\n\n${
        originalBody}\n\n</details>`;
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

  async function supersedeCommentIfPresent(comment, reason) {
    if (!obsoleteTitle) {
      await deleteCommentIfPresent(comment, reason);
      return;
    }
    if (isObsoleteComment(comment)) {
      return;
    }
    try {
      await github.rest.issues.updateComment({
        owner,
        repo,
        comment_id: comment.id,
        body: buildObsoleteBody(comment),
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
  let currentCommentId = null;

  if (existing) {
    try {
      const response = await github.rest.issues.updateComment({
        owner,
        repo,
        comment_id: existing.id,
        body: markedBody,
      });
      currentCommentId = existing.id;
      core.info(`Updated existing comment ${existing.id}`);
    } catch (error) {
      if (error.status !== 404) {
        throw error;
      }
      core.info(
          `Comment ${existing.id} disappeared before update; ` +
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
    core.info('Created new PR comment');
  }

  if (prunePrefix || legacyMarkers.length > 0) {
    comments = await listComments();
    const relatedComments = comments.filter(
        comment => comment.id !== currentCommentId &&
            typeof comment.body === 'string' &&
            ((prunePrefix && comment.body.includes(prunePrefix)) ||
             legacyMarkers.some(
                 legacyMarker => comment.body.includes(legacyMarker))));
    const activeRelatedComments =
        comments
            .filter(
                comment => typeof comment.body === 'string' &&
                    !isObsoleteComment(comment) &&
                    (comment.id === currentCommentId ||
                     relatedComments.some(
                         relatedComment => relatedComment.id === comment.id)))
            .sort(commentSortDescending);

    const keep =
        new Set(activeRelatedComments.slice(0, Math.max(preserveLatest, 1))
                    .map(comment => comment.id));

    const supersedeCandidates = obsoleteTitle ?
        activeRelatedComments.filter(comment => !keep.has(comment.id)) :
        relatedComments.filter(comment => !keep.has(comment.id));

    for (const comment of supersedeCandidates) {
      if (keep.has(comment.id)) {
        continue;
      }
      await supersedeCommentIfPresent(comment, 'Superseded old AI review');
    }
  }
};
