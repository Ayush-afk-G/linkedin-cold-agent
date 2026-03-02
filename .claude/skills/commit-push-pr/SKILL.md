---
name: commit-push-pr
description: Commit current changes, push to remote, and open a PR
disable-model-invocation: true
---

Commit and push the current changes, then open a PR.

1. Run `git status` to see what changed
2. Stage all changes with `git add -A`
3. Generate a descriptive commit message based on the diff
4. Commit the changes
5. Push to the current branch (create remote branch if needed)
6. Use `gh pr create --fill` to open a pull request
7. Report the PR URL
