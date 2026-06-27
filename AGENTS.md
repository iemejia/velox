# AGENTS.md

Instructions for AI coding agents working in the Velox repository.

## Primary Instructions

All agent guidance lives in [`.claude/CLAUDE.md`](.claude/CLAUDE.md). Read that
file for build commands, testing workflows, coding style, naming conventions,
common mistakes, and commit message rules.

## Skills

Specialized workflows are defined as skills in `.claude/skills/`:

| Skill | Purpose |
|-------|---------|
| [pr-review](.claude/skills/pr-review/SKILL.md) | Review PRs for code quality, memory safety, performance, and correctness |
| [query](.claude/skills/query/SKILL.md) | Answer questions about the codebase or pull requests |
| [write-commit-message](.claude/skills/write-commit-message/SKILL.md) | Draft commit messages following project conventions |
| [ci-failure-analysis](.claude/skills/ci-failure-analysis/SKILL.md) | Analyze CI failures and post diagnostic comments |

## Additional References

- [CODING_STYLE.md](CODING_STYLE.md) - Complete coding style guide
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
- [scripts/review/REVIEW_GUIDE.md](scripts/review/REVIEW_GUIDE.md) - PR review style guide
