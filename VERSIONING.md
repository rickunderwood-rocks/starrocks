# CelerData Enterprise Platform Versioning

## Version Format

```
celerdata-vMAJOR.MINOR.PATCH
```

Following [Semantic Versioning 2.0.0](https://semver.org/):

| Component | When to Increment |
|-----------|-------------------|
| **MAJOR** | Breaking changes, major architecture shifts |
| **MINOR** | New features, backward-compatible enhancements |
| **PATCH** | Bug fixes, performance improvements |

## Current Version

**celerdata-v1.0.0** (February 2026)

## Version History

### celerdata-v1.0.0 (2026-02-01)
*120-Day Foundation Release*

**Phase 1: Foundation Optimizations**
- O(n²) → O(n) Chunk column removal
- I/O 4K alignment for SSD performance
- Memory Pressure Manager
- TabletInvertedIndex 64-shard striped locking
- Fast Raft consensus (<2s failover)
- Async Query REST API

**Phase 2: Enterprise Deal-Closing Features**
- Query SLA Manager
- Resource Isolation Manager
- OpenTelemetry Integration

**Statistics:** 9 features, 2,955+ LOC, 15 files

---

## Planned Versions

### celerdata-v1.1.0 (TBD)
*Next roadmap items - to be defined*

Candidates from 120-Day Roadmap:
- Zero-copy string operations for parser
- Predicate pushdown enhancement
- Query memory estimator improvements
- Connection pooling optimizations
- Bloom filter improvements
- Vectorized hash join optimization
- Compaction scheduling improvements

---

## Git Workflow

### Creating a Release

```bash
# Ensure all changes are committed
git status

# Create annotated tag
git tag -a celerdata-vX.Y.Z -m "CelerData Enterprise Platform vX.Y.Z

Description of release...

Features:
- Feature 1
- Feature 2
"

# Push tag to remote
git push origin celerdata-vX.Y.Z
```

### Viewing Releases

```bash
# List all CelerData versions
git tag -l "celerdata-*"

# View specific release
git show celerdata-v1.0.0

# Compare versions
git diff celerdata-v1.0.0..celerdata-v1.1.0
```

### Branch Strategy

```
main (upstream StarRocks)
  │
  └── feature/120-day-foundations (CelerData enhancements)
        │
        ├── celerdata-v1.0.0 (tag)
        ├── celerdata-v1.1.0 (future tag)
        └── ...
```

---

## Release Checklist

- [ ] All features implemented and tested
- [ ] Code committed with descriptive messages
- [ ] Technical documentation updated
- [ ] SA/Product documentation created
- [ ] Session backup updated
- [ ] Version tag created
- [ ] VERSIONING.md updated

---

## Relationship to StarRocks Versions

CelerData versions are **independent** of upstream StarRocks versions:

| CelerData | Based On | Notes |
|-----------|----------|-------|
| v1.0.0 | StarRocks 4.0.x | Initial enterprise enhancements |
| v1.1.0 | TBD | Next feature set |

This allows CelerData to:
1. Release features independently of upstream
2. Maintain clear tracking of enterprise additions
3. Simplify merging with upstream updates
