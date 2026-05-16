# Release

This repo uses the workspace version in `Cargo.toml` as the release version source.
Release tags must match that version prefix, for example `v0.6.0`.

## Release Controls

- Version source: root `Cargo.toml`
- Changelog source: [`CHANGELOG.md`](/Users/bentaylor/code/jansu/CHANGELOG.md)
- Build evidence: `just release`, `just test`, `just security-lane`
- Integrity evidence: SHA256 for `target/release/jansu`, Docker image push to `ghcr.io/neverhuman/jansu`, and the generated SBOM placeholder in the release job
- Audit evidence: `jankurai audit . --mode advisory`

## Changelog

The changelog is the release-facing summary of what changed between tags.
Keep each release entry short and specific:

- user-visible behavior changes
- compatibility changes
- migration notes
- security or rollback caveats

Use merged commits since the previous `v*` tag as the source for the entry.
When publishing a tagged release, update `CHANGELOG.md` before the tag is pushed.

## Release Process

1. Start from a clean main branch.
2. Run the validation lane:

```bash
just audit-check
just test
just security-lane
```

3. Build the release artifact:

```bash
just release
```

4. Build and verify the release image:

```bash
just docker-build
docker compose config
```

5. Create the tag that matches the workspace version.
6. Push the tag so `.github/workflows/release.yml` can publish the artifact and GitHub release.

## Rollback

- If a release is wrong before publication, delete the tag locally and remotely, then retag after the fix.
- If a published release is wrong, prefer a follow-up patch release over rewriting history.
- Keep the previous tag available so consumers can pin or roll back immediately.

## Provenance

The release workflow records the binary checksum and emits release assets through GitHub Actions.
The workflow also logs into GHCR and pushes `ghcr.io/neverhuman/jansu` for the tagged release.

## Notes

- Do not publish from a dirty tree.
- Do not skip the test or security lanes for a tagged release.
- Keep the release workflow pinned to full action SHAs.
