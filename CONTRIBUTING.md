# Contributing to fit-go

Thanks for your interest in contributing! This document describes how to get
set up and the conventions we follow.

## Getting started

1. Fork the repository and clone your fork.
2. Ensure you have Go installed (see the version in [`go.mod`](go.mod)).
3. Install dependencies and verify the build:

   ```sh
   go mod download
   go build ./...
   go test ./...
   ```

## Development workflow

- Create a topic branch off `main` for your change.
- Keep changes focused; one logical change per pull request.
- Run `go fmt ./...` and `go vet ./...` before committing.
- Add or update tests for any behavior you change.
- Make sure `go test ./...` passes locally.

## Commit messages

- Use clear, imperative subject lines (e.g. "Add Redis sentinel support").
- Reference related issues in the body where relevant.

## Pull requests

- Describe what the change does and why.
- Note any breaking changes or new configuration.
- A maintainer will review; please respond to feedback and keep the branch
  up to date with `main`.

## Code style

- Follow standard Go conventions and `gofmt` formatting.
- Prefer small, well-named functions and table-driven tests.
- Keep public APIs documented with Go doc comments.

## Reporting bugs

Open an issue with a minimal reproduction, the expected behavior, and the
actual behavior. Include your Go version and OS.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).
