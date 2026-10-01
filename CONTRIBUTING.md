# CONTRIBUTING.md

Thank you for considering contributing to kahuna! Your contributions help improve and maintain the library for everyone.

## Getting Started

### Fork and Clone the Repository

1. Fork the repository on GitHub.
2. Clone your forked repository to your local machine:
```bash
git clone https://github.com/kahunakv/kahuna.git
```
3. Navigate to the cloned directory:
```bash
cd kahuna
```

### Setting Up Your Development Environment

1. Ensure you have the .NET 10 SDK installed. You can download it [here](https://dotnet.microsoft.com/download).
2. Restore dependencies:
```bash
dotnet restore
```
3. Build the project:
```bash
dotnet build
```

### Running Tests

Run affected in-process tests first; no Docker cluster is required:

```bash
dotnet test Kahuna.Server.Tests/Kahuna.Server.Tests.csproj -c Debug \
  --logger "trx;LogFileName=server-tests.trx" \
  --logger "console;verbosity=normal" 2>&1 | tee /tmp/kahuna-server-tests.log
```

Add `--filter` to scope a change's verification. Run only one `dotnet test` process at a time: embedded
nodes and timing-sensitive state make concurrent runs interfere. A full server run takes about
20 minutes; capture its TRX and console output and extract every failure from that run.

`Kahuna.Client.Tests` requires the Docker cluster at the configured HTTPS endpoints. See the
[README test instructions](README.md#running-tests) for startup and teardown. Do not use a solution-wide
`dotnet test` unless that cluster is up. When using `tee` in automation, enable pipeline failure
propagation in your shell to preserve the test exit status.

## Making Changes

### Branching

1. Create a new branch for your feature or bug fix:
```bash
git checkout -b feature/your-feature-name
```
or
```bash
git checkout -b bugfix/your-bugfix-name
```

### Coding Standards

- Follow the existing code style and conventions.
- Ensure your code is well-documented with comments where necessary.
- Write tests for new features and bug fixes.

### Commit Messages

- Write clear, concise commit messages.
- Use the present tense ("Add feature" not "Added feature").
- Include a reference to the issue number if applicable (e.g., `Fixes #123`).

### Pushing Changes

1. Push your changes to your fork:
```bash
git push origin feature/your-feature-name
```

2. Open a pull request on the original repository.

## Pull Request Process

1. Ensure your pull request description clearly explains the changes and the reasons for them.
2. Ensure your code passes all tests and adheres to the project's coding standards.
3. Be responsive to feedback and questions. The maintainers may request changes before your pull request can be merged.

## Reporting Issues

1. Check the [existing issues](https://github.com/kahunakv/kahuna/issues) before opening a new one to avoid duplicates.
2. Open a new issue and provide detailed information about the problem, including steps to reproduce, expected behavior, and actual behavior.
3. If you have a solution, feel free to submit a pull request along with the issue.

## Community and Support

- Join our [discussion forum](https://github.com/kahunakv/kahuna/discussions) for general questions and support.
- Follow the [Code of Conduct](CODE_OF_CONDUCT.md) to ensure a welcoming and inclusive environment for all contributors.

Thank you for your contributions!

---

By contributing to this project, you agree to abide by the [Code of Conduct](CODE_OF_CONDUCT.md).