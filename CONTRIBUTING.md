# Contributing to EventFlow

Thank you for your interest in contributing to EventFlow! We welcome contributions of all kinds: bug fixes, architectural improvements, new examples, performance optimizations, and documentation enhancements.

---

## 🛠 Local Development Setup

### Prerequisites
- **Node.js**: `v20.0.0` or higher (`v22` / `v24` recommended)
- **npm**: `v10.0.0` or higher
- **Git**

### Installation
```bash
# Clone the repository
git clone https://github.com/your-username/eventflow.git
cd eventflow

# Install dependencies
npm install

# Build the project
npm run build

# Run the automated test suite
npm test
```

---

## 📁 Codebase Architecture Tour

```
src/
├── core/
│   ├── dag/               # Directed Acyclic Graph validation, 3-color DFS cycle detection & Kahn topological sort
│   ├── state/             # Append-only event store, event replay, and disk WAL persistence
│   ├── engine/            # Main orchestrator, bounded worker pool, exponential backoff with jitter
│   ├── demo-workflows.ts  # Pre-packaged ETL and Saga sample pipelines
│   └── types.ts           # Central TypeScript interface declarations
├── server/
│   ├── http-server.ts     # REST API server & static UI dashboard provider
│   └── ws-server.ts       # WebSocket live telemetry broadcast hub
├── ui/
│   ├── index.html         # Modern dark-mode dashboard
│   ├── dashboard.css      # Observability stylesheet
│   └── dashboard.js       # Live canvas/SVG DAG renderer and WebSocket client
├── cli/
│   └── cli.ts             # CLI command runner (start, bench, help)
└── index.ts               # Primary library exports
```

---

## 🧪 Testing Guidelines

EventFlow maintains high test coverage across unit and integration layers:

```bash
# Run all unit and integration tests
npm test

# Type-check TypeScript sources without emitting
npm run typecheck

# Run realistic workflow examples
npm run example:etl
npm run example:saga
npm run example:enrich

# Run synthetic high-throughput benchmark
npx tsx src/cli/cli.ts bench --workflows 20 --concurrency 8
```

---

## 📝 Commit Message Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

- `feat(dag): add dynamic sub-graph dependency expansion`
- `fix(worker): prevent race condition during drain under high load`
- `perf(event-store): optimize sequence filtering with binary search`
- `docs(adr): add architectural record for Saga rollback ordering`
- `test(orchestrator): add timeout abort integration test`

---

## 🚀 Submitting a Pull Request

1. Fork the repository and create your branch from `main`:
   ```bash
   git checkout -b feat/your-feature-name
   ```
2. Make your code changes, ensuring all existing tests pass and new tests are added.
3. Run `npm test` and `npm run build`.
4. Commit your changes using conventional commit messages.
5. Push to your fork and submit a Pull Request against `main`.
6. Complete the PR template checklist.
