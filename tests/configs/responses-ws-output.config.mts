import { createCoverageConfig } from "../vitest.base.mts";

export default createCoverageConfig({
  name: "responses-ws-output",
  environment: "node",
  testFiles: [
    "tests/unit/server-ws-output.test.ts",
    "tests/unit/server-response-write-backpressure.test.ts",
    "tests/unit/server-ws-continuation.test.ts",
  ],
  sourceFiles: ["server-lib/responses-ws-output.js"],
  thresholds: { lines: 80, functions: 80, branches: 80, statements: 80 },
});
