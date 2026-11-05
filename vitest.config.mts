import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // One database, and tests that kill each other's connections: one at a time.
    fileParallelism: false,
    testTimeout: 20000,
    hookTimeout: 20000
  }
})
