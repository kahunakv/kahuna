// Node.js entry point for the browser-wasm smoke app. scripts/run-wasm-smoke.sh copies this file
// next to _framework/ in the build output and runs it.
import { dotnet } from './_framework/dotnet.js';

const exitCode = await dotnet.run();
process.exit(exitCode);
