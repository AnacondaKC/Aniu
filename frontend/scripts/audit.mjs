import { spawnSync } from "node:child_process";

const ALLOWED_ADVISORIES = new Map([
  [
    "https://github.com/advisories/GHSA-qwww-vcr4-c8h2",
    {
      packages: new Set(["react-router", "react-router-dom"]),
      trackingUrl:
        "https://github.com/remix-run/react-router/security/advisories/GHSA-qwww-vcr4-c8h2",
      reason:
        "RSC APIs are not used by this Vite client-only application; the upstream advisory marks 7.18.2 as patched",
      expiresOn: "2026-12-31",
    },
  ],
]);
const BLOCKING_SEVERITIES = new Set(["high", "critical"]);

const audit = spawnSync("npm", ["audit", "--json"], {
  cwd: new URL("..", import.meta.url),
  encoding: "utf8",
});

if (!audit.stdout) {
  process.stderr.write(audit.stderr || "npm audit produced no JSON output\n");
  process.exit(1);
}

let report;
try {
  report = JSON.parse(audit.stdout);
} catch (error) {
  process.stderr.write(`failed to parse npm audit output: ${String(error)}\n`);
  process.exit(1);
}

if (report.error) {
  process.stderr.write(`npm audit failed: ${JSON.stringify(report.error)}\n`);
  process.exit(1);
}

const vulnerabilities = report.vulnerabilities ?? {};
const resolved = new Map();

function isAllowedPackage(name, visiting = new Set()) {
  if (resolved.has(name)) return resolved.get(name);
  const vulnerability = vulnerabilities[name];
  if (!vulnerability || !BLOCKING_SEVERITIES.has(vulnerability.severity)) {
    resolved.set(name, true);
    return true;
  }
  if (visiting.has(name)) return false;

  const nextVisiting = new Set(visiting).add(name);
  const allowed = vulnerability.via.every((source) => {
    if (typeof source === "string") {
      return (
        [...ALLOWED_ADVISORIES.values()].some((advisory) => advisory.packages.has(name)) &&
        isAllowedPackage(source, nextVisiting)
      );
    }
    const advisory = ALLOWED_ADVISORIES.get(source.url);
    return advisory?.packages.has(name) === true;
  });
  resolved.set(name, allowed);
  return allowed;
}

const blocked = Object.keys(vulnerabilities).filter((name) => !isAllowedPackage(name));
if (blocked.length > 0) {
  process.stderr.write(`blocking npm audit findings: ${blocked.sort().join(", ")}\n`);
  process.exit(1);
}

const today = new Date().toISOString().slice(0, 10);
for (const [url, advisory] of ALLOWED_ADVISORIES) {
  if (today > advisory.expiresOn) {
    process.stderr.write(`audit exception expired for ${url}\n`);
    process.exit(1);
  }

  const present = Object.entries(vulnerabilities).some(
    ([name, vulnerability]) =>
      advisory.packages.has(name) &&
      vulnerability.via.some((source) => typeof source !== "string" && source.url === url),
  );
  if (present) {
    process.stdout.write(
      `allowed ${url} for ${[...advisory.packages].join(", ")}: ${advisory.reason}; tracking ${advisory.trackingUrl}; review by ${advisory.expiresOn}\n`,
    );
  }
}
