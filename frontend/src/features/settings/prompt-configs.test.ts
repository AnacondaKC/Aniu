import { afterEach, describe, expect, it } from "vitest";

import { listPromptConfigs, parseImportedConfig } from "./prompt-configs";

const legacyKey = "aniu.prompt-configs.v2";
const currentKey = "aniu.prompt-configs.v3";

const legacyConfig = {
  name: "legacy",
  global_prompt: "global",
  run_prompt: "run",
  summary_prompt: "summary",
};

afterEach(() => {
  localStorage.clear();
});

describe("prompt configs", () => {
  it("migrates v2 profiles and keeps the Dream prompt fallback empty", () => {
    localStorage.setItem(legacyKey, JSON.stringify([legacyConfig]));

    expect(listPromptConfigs()).toEqual([{ ...legacyConfig, dream_prompt: "" }]);
    expect(JSON.parse(localStorage.getItem(currentKey) ?? "null")).toEqual([
      { ...legacyConfig, dream_prompt: "" },
    ]);
  });

  it("accepts imports without the Dream prompt field", () => {
    expect(parseImportedConfig(legacyConfig)).toEqual({
      ...legacyConfig,
      dream_prompt: "",
    });
  });
});
