import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { ProviderTextGeneration } from "@t3tools/provider-core/server/textGeneration";

const unsupported = (operation: string) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: "xi text generation is not implemented in this build",
    }),
  );

export function makeXiTextGeneration(): ProviderTextGeneration {
  return {
    generateCommitMessage: () => unsupported("commit message"),
    generatePrContent: () => unsupported("pull request content"),
    generateBranchName: () => unsupported("branch name"),
    generateThreadTitle: () => unsupported("thread title"),
    generateReviewGuide: () => unsupported("review guide"),
  };
}
