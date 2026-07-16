/**
 * Focused tests for the Voucher Drops "Preview Drop" confirmation flow.
 * Run with: node --test voucher_drop_preview.test.js
 */
"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const {
  buildDropPreview,
  DropSubmissionController,
} = require("./static/js/voucher_drop_preview.js");

const FIXED_NOW = new Date("2026-07-16T04:00:00.000Z"); // 2026-07-16 12:00:00 KL

function baseInput(overrides) {
  return Object.assign(
    {
      type: "personalised",
      name: "VIP Drop",
      priority: "150",
      startsNative: "2026-08-01T10:00",
      endsNative: "2026-08-02T10:00",
      eligMode: "public",
      eligValuesRaw: "",
      regions: ["Malaysia"],
      pairsText: "@alice,CODE1\n@bob,CODE2\n@carol,CODE3\n@dave,CODE4\n@eve,CODE5\n@frank,CODE6",
      codesText: "",
      poolSelect: "public",
      whitelistText: "",
    },
    overrides
  );
}

test("personalised preview: valid pairs produce no errors and a correct summary", () => {
  const result = buildDropPreview(baseInput(), { now: FIXED_NOW });
  assert.deepEqual(result.errors, []);
  assert.equal(result.summary.type, "personalised");
  assert.equal(result.summary.totalCount, 6);
  assert.equal(result.summary.sampleEntries.length, 5);
  assert.equal(result.summary.remainingCount, 1);
  assert.equal(result.summary.calculatedState, "Scheduled");
  assert.equal(result.payload.assignments.length, 6);
  assert.equal(result.payload.type, "personalised");
});

test("pooled preview: valid codes produce no errors and a correct summary", () => {
  const result = buildDropPreview(
    baseInput({
      type: "pooled",
      pairsText: "",
      codesText: "CODE1\nCODE2\nCODE3\nCODE4\nCODE5\nCODE6\nCODE7",
      poolSelect: "public",
      whitelistText: "@alice, @bob",
    }),
    { now: FIXED_NOW }
  );
  assert.deepEqual(result.errors, []);
  assert.equal(result.summary.type, "pooled");
  assert.equal(result.summary.pool, "public");
  assert.equal(result.summary.totalCount, 7);
  assert.equal(result.summary.whitelistCount, 2);
  assert.equal(result.summary.sampleEntries.length, 5);
  assert.equal(result.summary.remainingCount, 2);
  assert.equal(result.payload.codes.length, 7);
  assert.equal(result.payload.whitelistUsernames.length, 2);
  // whitelist present -> no "public pooled drop with no whitelist" warning
  assert.ok(!result.warnings.some((w) => w.includes("no whitelist")));
});

test("pooled preview with no whitelist warns prominently", () => {
  const result = buildDropPreview(
    baseInput({
      type: "pooled",
      pairsText: "",
      codesText: "CODE1\nCODE2\nCODE3\nCODE4\nCODE5\nCODE6",
      poolSelect: "public",
      whitelistText: "",
    }),
    { now: FIXED_NOW }
  );
  assert.deepEqual(result.errors, []);
  assert.ok(result.warnings.some((w) => w.includes("no whitelist")));
});

test("invalid date range: end time not later than start time is a blocking error", () => {
  const result = buildDropPreview(
    baseInput({ startsNative: "2026-08-02T10:00", endsNative: "2026-08-01T10:00" }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes("End time must be later than start time")));
});

test("invalid date range: missing start time is a blocking error", () => {
  const result = buildDropPreview(baseInput({ startsNative: "" }), { now: FIXED_NOW });
  assert.ok(result.errors.some((e) => e.includes("start date/time is required")));
});

test("duplicate codes: pooled drop with a repeated code is blocked, not silently deduped", () => {
  const result = buildDropPreview(
    baseInput({
      type: "pooled",
      pairsText: "",
      codesText: "CODE1\nCODE2\nCODE1\nCODE3",
    }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes("Duplicate voucher codes") && e.includes("CODE1")));
});

test("duplicate codes: personalised drop with a repeated code is blocked", () => {
  const result = buildDropPreview(
    baseInput({
      pairsText: "@alice,CODE1\n@bob,CODE1",
    }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes("Duplicate voucher codes") && e.includes("CODE1")));
});

test("personalised drop with a repeated username is blocked", () => {
  const result = buildDropPreview(
    baseInput({
      pairsText: "@alice,CODE1\n@alice,CODE2",
    }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes("Duplicate usernames") && e.includes("@alice")));
});

test("exact duplicate personalised rows are silently deduped and reported as a warning, not an error", () => {
  const result = buildDropPreview(
    baseInput({
      pairsText: "@alice,CODE1\n@alice,CODE1\n@bob,CODE2\n@carol,CODE3\n@dave,CODE4\n@eve,CODE5",
    }),
    { now: FIXED_NOW }
  );
  assert.deepEqual(result.errors, []);
  assert.ok(result.warnings.some((w) => w.includes("duplicate row")));
  assert.equal(result.summary.totalCount, 5);
});

test("missing eligibility values: tier mode with no values is a blocking error", () => {
  const result = buildDropPreview(
    baseInput({ eligMode: "tier", eligValuesRaw: "" }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes('Eligibility values are required for "tier"')));
});

test("missing eligibility values: user_id mode with no values is a blocking error", () => {
  const result = buildDropPreview(
    baseInput({ eligMode: "user_id", eligValuesRaw: "" }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes('Eligibility values are required for "user_id"')));
});

test("eligibility values supplied for tier mode pass validation", () => {
  const result = buildDropPreview(
    baseInput({ eligMode: "tier", eligValuesRaw: "VIP1, VIP2" }),
    { now: FIXED_NOW }
  );
  assert.deepEqual(result.errors, []);
  assert.deepEqual(result.summary.eligValuesDisplay, ["VIP1", "VIP2"]);
  assert.deepEqual(result.payload.eligibility, { mode: "tier", allow: ["VIP1", "VIP2"] });
});

test("personalised drop with no valid pairs is blocked", () => {
  const result = buildDropPreview(baseInput({ pairsText: "" }), { now: FIXED_NOW });
  assert.ok(result.errors.some((e) => e.includes("no valid username/code pairs")));
});

test("pooled drop with no codes is blocked", () => {
  const result = buildDropPreview(
    baseInput({ type: "pooled", pairsText: "", codesText: "" }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes("no voucher codes")));
});

test("malformed personalised rows are blocked", () => {
  const result = buildDropPreview(
    baseInput({ pairsText: "@alice_missing_code\n@bob,CODE2\n@carol,CODE3\n@dave,CODE4\n@eve,CODE5" }),
    { now: FIXED_NOW }
  );
  assert.ok(result.errors.some((e) => e.includes("Malformed personalised rows")));
});

test("start time already in the past is a warning, not an error", () => {
  const result = buildDropPreview(
    baseInput({ startsNative: "2026-07-15T10:00", endsNative: "2026-08-01T10:00" }),
    { now: FIXED_NOW }
  );
  assert.deepEqual(result.errors, []);
  assert.ok(result.warnings.some((w) => w.includes("already in the past")));
  assert.equal(result.summary.calculatedState, "Live Now");
});

// --- DropSubmissionController: confirm double-click, API failure, success clears once ---

test("confirm button double-click: a second concurrent confirm() call is a no-op while in flight", async () => {
  let submitCalls = 0;
  let resolveFirst;
  const submitFn = () =>
    new Promise((resolve) => {
      submitCalls++;
      resolveFirst = () => resolve({ dropId: "abc123" });
    });
  const disabledStates = [];
  const controller = new DropSubmissionController({
    submitFn,
    setDisabled: (v) => disabledStates.push(v),
  });
  controller.stagePayload({ name: "Test" }, "admdrop");

  const firstCall = controller.confirm();
  const secondCall = controller.confirm(); // fired before first resolves
  assert.equal(submitCalls, 1, "second overlapping confirm() must not call submitFn again");
  resolveFirst();
  await Promise.all([firstCall, secondCall]);
  assert.deepEqual(disabledStates, [true, false]);
});

test("API failure preserves the staged payload so the form can be resubmitted", async () => {
  const submitFn = async () => {
    throw new Error("network error");
  };
  let errorSeen = null;
  let successCalled = false;
  const controller = new DropSubmissionController({
    submitFn,
    onError: (err) => {
      errorSeen = err;
    },
    onSuccess: () => {
      successCalled = true;
    },
  });
  const payload = { name: "Test Drop", assignments: [{ username: "@a", code: "X" }] };
  controller.stagePayload(payload, "admdrop");

  await controller.confirm();

  assert.equal(successCalled, false);
  assert.ok(errorSeen instanceof Error);
  assert.deepEqual(controller.pendingPayload, payload, "payload must remain staged after failure");
  assert.equal(controller.inFlight, false, "button must be re-enabled after failure");
});

test("successful creation clears the form exactly once", async () => {
  const submitFn = async () => ({ dropId: "drop_1", status: "ok" });
  let clearCount = 0;
  let successData = null;
  const controller = new DropSubmissionController({
    submitFn,
    onSuccess: (data) => {
      clearCount++;
      successData = data;
    },
  });
  controller.stagePayload({ name: "Test Drop" }, "admdrop");

  await controller.confirm();

  assert.equal(clearCount, 1);
  assert.equal(successData.dropId, "drop_1");
  assert.equal(controller.pendingPayload, null, "pending payload must be cleared after success");

  // Calling confirm again with nothing staged must not re-trigger success/clear.
  await controller.confirm();
  assert.equal(clearCount, 1, "confirm() with no staged payload must be a no-op");
});

test("idempotency key is generated once per staged payload and reused for the submitted request", async () => {
  const seenKeys = [];
  const submitFn = async (payload) => {
    seenKeys.push(payload.idempotency_key);
    return { dropId: "drop_1" };
  };
  const controller = new DropSubmissionController({ submitFn });
  controller.stagePayload({ name: "Test Drop" }, "admdrop");
  const key = controller.pendingIdempotencyKey;
  assert.ok(key && key.startsWith("admdrop:"));

  await controller.confirm();

  assert.equal(seenKeys.length, 1);
  assert.equal(seenKeys[0], key);
});
