// Apify can migrate a long run to another host and restarts the actor from the top with the same
// run id. Without a checkpoint that re-fetched finished variants and wrote duplicate jobs, so the
// progress is kept in the run's key-value store and restored by the new container.
const KEY = 'PROGRESS';

export function restoreProgress(checkpoint, runId) {
    if (!runId || !checkpoint || checkpoint.runId !== runId) return null;

    return {
        nextIndex: Number(checkpoint.nextIndex) || 0,
        saved: Number(checkpoint.saved) || 0,
        requests: Number(checkpoint.requests) || 0,
        seenIds: Array.isArray(checkpoint.seenIds) ? checkpoint.seenIds : [],
        seenSignatures: Array.isArray(checkpoint.seenSignatures) ? checkpoint.seenSignatures : [],
    };
}

export async function saveProgress(store, runId, progress) {
    if (!store || !runId) return;

    await store.setValue(KEY, {
        runId,
        nextIndex: progress.nextIndex,
        saved: progress.saved,
        requests: progress.requests,
        seenIds: [...progress.seenIds],
        seenSignatures: [...progress.seenSignatures],
    });
}
