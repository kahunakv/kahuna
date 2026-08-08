
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Diagnostics;
using Kommander.Time;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides methods to map HTTP routes for handling key-value operations in the RESTful API.
/// </summary>
/// <remarks>
/// This class is responsible for defining routes related to key-value functionalities,
/// including setting, extending, deleting, retrieving, checking existence,
/// and executing transaction scripts in the context of a key-value store.
/// </remarks>
public static class KeyValuesHandlers
{
    public static void MapKeyValueRoutes(WebApplication app)
    {
        app.MapPost("/v1/kv/try-set", async (KahunaSetKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key) || request.Value is null || request.ExpiresMs < 0)
                return new() { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType response, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTrySetKeyValue(
                request.TransactionId,
                request.Key,
                request.Value,
                request.CompareValue,
                request.CompareRevision,
                request.Flags,
                request.ExpiresMs,
                request.Durability,
                cancellationToken,
                coordinatorKey: request.CoordinatorKey ?? "",
                operationId: new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaSetKeyValueResponse
            {
                Type = response,
                Revision = revision,
                LastModified = lastModified,
            };
        });

        app.MapPost("/v1/kv/try-extend", async (KahunaExtendKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key) || request.ExpiresMs <= 0)
                return new() { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType response, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryExtendKeyValue(
                request.TransactionId,
                request.Key,
                request.ExpiresMs,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaExtendKeyValueResponse
            {
                Type = response,
                Revision = revision,
                LastModified = lastModified
            };

            /*int partitionId = raft.GetPartitionKey(request.Key);

            if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            {
                (KeyValueResponseType response, long revision, HLCTimestamp lastModified) = await keyValues.TryExtendKeyValue(request.TransactionId, request.Key, request.ExpiresMs, request.Durability);

                return new() { Type = response, Revision = revision, LastModified = lastModified };
            }

            string leader = await raft.WaitForLeader(partitionId, cancellationToken);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = KeyValueResponseType.MustRetry };

            logger.LogDebug("EXTEND-KEYVALUE Redirect {LockName} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);

            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaExtendKeyValueRequest);

                KahunaExtendKeyValueResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kv/try-extend")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaExtendKeyValueResponse>();

                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);

                return new() { Type = KeyValueResponseType.Errored };
            }*/
        });

        app.MapPost("/v1/kv/try-delete", async (KahunaDeleteKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new() { Type = KeyValueResponseType.InvalidInput };
            
            (KeyValueResponseType response, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryDeleteKeyValue(
                request.TransactionId,
                request.Key,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaDeleteKeyValueResponse
            {
                Type = response,
                Revision = revision,
                LastModified = lastModified
            };
            
            /*int partitionId = raft.GetPartitionKey(request.Key);

            if (!raft.Joined || await raft.AmILeader(partitionId, cancellationToken))
            {
                (KeyValueResponseType response, long revision) = await keyValues.TryDeleteKeyValue(request.TransactionId, request.Key, request.Durability);

                return new() { Type = response, Revision = revision };
            }
            
            string leader = await raft.WaitForLeader(partitionId, cancellationToken);
            if (leader == raft.GetLocalEndpoint())
                return new() { Type = KeyValueResponseType.MustRetry };
            
            logger.LogDebug("DELETE-KEYVALUE Redirect {LockName} to leader partition {Partition} at {Leader}", request.Key, partitionId, leader);
            
            try
            {
                string payload = JsonSerializer.Serialize(request, KahunaJsonContext.Default.KahunaDeleteKeyValueRequest);
                
                KahunaDeleteKeyValueResponse? response = await $"https://{leader}"
                    .AppendPathSegments("v1/kv/try-delete")
                    .WithHeader("Accept", "application/json")
                    .WithHeader("Content-Type", "application/json")
                    .WithSettings(o => o.HttpVersion = "2.0")
                    .PostStringAsync(payload, cancellationToken: cancellationToken)
                    .ReceiveJson<KahunaDeleteKeyValueResponse>();
                
                if (response is not null)
                    response.ServedFrom = $"https://{leader}";

                return response;
            }
            catch (Exception ex)
            {
                logger.LogError("{Node}: {Name}\n{Message}", leader, ex.GetType().Name, ex.Message);
                
                return new() { Type = KeyValueResponseType.Errored };
            }*/
        });

        app.MapPost("/v1/kv/try-set-many", async (KahunaSetManyKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (request.Items is null)
            {
                return new KahunaSetManyKeyValueResponse
                {
                    Type = KeyValueResponseType.InvalidInput,
                    Items =
                    [
                        new KahunaSetKeyValueResponseItem
                        {
                            Type = KeyValueResponseType.InvalidInput
                        }
                    ],
                    TimeElapsedMs = 0
                };
            }

            // The set-many wire carries no registration identity, so a transactional item cannot register
            // with the coordinator and its write would be silently invisible to commit. Reject it rather
            // than accept an operation that can never commit.
            if (request.Items.Any(static i => i.TransactionId != HLCTimestamp.Zero))
            {
                return new KahunaSetManyKeyValueResponse
                {
                    Type = KeyValueResponseType.InvalidInput,
                    Items = request.Items.Select(static i => new KahunaSetKeyValueResponseItem
                    {
                        Key = i.Key ?? "",
                        Durability = i.Durability,
                        Type = KeyValueResponseType.InvalidInput
                    }).ToList(),
                    TimeElapsedMs = 0
                };
            }

            ValueStopwatch stopwatch = ValueStopwatch.StartNew();

            List<KahunaSetKeyValueResponseItem> responses = await keyValues.LocateAndTrySetManyKeyValue(
                request.Items,
                cancellationToken
            );

            return new KahunaSetManyKeyValueResponse
            {
                Type = KeyValueResponseType.Set,
                Items = responses,
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };
        });

        app.MapPost("/v1/kv/try-delete-many", async (KahunaDeleteManyKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (request.Items is null)
            {
                return new KahunaDeleteManyKeyValueResponse
                {
                    Type = KeyValueResponseType.InvalidInput,
                    Items =
                    [
                        new KahunaDeleteKeyValueResponseItem
                        {
                            Type = KeyValueResponseType.InvalidInput
                        }
                    ],
                    TimeElapsedMs = 0
                };
            }

            // Delete-many can register (batch-level identity); a transactional batch without it cannot
            // register with the coordinator, so its effect would be silently invisible to commit.
            TransactionOperationId deleteOperationId = new(request.OperationIdHigh, request.OperationIdLow);
            if ((string.IsNullOrEmpty(request.CoordinatorKey) || deleteOperationId.IsEmpty) &&
                request.Items.Any(static i => i.TransactionId != HLCTimestamp.Zero))
            {
                return new KahunaDeleteManyKeyValueResponse
                {
                    Type = KeyValueResponseType.InvalidInput,
                    Items = request.Items.Select(static i => new KahunaDeleteKeyValueResponseItem
                    {
                        Key = i.Key ?? "",
                        Durability = i.Durability,
                        Type = KeyValueResponseType.InvalidInput
                    }).ToList(),
                    TimeElapsedMs = 0
                };
            }

            ValueStopwatch stopwatch = ValueStopwatch.StartNew();

            List<KahunaDeleteKeyValueResponseItem> responses = await keyValues.LocateAndTryDeleteManyKeyValue(
                request.Items,
                cancellationToken,
                request.CoordinatorKey ?? "",
                deleteOperationId
            );

            return new KahunaDeleteManyKeyValueResponse
            {
                Type = KeyValueResponseType.Deleted,
                Items = responses,
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };
        });

        app.MapPost("/v1/kv/try-get", async (KahunaGetKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key) || string.IsNullOrEmpty(request.Key))
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? keyValueContext) = await keyValues.LocateAndTryGetValue(
                request.TransactionId,
                request.Key,
                request.Revision,
                request.ReadTimestamp,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );
        
            if (keyValueContext is not null)
            {
                KahunaGetKeyValueResponse response = new()
                {
                    ServedFrom = "",
                    Type = type,
                    Value = keyValueContext.Value,
                    Revision = keyValueContext.Revision,
                    Expires = keyValueContext.Expires,
                    LastModified = keyValueContext.LastModified
                };
                
                return response;
            }

            return new()
            {
                Type = type
            };
        });
        
        app.MapPost("/v1/kv/try-exists", async (KahunaExistsKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key) || string.IsNullOrEmpty(request.Key))
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? keyValueContext) = await keyValues.LocateAndTryExistsValue(
                request.TransactionId,
                request.Key,
                request.Revision,
                request.ReadTimestamp,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );
        
            if (keyValueContext is not null)
            {
                KahunaExistsKeyValueResponse response = new()
                {
                    ServedFrom = "",
                    Type = type,
                    Revision = keyValueContext.Revision,
                    Expires = keyValueContext.Expires,
                    LastModified = keyValueContext.LastModified
                };
                
                return response;
            }

            return new()
            {
                Type = type
            };
        });
        
        app.MapPost("/v1/kv/try-execute-tx-script", async (KahunaTxKeyValueRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (request.Script is null)
                return new()
                {
                    Type = KeyValueResponseType.InvalidInput
                };
            
            KeyValueTransactionResult result = await keyValues.TryExecuteTransactionScript(request.Script, request.Hash, request.Parameters, request.Priority);

            // Carry the per-value results (key, value, revision, timestamps) with the same fidelity
            // as the gRPC script wire; the legacy scalar fields carry the first value for older clients.
            List<KahunaTxKeyValueResponseItem>? values = null;

            if (result.Values is { Count: > 0 })
            {
                values = new(result.Values.Count);

                foreach (KeyValueTransactionResultValue value in result.Values)
                    values.Add(new()
                    {
                        Key = value.Key,
                        Value = value.Value,
                        Revision = value.Revision,
                        Expires = value.Expires,
                        LastModified = value.LastModified
                    });
            }

            return new KahunaTxKeyValueResponse
            {
                ServedFrom = result.ServedFrom,
                Type = result.Type,
                Value = result.Value,
                Revision = result.Revision,
                Reason = result.Reason,
                Values = values
            };
        });

        app.MapPost("/v1/kv/try-get-many", async (KahunaManyKeyValuesRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (request.Items is null)
                return new KahunaManyKeyValuesResponse { Type = KeyValueResponseType.InvalidInput, Items = [], TimeElapsedMs = 0 };

            // No registration identity on this wire: a transactional batch read would skip read-set
            // registration and silently weaken isolation, so it is rejected outright.
            if (request.TransactionId != HLCTimestamp.Zero)
                return RejectTransactionalManyKeyValues(request);

            ValueStopwatch stopwatch = ValueStopwatch.StartNew();

            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = await keyValues.LocateAndTryGetManyValues(
                request.TransactionId,
                request.ReadTimestamp,
                GetManyRequestKeys(request.Items),
                cancellationToken
            );

            return new KahunaManyKeyValuesResponse
            {
                Type = KeyValueResponseType.Get,
                Items = GetManyResponseItems(responses, includeValues: true),
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };
        });

        app.MapPost("/v1/kv/try-exists-many", async (KahunaManyKeyValuesRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (request.Items is null)
                return new KahunaManyKeyValuesResponse { Type = KeyValueResponseType.InvalidInput, Items = [], TimeElapsedMs = 0 };

            // Same rejection as try-get-many: no identity on this wire means a transactional batch
            // existence check cannot register its read set.
            if (request.TransactionId != HLCTimestamp.Zero)
                return RejectTransactionalManyKeyValues(request);

            ValueStopwatch stopwatch = ValueStopwatch.StartNew();

            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses = await keyValues.LocateAndTryExistsManyValues(
                request.TransactionId,
                request.ReadTimestamp,
                GetManyRequestKeys(request.Items),
                cancellationToken
            );

            return new KahunaManyKeyValuesResponse
            {
                Type = KeyValueResponseType.Get,
                Items = GetManyResponseItems(responses, includeValues: false),
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };
        });

        app.MapPost("/v1/kv/get-by-range", async (KahunaGetByRangeRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Prefix))
                return new KahunaGetByRangeResponse { Type = KeyValueResponseType.InvalidInput };

            string? startKey = request.StartKey;
            bool startInclusive = startKey is null || request.StartInclusive;
            HLCTimestamp readTimestamp = request.ReadTimestamp;

            // A cursor supersedes the caller's start bound: it resumes exclusively past the last key
            // already returned and restores the snapshot the first page fixed, so every page of one
            // scan observes a single consistent view. Decoding stays server-side — the cursor is
            // opaque to clients, which merely echo it back.
            if (!string.IsNullOrEmpty(request.Cursor))
            {
                if (!KeyValueRangeCursor.TryDecode(request.Cursor, out string lastKey, out _, out _, out HLCTimestamp cursorTs))
                    return new KahunaGetByRangeResponse { Type = KeyValueResponseType.InvalidInput };

                startKey = lastKey;
                startInclusive = false;
                readTimestamp = cursorTs;
            }

            KeyValueGetByRangeResult result = await keyValues.LocateAndGetByRange(
                request.TransactionId,
                request.Prefix,
                startKey,
                startInclusive,
                request.EndKey,
                request.EndKey is not null && request.EndInclusive,
                request.Limit,
                readTimestamp,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaGetByRangeResponse
            {
                Type = result.Type,
                Items = GetBucketItems(result.Items),
                NextCursor = result.NextCursor,
                HasMore = result.HasMore
            };
        });

        app.MapPost("/v1/kv/get-by-bucket", async (KahunaGetByBucketRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.PrefixKey))
                return new KahunaGetByBucketResponse { Type = KeyValueResponseType.InvalidInput };

            KeyValueGetByBucketResult result = await keyValues.LocateAndGetByBucket(
                request.TransactionId,
                request.PrefixKey,
                request.ReadTimestamp,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaGetByBucketResponse
            {
                Type = result.Type,
                Items = GetBucketItems(result.Items)
            };
        });

        app.MapPost("/v1/kv/scan-all-by-prefix", async (KahunaScanAllByPrefixRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.PrefixKey))
                return new KahunaGetByBucketResponse { Type = KeyValueResponseType.InvalidInput };

            KeyValueGetByBucketResult result = await keyValues.ScanAllByPrefix(
                request.PrefixKey,
                request.ReadTimestamp,
                request.Durability,
                cancellationToken
            );

            return new KahunaGetByBucketResponse
            {
                Type = result.Type,
                Items = GetBucketItems(result.Items)
            };
        });

        app.MapPost("/v1/kv/try-acquire-exclusive-lock", async (KahunaAcquireKeyValueLockRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new KahunaKeyValueLockResponse { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType type, string _, KeyValueDurability _, HLCTimestamp holder) = await keyValues.LocateAndTryAcquireExclusiveLock(
                request.TransactionId,
                request.Key,
                request.ExpiresMs,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaKeyValueLockResponse { Type = type, HolderTransactionId = holder };
        });

        app.MapPost("/v1/kv/try-acquire-prefix-lock", async (KahunaAcquireKeyValueLockRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new KahunaKeyValueLockResponse { Type = KeyValueResponseType.InvalidInput };

            KeyValueResponseType type = await keyValues.LocateAndTryAcquireExclusivePrefixLock(
                request.TransactionId,
                request.Key,
                request.ExpiresMs,
                request.Durability,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaKeyValueLockResponse { Type = type };
        });

        app.MapPost("/v1/kv/try-release-prefix-lock", async (KahunaReleaseKeyValueLockRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Key))
                return new KahunaKeyValueLockResponse { Type = KeyValueResponseType.InvalidInput };

            KeyValueResponseType type = await keyValues.LocateAndTryReleaseExclusivePrefixLock(
                request.TransactionId,
                request.Key,
                request.Durability,
                cancellationToken
            );

            return new KahunaKeyValueLockResponse { Type = type };
        });

        app.MapPost("/v1/kv/try-acquire-range-lock", async (KahunaAcquireRangeLockRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Prefix))
                return new KahunaKeyValueLockResponse { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType type, HLCTimestamp holder) = await keyValues.LocateAndTryAcquireRangeLock(
                request.TransactionId,
                request.Prefix,
                request.StartKey,
                request.StartInclusive,
                request.EndKey,
                request.EndInclusive,
                request.ExpiresMs,
                request.Durability,
                request.Mode,
                cancellationToken,
                request.CoordinatorKey ?? "",
                new TransactionOperationId(request.OperationIdHigh, request.OperationIdLow)
            );

            return new KahunaKeyValueLockResponse { Type = type, HolderTransactionId = holder };
        });

        app.MapPost("/v1/kv/try-release-range-lock", async (KahunaReleaseRangeLockRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.Prefix))
                return new KahunaKeyValueLockResponse { Type = KeyValueResponseType.InvalidInput };

            KeyValueResponseType type = await keyValues.LocateAndTryReleaseExclusiveRangeLock(
                request.TransactionId,
                request.Prefix,
                request.StartKey,
                request.StartInclusive,
                request.EndKey,
                request.EndInclusive,
                request.Durability,
                cancellationToken
            );

            return new KahunaKeyValueLockResponse { Type = type };
        });

        app.MapPost("/v1/kv/start-tx-session", async (KahunaStartTransactionRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.CoordinatorKey))
                return new KahunaStartTransactionResponse { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType type, TransactionHandle handle) = await keyValues.LocateAndStartTransaction(new()
            {
                CoordinatorKey = request.CoordinatorKey,
                Locking = request.LockingType,
                Timeout = request.Timeout,
                AsyncRelease = request.AsyncRelease,
                AutoCommit = request.AutoCommit,
                ReadValidation = request.ReadValidation,
                DecisionDurability = request.DecisionDurability,
                Priority = request.Priority,
                ReadTimestamp = request.ReadTimestamp,
                AdmissionWaitMs = request.AdmissionWaitMs
            }, cancellationToken);

            return new KahunaStartTransactionResponse { Type = type, TransactionId = handle.TransactionId };
        });

        app.MapPost("/v1/kv/commit-tx-session", async (KahunaCommitTransactionRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.CoordinatorKey))
                return new KahunaCommitTransactionResponse { Type = KeyValueResponseType.InvalidInput };

            // The caller's anchor is the only route to the durable decision once the coordinating
            // session is gone, so a retry that supplies it resolves the outcome instead of erroring.
            TransactionHandle handle = new(request.TransactionId, request.CoordinatorKey, request.RecordAnchorKey);

            (KeyValueResponseType type, string? recordAnchorKey) = await keyValues.LocateAndCommitTransaction(handle, cancellationToken);

            return new KahunaCommitTransactionResponse { Type = type, RecordAnchorKey = recordAnchorKey };
        });

        app.MapPost("/v1/kv/rollback-tx-session", async (KahunaCommitTransactionRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.CoordinatorKey))
                return new KahunaCommitTransactionResponse { Type = KeyValueResponseType.InvalidInput };

            // Carrying the anchor lets a rollback retry consult the durable decision: a decided commit
            // must not be undone by a late rollback.
            TransactionHandle handle = new(request.TransactionId, request.CoordinatorKey, request.RecordAnchorKey);

            KeyValueResponseType type = await keyValues.LocateAndRollbackTransaction(handle, cancellationToken);

            return new KahunaCommitTransactionResponse { Type = type };
        });

        app.MapPost("/v1/kv/snapshot-hold/acquire", async (KahunaAcquireSnapshotHoldRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.HolderId))
                return new KahunaAcquireSnapshotHoldResponse { Type = KeyValueResponseType.InvalidInput };

            if (request.LeaseMs <= 0)
                return new KahunaAcquireSnapshotHoldResponse { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
                await keyValues.LocateAndAcquireSnapshotHold(request.HolderId, request.Timestamp, request.LeaseMs, cancellationToken);

            return new KahunaAcquireSnapshotHoldResponse { Type = type, HoldId = holdId, LeaseExpiry = leaseExpiry };
        });

        app.MapPost("/v1/kv/snapshot-hold/renew", async (KahunaRenewSnapshotHoldRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.HoldId))
                return new KahunaRenewSnapshotHoldResponse { Type = KeyValueResponseType.InvalidInput };

            if (request.LeaseMs <= 0)
                return new KahunaRenewSnapshotHoldResponse { Type = KeyValueResponseType.InvalidInput };

            (KeyValueResponseType type, HLCTimestamp leaseExpiry) =
                await keyValues.LocateAndRenewSnapshotHold(request.HoldId, request.LeaseMs, cancellationToken);

            return new KahunaRenewSnapshotHoldResponse { Type = type, LeaseExpiry = leaseExpiry };
        });

        app.MapPost("/v1/kv/snapshot-hold/release", async (KahunaReleaseSnapshotHoldRequest request, IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            if (string.IsNullOrEmpty(request.HoldId))
                return new KahunaReleaseSnapshotHoldResponse { Type = KeyValueResponseType.InvalidInput };

            KeyValueResponseType type =
                await keyValues.LocateAndReleaseSnapshotHold(request.HoldId, cancellationToken);

            return new KahunaReleaseSnapshotHoldResponse { Type = type };
        });

        app.MapGet("/v1/kv/snapshot-floor", async (IKahuna keyValues, CancellationToken cancellationToken) =>
        {
            (KeyValueResponseType type, HLCTimestamp floor, int liveHolds) = await keyValues.GetSnapshotFloor(cancellationToken);
            return new KahunaGetSnapshotFloorResponse { Type = type, EffectiveFloor = floor, LiveHolds = liveHolds };
        });
    }

    /// <summary>
    /// Rejection response for a transactional batch read arriving without registration identity: one
    /// InvalidInput item per requested key, so callers correlating by key see every entry rejected.
    /// </summary>
    private static KahunaManyKeyValuesResponse RejectTransactionalManyKeyValues(KahunaManyKeyValuesRequest request)
    {
        return new()
        {
            Type = KeyValueResponseType.InvalidInput,
            Items = request.Items!.Select(static i => new KahunaGetManyKeyValuesResponseItem
            {
                Key = i.Key ?? "",
                Durability = i.Durability,
                Type = KeyValueResponseType.InvalidInput
            }).ToList(),
            TimeElapsedMs = 0
        };
    }

    private static List<(string key, long revision, KeyValueDurability durability)> GetManyRequestKeys(List<KahunaGetManyKeyValuesRequestItem> items)
    {
        List<(string, long, KeyValueDurability)> keys = new(items.Count);

        foreach (KahunaGetManyKeyValuesRequestItem item in items)
            keys.Add((item.Key ?? "", item.Revision, item.Durability));

        return keys;
    }

    /// <summary>
    /// Projects batched read results onto the wire items. Exists-many omits the payload: the caller
    /// asked whether the key is there, and shipping the value back would make an existence probe cost
    /// as much as a read.
    /// </summary>
    private static List<KahunaGetManyKeyValuesResponseItem> GetManyResponseItems(
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> responses,
        bool includeValues)
    {
        List<KahunaGetManyKeyValuesResponseItem> items = new(responses.Count);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in responses)
        {
            items.Add(new()
            {
                Key = key,
                Type = type,
                Value = includeValues ? entry?.Value : null,
                Revision = entry?.Revision ?? 0,
                LastModified = entry?.LastModified ?? HLCTimestamp.Zero,
                Durability = durability
            });
        }

        return items;
    }

    private static List<KeyValueGetByBucketItem> GetBucketItems(List<(string, ReadOnlyKeyValueEntry)> results)
    {
        List<KeyValueGetByBucketItem> items = new(results.Count);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in results)
        {
            items.Add(new()
            {
                Key = key,
                Value = entry.Value,
                Revision = entry.Revision,
                LastModified = entry.LastModified
            });
        }

        return items;
    }
}
