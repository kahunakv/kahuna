
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
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
                cancellationToken
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
                cancellationToken
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
                cancellationToken
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
                request.Durability, 
                cancellationToken
            );
        
            if (keyValueContext is not null)
            {
                KahunaGetKeyValueResponse response = new()
                {
                    ServedFrom = "",
                    Type = type,
                    Value = keyValueContext.Value,
                    Revision = keyValueContext.Revision,
                    Expires = keyValueContext.Expires
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
                request.Durability, 
                cancellationToken
            );
        
            if (keyValueContext is not null)
            {
                KahunaExistsKeyValueResponse response = new()
                {
                    ServedFrom = "",
                    Type = type,
                    Revision = keyValueContext.Revision,
                    Expires = keyValueContext.Expires
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
            
            KeyValueTransactionResult result = await keyValues.TryExecuteTransactionScript(request.Script, request.Hash, request.Parameters);

            return new KahunaTxKeyValueResponse
            {
                ServedFrom = result.ServedFrom,
                Type = result.Type,
                //Value = result.Value,
                //Revision = result.Revision,
                //Expires = result.Expires,
                Reason = result.Reason
            };
        });
    }
}