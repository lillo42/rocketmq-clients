/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Proto = Apache.Rocketmq.V2;
using grpcLib = Grpc.Core;

[assembly: InternalsVisibleTo("tests")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
namespace Org.Apache.Rocketmq
{
    public abstract partial class Client
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<Client>();

        private static readonly TimeSpan HeartbeatScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan HeartbeatSchedulePeriod = TimeSpan.FromSeconds(10);
        private readonly CancellationTokenSource _heartbeatCts;

        private static readonly TimeSpan TopicRouteUpdateScheduleDelay = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan TopicRouteUpdateSchedulePeriod = TimeSpan.FromSeconds(30);
        private readonly CancellationTokenSource _topicRouteUpdateCts;

        private static readonly TimeSpan SettingsSyncScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan SettingsSyncSchedulePeriod = TimeSpan.FromMinutes(5);
        private readonly CancellationTokenSource _settingsSyncCts;

        private static readonly TimeSpan StatsScheduleDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan StatsSchedulePeriod = TimeSpan.FromSeconds(60);
        private readonly CancellationTokenSource _statsCts;

        protected readonly ClientConfig ClientConfig;
        protected readonly Endpoints Endpoints;
        protected IClientManager ClientManager;
        protected readonly string ClientId;
        protected readonly ClientMeterManager ClientMeterManager;

        protected readonly ConcurrentDictionary<Endpoints, bool> Isolated;
        private readonly ConcurrentDictionary<string, TopicRouteData> _topicRouteCache;

        private readonly Dictionary<Endpoints, Session> _sessionsTable;
        private readonly ReaderWriterLockSlim _sessionLock;

        internal volatile State State;

        protected Client(ClientConfig clientConfig)
        {
            ClientConfig = clientConfig;
            Endpoints = new Endpoints(clientConfig.Endpoints);
            ClientId = Utilities.GetClientId();
            ClientMeterManager = new ClientMeterManager(this);

            ClientManager = new ClientManager(this);
            Isolated = new ConcurrentDictionary<Endpoints, bool>();
            _topicRouteCache = new ConcurrentDictionary<string, TopicRouteData>();

            _topicRouteUpdateCts = new CancellationTokenSource();
            _settingsSyncCts = new CancellationTokenSource();
            _heartbeatCts = new CancellationTokenSource();
            _statsCts = new CancellationTokenSource();

            _sessionsTable = new Dictionary<Endpoints, Session>();
            _sessionLock = new ReaderWriterLockSlim();

            State = State.New;
        }

        protected virtual async Task Start()
        {
            LogMessages.LogStartBegin(Logger, ClientId);
            foreach (var topic in GetTopics())
            {
                await FetchTopicRoute(topic);
            }

            ScheduleWithFixedDelay(UpdateTopicRouteCache, TopicRouteUpdateScheduleDelay,
                TopicRouteUpdateSchedulePeriod, _topicRouteUpdateCts.Token);
            ScheduleWithFixedDelay(Heartbeat, HeartbeatScheduleDelay, HeartbeatSchedulePeriod, _heartbeatCts.Token);
            ScheduleWithFixedDelay(SyncSettings, SettingsSyncScheduleDelay, SettingsSyncSchedulePeriod,
                _settingsSyncCts.Token);
            ScheduleWithFixedDelay(Stats, StatsScheduleDelay, StatsSchedulePeriod, _statsCts.Token);
            LogMessages.LogStartSuccess(Logger, ClientId);
        }

        protected virtual async Task Shutdown()
        {
            LogMessages.LogShutdownBegin(Logger, ClientId);
            _heartbeatCts.Cancel();
            _topicRouteUpdateCts.Cancel();
            _settingsSyncCts.Cancel();
            _statsCts.Cancel();
            NotifyClientTermination();
            await ClientManager.Shutdown();
            ClientMeterManager.Shutdown();
            LogMessages.LogShutdownSuccess(Logger, ClientId);
        }

        private protected (bool, Session) GetSession(Endpoints endpoints)
        {
            _sessionLock.EnterReadLock();
            try
            {
                // Session exists, return in advance.
                if (_sessionsTable.TryGetValue(endpoints, out var session))
                {
                    return (false, session);
                }
            }
            finally
            {
                _sessionLock.ExitReadLock();
            }

            _sessionLock.EnterWriteLock();
            try
            {
                // Session exists, return in advance.
                if (_sessionsTable.TryGetValue(endpoints, out var session))
                {
                    return (false, session);
                }

                var stream = ClientManager.Telemetry(endpoints);
                var created = new Session(endpoints, stream, this);
                _sessionsTable.Add(endpoints, created);
                return (true, created);
            }
            finally
            {
                _sessionLock.ExitWriteLock();
            }
        }

        protected abstract IEnumerable<string> GetTopics();

        internal abstract Proto::HeartbeatRequest WrapHeartbeatRequest();

        protected abstract void OnTopicRouteDataUpdated0(string topic, TopicRouteData topicRouteData);

        internal async Task OnTopicRouteDataFetched(string topic, TopicRouteData topicRouteData)
        {
            var routeEndpoints = new HashSet<Endpoints>();
            foreach (var mq in topicRouteData.MessageQueues)
            {
                routeEndpoints.Add(mq.Broker.Endpoints);
            }

            var existedRouteEndpoints = GetTotalRouteEndpoints();
            var newEndpoints = routeEndpoints.Except(existedRouteEndpoints);

            foreach (var endpoints in newEndpoints)
            {
                var (created, session) = GetSession(endpoints);
                if (!created)
                {
                    continue;
                }

                LogMessages.LogEstablishSessionBegin(Logger, endpoints, ClientId);
                await session.SyncSettings(true);
                LogMessages.LogEstablishSessionSuccess(Logger, endpoints, ClientId);
            }

            _topicRouteCache[topic] = topicRouteData;
            OnTopicRouteDataUpdated0(topic, topicRouteData);
        }

        /**
         * Return all endpoints of brokers in route table.
         */
        private HashSet<Endpoints> GetTotalRouteEndpoints()
        {
            var endpoints = new HashSet<Endpoints>();
            foreach (var item in _topicRouteCache)
            {
                foreach (var endpoint in item.Value.MessageQueues.Select(mq => mq.Broker.Endpoints))
                {
                    endpoints.Add(endpoint);
                }
            }

            return endpoints;
        }

        private async void UpdateTopicRouteCache()
        {
            try
            {
                LogMessages.LogUpdateTopicRouteCacheBegin(Logger, ClientId);
                Dictionary<string, Task<TopicRouteData>> responses = new Dictionary<string, Task<TopicRouteData>>();

                foreach (var topic in GetTopics())
                {
                    var task = FetchTopicRoute(topic);
                    responses[topic] = task;
                }

                foreach (var item in responses.Keys)
                {
                    try
                    {
                        await responses[item];
                    }
                    catch (Exception e)
                    {
                        LogMessages.LogUpdateTopicRouteCacheFailure(Logger, item, e);
                    }
                }
            }
            catch (Exception e)
            {
                LogMessages.LogUpdateTopicRouteCacheUnexpectedException(Logger, ClientId, e);
            }
        }

        private async void SyncSettings()
        {
            try
            {
                var totalRouteEndpoints = GetTotalRouteEndpoints();
                foreach (var endpoints in totalRouteEndpoints)
                {
                    var (_, session) = GetSession(endpoints);
                    await session.SyncSettings(false);
                    LogMessages.LogSyncSettings(Logger, endpoints);
                }
            }
            catch (Exception e)
            {
                LogMessages.LogSyncSettingsUnexpectedException(Logger, ClientId, e);
            }
        }

        private void Stats()
        {
            ThreadPool.GetAvailableThreads(out var availableWorker, out var availableIo);
            LogMessages.LogStats(Logger, ClientId, MetadataConstants.Instance.ClientVersion, Environment.Version.ToString(), ThreadPool.ThreadCount, ThreadPool.CompletedWorkItemCount, ThreadPool.PendingWorkItemCount, availableWorker, availableIo);
        }

        private protected void ScheduleWithFixedDelay(Action action, TimeSpan delay, TimeSpan period, CancellationToken token)
        {
            Task.Run(async () =>
            {
                await Task.Delay(delay, token);
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        action();
                    }
                    catch (Exception e)
                    {
                        LogMessages.LogScheduledTaskFailure(Logger, ClientId, e);
                    }
                    finally
                    {
                        await Task.Delay(period, token);
                    }
                }
            }, token);
        }

        protected async Task<TopicRouteData> GetRouteData(string topic)
        {
            if (_topicRouteCache.TryGetValue(topic, out var topicRouteData))
            {
                return topicRouteData;
            }

            topicRouteData = await FetchTopicRoute(topic);
            return topicRouteData;
        }

        private async Task<TopicRouteData> FetchTopicRoute(string topic)
        {
            var topicRouteData = await FetchTopicRoute0(topic);
            await OnTopicRouteDataFetched(topic, topicRouteData);
            LogMessages.LogFetchTopicRouteSuccess(Logger, ClientId, topic, topicRouteData);
            return topicRouteData;
        }


        private async Task<TopicRouteData> FetchTopicRoute0(string topic)
        {
            try
            {
                var request = new Proto::QueryRouteRequest
                {
                    Topic = new Proto::Resource
                    {
                        ResourceNamespace = ClientConfig.Namespace,
                        Name = topic
                    },
                    Endpoints = Endpoints.ToProtobuf()
                };

                var invocation =
                    await ClientManager.QueryRoute(Endpoints, request, ClientConfig.RequestTimeout);
                var code = invocation.Response.Status.Code;
                if (!Proto.Code.Ok.Equals(code))
                {
                    LogMessages.LogFetchTopicRouteFailure(Logger, ClientId, topic, code, invocation.Response.Status.Message, null);
                }

                StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);

                var messageQueues = invocation.Response.MessageQueues.ToList();
                return new TopicRouteData(messageQueues);
            }
            catch (Exception e)
            {
                LogMessages.LogFetchTopicRouteGeneralFailure(Logger, ClientId, topic, e);
                throw;
            }
        }

        private async void Heartbeat()
        {
            try
            {
                var endpoints = GetTotalRouteEndpoints();
                var request = WrapHeartbeatRequest();
                var invocations =
                    new Dictionary<Endpoints, Task<RpcInvocation<Proto.HeartbeatRequest, Proto.HeartbeatResponse>>>();

                // Collect task into a map.
                foreach (var item in endpoints)
                {
                    var task = ClientManager.Heartbeat(item, request, ClientConfig.RequestTimeout);
                    invocations[item] = task;
                }

                foreach (var item in invocations.Keys)
                {
                    try
                    {
                        var invocation = await invocations[item];
                        var code = invocation.Response.Status.Code;

                        if (code.Equals(Proto.Code.Ok))
                        {
                            LogMessages.LogHeartbeatSuccess(Logger, item, ClientId);
                            if (Isolated.TryRemove(item, out _))
                            {
                                LogMessages.LogRejoinEndpoints(Logger, item, ClientId);
                            }

                            return;
                        }

                        var statusMessage = invocation.Response.Status.Message;
                        LogMessages.LogHeartbeatFailure(Logger, item, code, statusMessage, ClientId);
                    }
                    catch (Exception e)
                    {
                        LogMessages.LogHeartbeatGeneralFailure(Logger, item, e);
                    }
                }
            }
            catch (Exception e)
            {
                LogMessages.LogHeartbeatUnexpectedException(Logger, ClientId, e);
            }
        }

        internal grpcLib.Metadata Sign()
        {
            var metadata = new grpcLib::Metadata();
            Signature.Sign(this, metadata);
            return metadata;
        }

        internal abstract Proto::NotifyClientTerminationRequest WrapNotifyClientTerminationRequest();

        private async void NotifyClientTermination()
        {
            LogMessages.LogNotifyClientTerminationBegin(Logger, ClientId);
            var endpoints = GetTotalRouteEndpoints();
            var request = WrapNotifyClientTerminationRequest();
            foreach (var item in endpoints)
            {
                var invocation =
                    await ClientManager.NotifyClientTermination(item, request, ClientConfig.RequestTimeout);
                try
                {
                    StatusChecker.Check(invocation.Response.Status, request, invocation.RequestId);
                }
                catch (Exception e)
                {
                    LogMessages.LogNotifyClientTerminationFailure(Logger, ClientId, item, e);
                }
            }
        }

        internal abstract Settings GetSettings();

        internal string GetClientId()
        {
            return ClientId;
        }

        internal ClientConfig GetClientConfig()
        {
            return ClientConfig;
        }

        internal IClientManager GetClientManager()
        {
            return ClientManager;
        }

        // Only for testing
        internal void SetClientManager(IClientManager clientManager)
        {
            ClientManager = clientManager;
        }

        internal virtual void OnRecoverOrphanedTransactionCommand(Endpoints endpoints,
            Proto.RecoverOrphanedTransactionCommand command)
        {
            LogMessages.LogIgnoreOrphanedTransactionCommand(Logger, ClientId, endpoints);
        }

        internal virtual async void OnVerifyMessageCommand(Endpoints endpoints, Proto.VerifyMessageCommand command)
        {
            // Only push consumer support message consumption verification.
            LogMessages.LogIgnoreVerifyMessageCommand(Logger, ClientId, endpoints, command);
            var status = new Proto.Status
            {
                Code = Proto.Code.Unsupported,
                Message = "Message consumption verification is not supported"
            };
            var verifyMessageResult = new Proto.VerifyMessageResult
            {
                Nonce = command.Nonce
            };

            var telemetryCommand = new Proto.TelemetryCommand
            {
                VerifyMessageResult = verifyMessageResult,
                Status = status
            };
            var (_, session) = GetSession(endpoints);
            await session.WriteAsync(telemetryCommand);
        }

        internal async void OnPrintThreadStackTraceCommand(Endpoints endpoints,
            Proto.PrintThreadStackTraceCommand command)
        {
            LogMessages.LogIgnoreThreadStackTraceCommand(Logger, ClientId, endpoints);
            var status = new Proto.Status
            {
                Code = Proto.Code.Unsupported,
                Message = "C# don't support thread stack trace printing"
            };
            var threadStackTrace = new Proto.ThreadStackTrace
            {
                Nonce = command.Nonce
            };

            var telemetryCommand = new Proto.TelemetryCommand
            {
                ThreadStackTrace = threadStackTrace,
                Status = status,
            };
            var (_, session) = GetSession(endpoints);
            await session.WriteAsync(telemetryCommand);
        }

        internal void OnSettingsCommand(Endpoints endpoints, Proto.Settings settings)
        {
            var metric = new Metric(settings.Metric ?? new Proto.Metric());
            ClientMeterManager.Reset(metric);
            GetSettings().Sync(settings);
        }
        
        private static partial class LogMessages
        {
            [LoggerMessage(EventId = 1, Level = LogLevel.Debug, Message = "Begin to start the rocketmq client, clientId={ClientId}")]
            public static partial void LogStartBegin(ILogger logger, string clientId);

            [LoggerMessage(EventId = 2, Level = LogLevel.Debug, Message = "Start the rocketmq client successfully, clientId={ClientId}")]
            public static partial void LogStartSuccess(ILogger logger, string clientId);

            [LoggerMessage(EventId = 3, Level = LogLevel.Debug, Message = "Begin to shutdown rocketmq client, clientId={ClientId}")]
            public static partial void LogShutdownBegin(ILogger logger, string clientId);

            [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Shutdown the rocketmq client successfully, clientId={ClientId}")]
            public static partial void LogShutdownSuccess(ILogger logger, string clientId);

            [LoggerMessage(EventId = 5, Level = LogLevel.Information, Message = "Begin to establish session for endpoints={Endpoints}, clientId={ClientId}")]
            public static partial void LogEstablishSessionBegin(ILogger logger, Endpoints endpoints, string clientId);

            [LoggerMessage(EventId = 6, Level = LogLevel.Information, Message = "Establish session for endpoints={Endpoints} successfully, clientId={ClientId}")]
            public static partial void LogEstablishSessionSuccess(ILogger logger, Endpoints endpoints, string clientId);

            [LoggerMessage(EventId = 7, Level = LogLevel.Information, Message = "Start to update topic route cache for a new round, clientId={ClientId}")]
            public static partial void LogUpdateTopicRouteCacheBegin(ILogger logger, string clientId);

            [LoggerMessage(EventId = 8, Level = LogLevel.Error, Message = "Failed to update topic route cache, topic={Topic}")]
            public static partial void LogUpdateTopicRouteCacheFailure(ILogger logger, string topic, Exception exception);

            [LoggerMessage(EventId = 9, Level = LogLevel.Error, Message = "[Bug] unexpected exception raised during topic route cache update, clientId={ClientId}")]
            public static partial void LogUpdateTopicRouteCacheUnexpectedException(ILogger logger, string clientId, Exception exception);

            [LoggerMessage(EventId = 10, Level = LogLevel.Information, Message = "Sync settings to remote, endpoints={Endpoints}")]
            public static partial void LogSyncSettings(ILogger logger, Endpoints endpoints);

            [LoggerMessage(EventId = 11, Level = LogLevel.Error, Message = "[Bug] unexpected exception raised during setting sync, clientId={ClientId}")]
            public static partial void LogSyncSettingsUnexpectedException(ILogger logger, string clientId, Exception exception);

            [LoggerMessage(EventId = 12, Level = LogLevel.Information, Message = "ClientId={ClientId}, ClientVersion={ClientVersion}, .NET Version={DotNetVersion}, ThreadCount={ThreadCount}, CompletedWorkItemCount={CompletedWorkItemCount}, PendingWorkItemCount={PendingWorkItemCount}, AvailableWorkerThreads={AvailableWorkerThreads}, AvailableCompletionPortThreads={AvailableCompletionPortThreads}")]
            public static partial void LogStats(ILogger logger, string clientId, string clientVersion, string dotNetVersion, int threadCount, long completedWorkItemCount, long pendingWorkItemCount, int availableWorkerThreads, int availableCompletionPortThreads);

            [LoggerMessage(EventId = 13, Level = LogLevel.Error, Message = "Failed to execute scheduled task, ClientId={ClientId}")]
            public static partial void LogScheduledTaskFailure(ILogger logger, string clientId, Exception exception);

            [LoggerMessage(EventId = 14, Level = LogLevel.Information, Message = "Fetch topic route successfully, clientId={ClientId}, topic={Topic}, topicRouteData={TopicRouteData}")]
            public static partial void LogFetchTopicRouteSuccess(ILogger logger, string clientId, string topic, TopicRouteData topicRouteData);

            [LoggerMessage(EventId = 15, Level = LogLevel.Error, Message = "Failed to fetch topic route, clientId={ClientId}, topic={Topic}, code={Code}, statusMessage={StatusMessage}")]
            public static partial void LogFetchTopicRouteFailure(ILogger logger, string clientId, string topic, Proto.Code code, string statusMessage, Exception exception);

            [LoggerMessage(EventId = 16, Level = LogLevel.Error, Message = "Failed to fetch topic route, clientId={ClientId}, topic={Topic}")]
            public static partial void LogFetchTopicRouteGeneralFailure(ILogger logger, string clientId, string topic, Exception exception);

            [LoggerMessage(EventId = 17, Level = LogLevel.Information, Message = "Send heartbeat successfully, endpoints={Endpoints}, clientId={ClientId}")]
            public static partial void LogHeartbeatSuccess(ILogger logger, Endpoints endpoints, string clientId);

            [LoggerMessage(EventId = 18, Level = LogLevel.Information, Message = "Rejoin endpoints which was isolated before, endpoints={Endpoints}, clientId={ClientId}")]
            public static partial void LogRejoinEndpoints(ILogger logger, Endpoints endpoints, string clientId);

            [LoggerMessage(EventId = 19, Level = LogLevel.Information, Message = "Failed to send heartbeat, endpoints={Endpoints}, code={Code}, statusMessage={StatusMessage}, clientId={ClientId}")]
            public static partial void LogHeartbeatFailure(ILogger logger, Endpoints endpoints, Proto.Code code, string statusMessage, string clientId);

            [LoggerMessage(EventId = 20, Level = LogLevel.Error, Message = "Failed to send heartbeat, endpoints={Endpoints}")]
            public static partial void LogHeartbeatGeneralFailure(ILogger logger, Endpoints endpoints, Exception exception);

            [LoggerMessage(EventId = 21, Level = LogLevel.Error, Message = "[Bug] unexpected exception raised during heartbeat, clientId={ClientId}")]
            public static partial void LogHeartbeatUnexpectedException(ILogger logger, string clientId, Exception exception);

            [LoggerMessage(EventId = 22, Level = LogLevel.Information, Message = "Notify remote endpoints that current client is terminated, clientId={ClientId}")]
            public static partial void LogNotifyClientTerminationBegin(ILogger logger, string clientId);

            [LoggerMessage(EventId = 23, Level = LogLevel.Error, Message = "Failed to notify client's termination, clientId={ClientId}, endpoints={Endpoints}")]
            public static partial void LogNotifyClientTerminationFailure(ILogger logger, string clientId, Endpoints endpoints, Exception exception);

            [LoggerMessage(EventId = 24, Level = LogLevel.Warning, Message = "Ignore orphaned transaction recovery command from remote, which is not expected, clientId={ClientId}, endpoints={Endpoints}")]
            public static partial void LogIgnoreOrphanedTransactionCommand(ILogger logger, string clientId, Endpoints endpoints);

            [LoggerMessage(EventId = 25, Level = LogLevel.Warning, Message = "Ignore verify message command from remote, which is not expected, clientId={ClientId}, endpoints={Endpoints}, command={Command}")]
            public static partial void LogIgnoreVerifyMessageCommand(ILogger logger, string clientId, Endpoints endpoints, Proto.VerifyMessageCommand command);

            [LoggerMessage(EventId = 26, Level = LogLevel.Warning, Message = "Ignore thread stack trace printing command from remote because it is still not supported, clientId={ClientId}, endpoints={Endpoints}")]
            public static partial void LogIgnoreThreadStackTraceCommand(ILogger logger, string clientId, Endpoints endpoints);
        }
    }
}