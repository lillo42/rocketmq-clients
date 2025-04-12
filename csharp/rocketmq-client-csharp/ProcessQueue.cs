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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Rocketmq.V2;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Org.Apache.Rocketmq.Error;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Process queue is a cache to store fetched messages from remote for <c>PushConsumer</c>.
    /// 
    /// <c>PushConsumer</c> queries assignments periodically and converts them into message queues, each message queue is
    /// mapped into one process queue to fetch message from remote. If the message queue is removed from the newest 
    /// assignment, the corresponding process queue is marked as expired soon, which means its lifecycle is over.
    /// </summary>
    public partial class ProcessQueue
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<ProcessQueue>();

        internal static readonly TimeSpan AckMessageFailureBackoffDelay = TimeSpan.FromSeconds(1);
        internal static readonly TimeSpan ChangeInvisibleDurationFailureBackoffDelay = TimeSpan.FromSeconds(1);
        internal static readonly TimeSpan ForwardMessageToDeadLetterQueueFailureBackoffDelay = TimeSpan.FromSeconds(1);

        private static readonly TimeSpan ReceivingFlowControlBackoffDelay = TimeSpan.FromMilliseconds(20);
        private static readonly TimeSpan ReceivingFailureBackoffDelay = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan ReceivingBackoffDelayWhenCacheIsFull = TimeSpan.FromSeconds(1);

        private readonly PushConsumer _consumer;

        /// <summary>
        /// Dropped means ProcessQueue is deprecated, which means no message would be fetched from remote anymore.
        /// </summary>
        private volatile bool _dropped;
        private readonly MessageQueue _mq;
        private readonly FilterExpression _filterExpression;

        /// <summary>
        /// Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
        /// </summary>
        private readonly List<MessageView> _cachedMessages;
        private readonly ReaderWriterLockSlim _cachedMessageLock;
        private long _cachedMessagesBytes;

        private long _activityTime = DateTime.UtcNow.Ticks;
        private long _cacheFullTime = long.MinValue;

        private readonly CancellationTokenSource _receiveMsgCts;
        private readonly CancellationTokenSource _ackMsgCts;
        private readonly CancellationTokenSource _changeInvisibleDurationCts;
        private readonly CancellationTokenSource _forwardMessageToDeadLetterQueueCts;

        public ProcessQueue(PushConsumer consumer, MessageQueue mq, FilterExpression filterExpression,
            CancellationTokenSource receiveMsgCts, CancellationTokenSource ackMsgCts,
            CancellationTokenSource changeInvisibleDurationCts, CancellationTokenSource forwardMessageToDeadLetterQueueCts)
        {
            _consumer = consumer;
            _dropped = false;
            _mq = mq;
            _filterExpression = filterExpression;
            _cachedMessages = new List<MessageView>();
            _cachedMessageLock = new ReaderWriterLockSlim();
            _cachedMessagesBytes = 0;
            _receiveMsgCts = receiveMsgCts;
            _ackMsgCts = ackMsgCts;
            _changeInvisibleDurationCts = changeInvisibleDurationCts;
            _forwardMessageToDeadLetterQueueCts = forwardMessageToDeadLetterQueueCts;
        }

        /// <summary>
        /// Get the mapped message queue.
        /// </summary>
        /// <returns>mapped message queue.</returns>
        public MessageQueue GetMessageQueue()
        {
            return _mq;
        }

        /// <summary>
        /// Drop the current process queue, which means the process queue's lifecycle is over,
        /// thus it would not fetch messages from the remote anymore if dropped.
        /// </summary>
        public void Drop()
        {
            _dropped = true;
        }

        /// <summary>
        /// ProcessQueue would be regarded as expired if no fetch message for a long time.
        /// </summary>
        /// <returns>if it is expired.</returns>
        public bool Expired()
        {
            var longPollingTimeout = _consumer.GetPushConsumerSettings().GetLongPollingTimeout();
            var requestTimeout = _consumer.GetClientConfig().RequestTimeout;
            var maxIdleDuration = longPollingTimeout.Add(requestTimeout).Multiply(3);
            var idleDuration = DateTime.UtcNow.Ticks - Interlocked.Read(ref _activityTime);
            if (idleDuration < maxIdleDuration.Ticks)
            {
                return false;
            }
            var afterCacheFullDuration = DateTime.UtcNow.Ticks - Interlocked.Read(ref _cacheFullTime);
            if (afterCacheFullDuration < maxIdleDuration.Ticks)
            {
                return false;
            }
            LogMessages.ProcessQueueIdle(Logger, idleDuration, maxIdleDuration, afterCacheFullDuration, _mq, _consumer.GetClientId());
            return true;
        }

        internal void CacheMessages(List<MessageView> messageList)
        {
            _cachedMessageLock.EnterWriteLock();
            try
            {
                foreach (var messageView in messageList)
                {
                    _cachedMessages.Add(messageView);
                    Interlocked.Add(ref _cachedMessagesBytes, messageView.Body.Length);
                }
            }
            finally
            {
                _cachedMessageLock.ExitWriteLock();
            }
        }

        private int GetReceptionBatchSize()
        {
            var bufferSize = _consumer.CacheMessageCountThresholdPerQueue() - CachedMessagesCount();
            bufferSize = Math.Max(bufferSize, 1);
            return Math.Min(bufferSize, _consumer.GetPushConsumerSettings().GetReceiveBatchSize());
        }

        /// <summary>
        /// Start to fetch messages from remote immediately.
        /// </summary>
        public void FetchMessageImmediately()
        {
            ReceiveMessageImmediately();
        }

        /// <summary>
        /// Receive message later by message queue.
        /// </summary>
        /// <remarks>
        /// Make sure that no exception will be thrown.
        /// </remarks>
        public void OnReceiveMessageException(Exception t, string attemptId)
        {
            var delay = t is TooManyRequestsException ? ReceivingFlowControlBackoffDelay : ReceivingFailureBackoffDelay;
            ReceiveMessageLater(delay, attemptId);
        }

        private void ReceiveMessageLater(TimeSpan delay, string attemptId)
        {
            var clientId = _consumer.GetClientId();
            LogMessages.TryToReceiveMessageLater(Logger, _mq, delay, clientId);
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(delay, _receiveMsgCts.Token);
                    ReceiveMessage(attemptId);
                }
                catch (Exception ex)
                {
                    if (_receiveMsgCts.IsCancellationRequested)
                    {
                        return;
                    }
                    LogMessages.FailedToScheduleMessageReceivingRequest(Logger, _mq, clientId);
                    OnReceiveMessageException(ex, attemptId);
                }
            });
        }

        private string GenerateAttemptId()
        {
            return Guid.NewGuid().ToString();
        }

        public void ReceiveMessage()
        {
            ReceiveMessage(GenerateAttemptId());
        }

        public void ReceiveMessage(string attemptId)
        {
            var clientId = _consumer.GetClientId();
            if (_dropped)
            {
                LogMessages.ProcessQueueDropped(Logger, _mq, clientId);
                return;
            }
            if (IsCacheFull())
            {
                LogMessages.CacheMessagesQuantityExceedsThreshold(Logger, _consumer.CacheMessageCountThresholdPerQueue(), CachedMessagesCount(), _mq, clientId);
                ReceiveMessageLater(ReceivingBackoffDelayWhenCacheIsFull, attemptId);
                return;
            }
            ReceiveMessageImmediately(attemptId);
        }

        private void ReceiveMessageImmediately()
        {
            ReceiveMessageImmediately(GenerateAttemptId());
        }

        private void ReceiveMessageImmediately(string attemptId)
        {
            var clientId = _consumer.GetClientId();
            if (_consumer.State != State.Running)
            {
                LogMessages.ConsumerNotRunning(Logger, _mq, clientId);
                return;
            }

            try
            {
                var endpoints = _mq.Broker.Endpoints;
                var batchSize = GetReceptionBatchSize();
                var longPollingTimeout = _consumer.GetPushConsumerSettings().GetLongPollingTimeout();
                var request = _consumer.WrapReceiveMessageRequest(batchSize, _mq, _filterExpression, longPollingTimeout,
                    attemptId);

                Interlocked.Exchange(ref _activityTime, DateTime.UtcNow.Ticks);

                var task = _consumer.ReceiveMessage(request, _mq, longPollingTimeout);
                task.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        string nextAttemptId = null;
                        if (t.Exception is { InnerException: RpcException { StatusCode: StatusCode.DeadlineExceeded } })
                        {
                            nextAttemptId = request.AttemptId;
                        }
                        
                        LogMessages.ExceptionRaisedDuringMessageReception(Logger, t.Exception, _mq, request.AttemptId, nextAttemptId, clientId);
                        OnReceiveMessageException(t.Exception, nextAttemptId);
                    }
                    else
                    {
                        try
                        {
                            var result = t.Result;
                            OnReceiveMessageResult(result);
                        }
                        catch (Exception ex)
                        {
                            LogMessages.ExceptionRaisedWhileHandlingReceiveResult(Logger, ex, _mq, endpoints, clientId);
                            OnReceiveMessageException(ex, attemptId);
                        }
                    }
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
            catch (Exception ex)
            {
                LogMessages.ExceptionRaisedDuringMessageReceptionGeneral(Logger, ex, _mq, clientId);
                OnReceiveMessageException(ex, attemptId);
            }
        }

        private void OnReceiveMessageResult(ReceiveMessageResult result)
        {
            var messages = result.Messages;
            if (messages.Count > 0)
            {
                CacheMessages(messages);
                _consumer.GetConsumeService().Consume(this, messages);
            }
            ReceiveMessage();
        }

        private bool IsCacheFull()
        {
            var cacheMessageCountThresholdPerQueue = _consumer.CacheMessageCountThresholdPerQueue();
            var actualMessagesQuantity = CachedMessagesCount();
            var clientId = _consumer.GetClientId();
            if (cacheMessageCountThresholdPerQueue <= actualMessagesQuantity)
            {
                LogMessages.CacheMessagesQuantityExceedsThreshold(Logger, cacheMessageCountThresholdPerQueue, actualMessagesQuantity, _mq, clientId);
                Interlocked.Exchange(ref _cacheFullTime, DateTime.UtcNow.Ticks);
                return true;
            }
            var cacheMessageBytesThresholdPerQueue = _consumer.CacheMessageBytesThresholdPerQueue();
            var actualCachedMessagesBytes = CachedMessageBytes();
            if (cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes)
            {
                LogMessages.CacheMessagesMemoryExceedsThreshold(Logger, cacheMessageBytesThresholdPerQueue, actualCachedMessagesBytes, _mq, clientId);
                Interlocked.Exchange(ref _cacheFullTime, DateTime.UtcNow.Ticks);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Erase messages(Non-FIFO-consume-mode) which have been consumed properly.
        /// </summary>
        /// <param name="messageView">the message to erase.</param>
        /// <param name="consumeResult">consume result.</param>
        public void EraseMessage(MessageView messageView, ConsumeResult consumeResult)
        {
            var task = ConsumeResult.SUCCESS.Equals(consumeResult) ? AckMessage(messageView) : NackMessage(messageView);
            _ = task.ContinueWith(_ =>
            {
                EvictCache(messageView);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private Task AckMessage(MessageView messageView)
        {
            var tcs = new TaskCompletionSource<bool>();
            AckMessage(messageView, 1, tcs);
            return tcs.Task;
        }

        private void AckMessage(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            var clientId = _consumer.GetClientId();
            var consumerGroup = _consumer.GetConsumerGroup();
            var messageId = messageView.MessageId;
            var endpoints = messageView.MessageQueue.Broker.Endpoints;

            var request = _consumer.WrapAckMessageRequest(messageView);
            var task = _consumer.GetClientManager().AckMessage(messageView.MessageQueue.Broker.Endpoints, request,
                _consumer.GetClientConfig().RequestTimeout);

            task.ContinueWith(responseTask =>
            {
                if (responseTask.IsFaulted)
                {
                    LogMessages.FailedToScheduleMessageAckRequest(Logger, responseTask.Exception, _mq, messageView.MessageId, clientId);
                    AckMessageLater(messageView, attempt + 1, tcs);
                }
                else
                {
                    var invocation = responseTask.Result;
                    var requestId = invocation.RequestId;
                    var status = invocation.Response.Status;
                    var statusCode = status.Code;
                    if (statusCode == Code.InvalidReceiptHandle)
                    {
                        LogMessages.FailedToAckMessageInvalidReceiptHandle(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId, status.Message);
                        tcs.SetException(new BadRequestException((int)statusCode, requestId, status.Message));
                    }
                    if (statusCode != Code.Ok)
                    {
                        LogMessages.FailedToChangeInvisibleDuration(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId, status.Message);
                        AckMessageLater(messageView, attempt + 1, tcs);
                        return;
                    }
                    tcs.SetResult(true);
                    if (attempt > 1)
                    {
                        LogMessages.SuccessfullyAckedMessageFinally(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId);
                    }
                    else
                    {
                        LogMessages.SuccessfullyAckedMessage(Logger, clientId, consumerGroup, messageId, _mq, endpoints, requestId);
                    }
                }
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private void AckMessageLater(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(AckMessageFailureBackoffDelay, _ackMsgCts.Token);
                    AckMessage(messageView, attempt + 1, tcs);
                }
                catch (Exception ex)
                {
                    if (_ackMsgCts.IsCancellationRequested)
                    {
                        return;
                    }
                    LogMessages.FailedToScheduleMessageAckRequest(Logger, ex, _mq, messageView.MessageId, _consumer.GetClientId());
                    AckMessageLater(messageView, attempt + 1, tcs);
                }
            });
        }

        private Task NackMessage(MessageView messageView)
        {
            var deliveryAttempt = messageView.DeliveryAttempt;
            var duration = _consumer.GetRetryPolicy().GetNextAttemptDelay(deliveryAttempt);
            var tcs = new TaskCompletionSource<bool>();
            ChangeInvisibleDuration(messageView, duration, 1, tcs);
            return tcs.Task;
        }

        private void ChangeInvisibleDuration(MessageView messageView, TimeSpan duration, int attempt, TaskCompletionSource<bool> tcs)
        {
            var clientId = _consumer.GetClientId();
            var consumerGroup = _consumer.GetConsumerGroup();
            var messageId = messageView.MessageId;
            var endpoints = messageView.MessageQueue.Broker.Endpoints;

            var request = _consumer.WrapChangeInvisibleDuration(messageView, duration);
            var task = _consumer.GetClientManager().ChangeInvisibleDuration(endpoints,
                request, _consumer.GetClientConfig().RequestTimeout);
            task.ContinueWith(responseTask =>
            {
                if (responseTask.IsFaulted)
                {
                    LogMessages.FailedToScheduleMessageAckRequest(Logger, responseTask.Exception, _mq, messageView.MessageId, clientId);
                    ChangeInvisibleDurationLater(messageView, duration, attempt + 1, tcs);
                }
                else
                {
                    var invocation = responseTask.Result;
                    var requestId = invocation.RequestId;
                    var status = invocation.Response.Status;
                    var statusCode = status.Code;
                    if (statusCode == Code.InvalidReceiptHandle)
                    {
                        LogMessages.FailedToAckMessageInvalidReceiptHandle(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId, status.Message);
                        tcs.SetException(new BadRequestException((int)statusCode, requestId, status.Message));
                    }
                    if (statusCode != Code.Ok)
                    {
                        LogMessages.FailedToChangeInvisibleDuration(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId, status.Message);
                        ChangeInvisibleDurationLater(messageView, duration, attempt + 1, tcs);
                        return;
                    }
                    tcs.SetResult(true);
                    if (attempt > 1)
                    {
                        LogMessages.ChangedInvisibleDurationSuccessfullyFinally(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId);
                    }
                    else
                    {
                        LogMessages.ChangedInvisibleDurationSuccessfully(Logger, clientId, consumerGroup, messageId, _mq, endpoints, requestId);
                    }
                }
            });
        }

        private void ChangeInvisibleDurationLater(MessageView messageView, TimeSpan duration, int attempt,
            TaskCompletionSource<bool> tcs)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(ChangeInvisibleDurationFailureBackoffDelay, _changeInvisibleDurationCts.Token);
                    ChangeInvisibleDuration(messageView, duration, attempt, tcs);
                }
                catch (Exception ex)
                {
                    if (_changeInvisibleDurationCts.IsCancellationRequested)
                    {
                        return;
                    }
                    LogMessages.FailedToScheduleMessageAckRequest(Logger, ex, _mq, messageView.MessageId, _consumer.GetClientId());
                    ChangeInvisibleDurationLater(messageView, duration, attempt + 1, tcs);
                }
            });
        }

        public Task EraseFifoMessage(MessageView messageView, ConsumeResult consumeResult)
        {
            var retryPolicy = _consumer.GetRetryPolicy();
            var maxAttempts = retryPolicy.GetMaxAttempts();
            var attempt = messageView.DeliveryAttempt;
            var messageId = messageView.MessageId;
            var service = _consumer.GetConsumeService();
            var clientId = _consumer.GetClientId();

            if (consumeResult == ConsumeResult.FAILURE && attempt < maxAttempts)
            {
                var nextAttemptDelay = retryPolicy.GetNextAttemptDelay(attempt);
                attempt = messageView.IncrementAndGetDeliveryAttempt();
                LogMessages.PrepareToRedeliverFifoMessage(Logger, maxAttempts, attempt, messageView.MessageQueue, messageId, nextAttemptDelay, clientId);
                var redeliverTask = service.Consume(messageView, nextAttemptDelay);
                _ = redeliverTask.ContinueWith(async t =>
                {
                    var result = await t;
                    await EraseFifoMessage(messageView, result);
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
            else
            {
                var success = consumeResult == ConsumeResult.SUCCESS;
                if (!success)
                {
                    LogMessages.FailedToConsumeFifoMessageFinally(Logger, maxAttempts, attempt, messageView.MessageQueue, messageId, clientId);
                }

                var task = ConsumeResult.SUCCESS.Equals(consumeResult)
                    ? AckMessage(messageView)
                    : ForwardToDeadLetterQueue(messageView);

                _ = task.ContinueWith(_ => { EvictCache(messageView); },
                    TaskContinuationOptions.ExecuteSynchronously);
            }

            return Task.CompletedTask;
        }

        private Task ForwardToDeadLetterQueue(MessageView messageView)
        {
            var tcs = new TaskCompletionSource<bool>();
            ForwardToDeadLetterQueue(messageView, 1, tcs);
            return tcs.Task;
        }

        private void ForwardToDeadLetterQueue(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            var clientId = _consumer.GetClientId();
            var consumerGroup = _consumer.GetConsumerGroup();
            var messageId = messageView.MessageId;
            var endpoints = messageView.MessageQueue.Broker.Endpoints;

            var request = _consumer.WrapForwardMessageToDeadLetterQueueRequest(messageView);
            var task = _consumer.GetClientManager().ForwardMessageToDeadLetterQueue(endpoints, request,
                _consumer.GetClientConfig().RequestTimeout);

            task.ContinueWith(responseTask =>
            {
                if (responseTask.IsFaulted)
                {
                    LogMessages.ExceptionRaisedDuringWhileForwardMessageToDLQ(Logger, responseTask.Exception, clientId, consumerGroup, messageId, _mq);
                    ForwardToDeadLetterQueueLater(messageView, attempt, tcs);
                }
                else
                {
                    var invocation = responseTask.Result;
                    var requestId = invocation.RequestId;
                    var status = invocation.Response.Status;
                    var statusCode = status.Code;

                    // Log failure and retry later.
                    if (statusCode != Code.Ok)
                    {
                        LogMessages.FailedToForwardMessageToDeadLetterQueue(Logger, clientId, consumerGroup, messageId, attempt, _mq, endpoints, requestId, statusCode, status.Message);
                        ForwardToDeadLetterQueueLater(messageView, attempt, tcs);
                        return;
                    }

                    tcs.SetResult(true);

                    // Log success.
                    if (attempt > 1)
                    {
                        LogMessages.ReForwardMessageToDeadLetterQueueSuccessfully(Logger, clientId, consumerGroup, attempt, messageId, _mq, endpoints, requestId);
                    }
                    else
                    {
                        LogMessages.ForwardMessageToDeadLetterQueueSuccessfully(Logger, clientId, consumerGroup, messageId, _mq, endpoints, requestId);
                    }
                }
            });
        }

        private void ForwardToDeadLetterQueueLater(MessageView messageView, int attempt, TaskCompletionSource<bool> tcs)
        {
            Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(ForwardMessageToDeadLetterQueueFailureBackoffDelay,
                        _forwardMessageToDeadLetterQueueCts.Token);
                    ForwardToDeadLetterQueue(messageView, attempt, tcs);
                }
                catch (Exception ex)
                {
                    // Should never reach here.
                    LogMessages.FailedToScheduleDLQMessageRequest(Logger, ex, _mq, messageView.MessageId, _consumer.GetClientId());
                    ForwardToDeadLetterQueueLater(messageView, attempt + 1, tcs);
                }
            });
        }

        /// <summary>
        /// Discard the message(Non-FIFO-consume-mode) which could not be consumed properly.
        /// </summary>
        /// <param name="messageView">the message to discard.</param>
        public void DiscardMessage(MessageView messageView)
        {
            LogMessages.DiscardMessage(Logger, _mq, messageView.MessageId, _consumer.GetClientId());
            var task = NackMessage(messageView);
            _ = task.ContinueWith(_ =>
            {
                EvictCache(messageView);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Discard the message(FIFO-consume-mode) which could not consumed properly.
        /// </summary>
        /// <param name="messageView">the FIFO message to discard.</param>
        public void DiscardFifoMessage(MessageView messageView)
        {
            LogMessages.DiscardFifoMessage(Logger, _mq, messageView.MessageId, _consumer.GetClientId());
            var task = ForwardToDeadLetterQueue(messageView);
            _ = task.ContinueWith(_ =>
            {
                EvictCache(messageView);
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        private void EvictCache(MessageView messageView)
        {
            _cachedMessageLock.EnterWriteLock();
            try
            {
                if (_cachedMessages.Remove(messageView))
                {
                    Interlocked.Add(ref _cachedMessagesBytes, -messageView.Body.Length);
                }
            }
            finally
            {
                _cachedMessageLock.ExitWriteLock();
            }
        }

        public int CachedMessagesCount()
        {
            _cachedMessageLock.EnterReadLock();
            try
            {
                return _cachedMessages.Count;
            }
            finally
            {
                _cachedMessageLock.ExitReadLock();
            }
        }

        public long CachedMessageBytes()
        {
            return Interlocked.Read(ref _cachedMessagesBytes);
        }

        /// <summary>
        /// Get the count of cached messages.
        /// </summary>
        /// <returns>count of pending messages.</returns>
        public long GetCachedMessageCount()
        {
            _cachedMessageLock.EnterReadLock();
            try
            {
                return _cachedMessages.Count;
            }
            finally
            {
                _cachedMessageLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Get the bytes of cached message memory footprint.
        /// </summary>
        /// <returns>bytes of cached message memory footprint.</returns>
        public long GetCachedMessageBytes()
        {
            return _cachedMessagesBytes;
        }
        
        public static partial class LogMessages
        {
            [LoggerMessage(EventId = 40, Level = LogLevel.Warning, Message = "Process queue is idle, idleDuration={IdleDuration}, maxIdleDuration={MaxIdleDuration}, afterCacheFullDuration={AfterCacheFullDuration}, mq={Mq}, clientId={ClientId}")]
            public static partial void ProcessQueueIdle(ILogger logger, long idleDuration, TimeSpan maxIdleDuration, long afterCacheFullDuration, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 41, Level = LogLevel.Information, Message = "Try to receive message later, mq={Mq}, delay={Delay}, clientId={ClientId}")]
            public static partial void TryToReceiveMessageLater(ILogger logger, MessageQueue mq, TimeSpan delay, string clientId);
        
            [LoggerMessage(EventId = 42, Level = LogLevel.Error, Message = "[Bug] Failed to schedule message receiving request, mq={Mq}, clientId={ClientId}")]
            public static partial void FailedToScheduleMessageReceivingRequest(ILogger logger, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 43, Level = LogLevel.Error, Message = "Exception raised during message reception, mq={Mq}, attemptId={AttemptId}, nextAttemptId={NextAttemptId}, clientId={ClientId}")]
            public static partial void ExceptionRaisedDuringMessageReception(ILogger logger, Exception exception, MessageQueue mq, string attemptId, string nextAttemptId, string clientId);
        
            [LoggerMessage(EventId = 44, Level = LogLevel.Error, Message = "[Bug] Exception raised while handling receive result, mq={Mq}, endpoints={Endpoints}, clientId={ClientId}")]
            public static partial void ExceptionRaisedWhileHandlingReceiveResult(ILogger logger, Exception exception, MessageQueue mq, Endpoints endpoints, string clientId);
        
            [LoggerMessage(EventId = 45, Level = LogLevel.Warning, Message = "Process queue total cached messages quantity exceeds the threshold, threshold={Threshold}, actual={Actual}, mq={Mq}, clientId={ClientId}")]
            public static partial void CacheMessagesQuantityExceedsThreshold(ILogger logger, int threshold, int actual, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 46, Level = LogLevel.Warning, Message = "Process queue total cached messages memory exceeds the threshold, threshold={Threshold} bytes, actual={Actual} bytes, mq={Mq}, clientId={ClientId}")]
            public static partial void CacheMessagesMemoryExceedsThreshold(ILogger logger, long threshold, long actual, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 47, Level = LogLevel.Information, Message = "Process queue has been dropped, no longer receive message, mq={Mq}, clientId={ClientId}")]
            public static partial void ProcessQueueDropped(ILogger logger, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 48, Level = LogLevel.Information, Message = "Stop to receive message because consumer is not running, mq={Mq}, clientId={ClientId}")]
            public static partial void ConsumerNotRunning(ILogger logger, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 49, Level = LogLevel.Error, Message = "Exception raised during message reception, mq={Mq}, clientId={ClientId}")]
            public static partial void ExceptionRaisedDuringMessageReceptionGeneral(ILogger logger, Exception exception, MessageQueue mq, string clientId);
        
            [LoggerMessage(EventId = 50, Level = LogLevel.Debug, Message = "Successfully acked message, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}")]
            public static partial void SuccessfullyAckedMessage(ILogger logger, string clientId, string consumerGroup, string messageId, MessageQueue mq, Endpoints endpoints, string requestId);
        
            [LoggerMessage(EventId = 51, Level = LogLevel.Information, Message = "Successfully acked message finally, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, attempt={Attempt}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}")]
            public static partial void SuccessfullyAckedMessageFinally(ILogger logger, string clientId, string consumerGroup, string messageId, int attempt, MessageQueue mq, Endpoints endpoints, string requestId);
        
            [LoggerMessage(EventId = 52, Level = LogLevel.Error, Message = "Failed to ack message due to the invalid receipt handle, forgive to retry, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, attempt={Attempt}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}, status message={StatusMessage}")]
            public static partial void FailedToAckMessageInvalidReceiptHandle(ILogger logger, string clientId, string consumerGroup, string messageId, int attempt, MessageQueue mq, Endpoints endpoints, string requestId, string statusMessage);
        
            [LoggerMessage(EventId = 53, Level = LogLevel.Error, Message = "Failed to change invisible duration, would retry later, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, attempt={Attempt}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}, status message={StatusMessage}")]
            public static partial void FailedToChangeInvisibleDuration(ILogger logger, string clientId, string consumerGroup, string messageId, int attempt, MessageQueue mq, Endpoints endpoints, string requestId, string statusMessage);
        
            [LoggerMessage(EventId = 54, Level = LogLevel.Debug, Message = "Changed invisible duration successfully, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}")]
            public static partial void ChangedInvisibleDurationSuccessfully(ILogger logger, string clientId, string consumerGroup, string messageId, MessageQueue mq, Endpoints endpoints, string requestId);
        
            [LoggerMessage(EventId = 55, Level = LogLevel.Information, Message = "Finally, changed invisible duration successfully, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, attempt={Attempt}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}")]
            public static partial void ChangedInvisibleDurationSuccessfullyFinally(ILogger logger, string clientId, string consumerGroup, string messageId, int attempt, MessageQueue mq, Endpoints endpoints, string requestId);
        
            [LoggerMessage(EventId = 56, Level = LogLevel.Error, Message = "[Bug] Failed to schedule message ack request, mq={Mq}, messageId={MessageId}, clientId={ClientId}")]
            public static partial void FailedToScheduleMessageAckRequest(ILogger logger, Exception exception, MessageQueue mq, string messageId, string clientId);
        
            [LoggerMessage(EventId = 57, Level = LogLevel.Debug, Message = "Prepare to redeliver the fifo message because of the consumption failure, maxAttempt={MaxAttempts}, attempt={Attempt}, mq={Mq}, messageId={MessageId}, nextAttemptDelay={NextAttemptDelay}, clientId={ClientId}")]
            public static partial void PrepareToRedeliverFifoMessage(ILogger logger, int maxAttempts, int attempt, MessageQueue mq, string messageId, TimeSpan nextAttemptDelay, string clientId);
        
            [LoggerMessage(EventId = 58, Level = LogLevel.Information, Message = "Failed to consume fifo message finally, run out of attempt times, maxAttempts={MaxAttempts}, attempt={Attempt}, mq={Mq}, messageId={MessageId}, clientId={ClientId}")]
            public static partial void FailedToConsumeFifoMessageFinally(ILogger logger, int maxAttempts, int attempt, MessageQueue mq, string messageId, string clientId);
        
            [LoggerMessage(EventId = 59, Level = LogLevel.Information, Message = "Forward message to dead letter queue successfully, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}")]
            public static partial void ForwardMessageToDeadLetterQueueSuccessfully(ILogger logger, string clientId, string consumerGroup, string messageId, MessageQueue mq, Endpoints endpoints, string requestId);
        
            [LoggerMessage(EventId = 60, Level = LogLevel.Information, Message = "Re-forward message to dead letter queue successfully, clientId={ClientId}, consumerGroup={ConsumerGroup}, attempt={Attempt}, messageId={MessageId}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}")]
            public static partial void ReForwardMessageToDeadLetterQueueSuccessfully(ILogger logger, string clientId, string consumerGroup, int attempt, string messageId, MessageQueue mq, Endpoints endpoints, string requestId);
        
            [LoggerMessage(EventId = 61, Level = LogLevel.Error, Message = "Failed to forward message to dead letter queue, would attempt to re-forward later, clientId={ClientId}, consumerGroup={ConsumerGroup}, messageId={MessageId}, attempt={Attempt}, mq={Mq}, endpoints={Endpoints}, requestId={RequestId}, code={Code}, status message={StatusMessage}")]
            public static partial void FailedToForwardMessageToDeadLetterQueue(ILogger logger, string clientId, string consumerGroup, string messageId, int attempt, MessageQueue mq, Endpoints endpoints, string requestId, Code code, string statusMessage);
            
            [LoggerMessage(EventId = 62, Level = LogLevel.Error, Message = "[Bug] Failed to schedule DLQ message request, mq={Mq}, messageId={MessageId}, clientId={ClientId}")]
            public static partial void FailedToScheduleDLQMessageRequest(ILogger logger, Exception exception, MessageQueue mq, string messageId, string clientId);
        
            [LoggerMessage(EventId = 63, Level = LogLevel.Information, Message = "Discard message, mq={Mq}, messageId={MessageId}, clientId={ClientId}")]
            public static partial void DiscardMessage(ILogger logger, MessageQueue mq, string messageId, string clientId);
        
            [LoggerMessage(EventId = 64, Level = LogLevel.Information, Message = "Discard fifo message, mq={Mq}, messageId={MessageId}, clientId={ClientId}")]
            public static partial void DiscardFifoMessage(ILogger logger, MessageQueue mq, string messageId, string clientId);
            
            [LoggerMessage(EventId = 65, Level = LogLevel.Error, Message = "Exception raised while forward message to DLQ, would attempt to re-forward later, clientId={ClientId}, consumerGroup={consumerGroup}, messageId={MessageId}, mq={Mq}")]
            public static partial void ExceptionRaisedDuringWhileForwardMessageToDLQ(ILogger logger, Exception exception, string clientId, string consumerGroup, string messageId, MessageQueue mq);
        }
    }
}