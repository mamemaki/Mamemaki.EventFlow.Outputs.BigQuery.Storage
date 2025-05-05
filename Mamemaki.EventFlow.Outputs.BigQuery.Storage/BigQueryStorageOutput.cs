using Google.Apis.Util;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Protobuf;
using Mamemaki.EventFlow.Outputs.BigQuery.Storage.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Diagnostics.EventFlow.Utilities;
using Microsoft.Diagnostics.EventFlow;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Validation;

namespace Mamemaki.EventFlow.Outputs.BigQuery.Storage
{
    public class BigQueryStorageOutput : IOutput, IDisposable
    {
        private readonly IHealthReporter healthReporter;
        private BigQueryStorageOutputConfiguration Config { get; set; }
        private IProtobufMessageMapper _ProtobufMessageMapper;
        private BigqueryService _BQSvc;
        private IBackOff _BackOff;
        private BigQueryWriteClient _BigQueryWriteClient;
        private BigQueryWriteClient.AppendRowsStream _AppendRowsStream;
        private Task _ProcessResponsesTask;
        private bool _NeedReconnectStream;
        private readonly SemaphoreSlim _ReconnectStreamLock = new SemaphoreSlim(1, 1);

        public Google.Apis.Bigquery.v2.Data.TableSchema TableSchema { get; private set; }

        public string TableIdExpanded
        {
            get
            {
                return _TableIdExpanded;
            }
            set
            {
                if (String.Compare(_TableIdExpanded, value) != 0)
                {
                    _TableIdExpanded = value;
                    _NeedCheckTableExists = true;
                    _NeedWriteSchema = true;
                }
            }
        }
        private TimeZoneInfo _TimeZoneInfo;
        private string _TableIdExpanded;
        private bool _TableIdExpandable;
        private bool _NeedCheckTableExists;
        private bool _NeedWriteSchema;
        private readonly SemaphoreSlim _EnsureTableExistsLock = new SemaphoreSlim(1, 1);

        public BigQueryStorageOutput(IConfiguration configuration, IHealthReporter healthReporter)
        {
            Requires.NotNull(configuration, nameof(configuration));
            Requires.NotNull(healthReporter, nameof(healthReporter));

            this.healthReporter = healthReporter;
            var bqOutputConfiguration = new BigQueryStorageOutputConfiguration();
            try
            {
                configuration.Bind(bqOutputConfiguration);
            }
            catch
            {
                healthReporter.ReportProblem($"Invalid {nameof(BigQueryStorageOutput)} configuration encountered: '{configuration.ToString()}'",
                    EventFlowContextIdentifiers.Configuration);
                throw;
            }

            Initialize(bqOutputConfiguration);
        }

        public BigQueryStorageOutput(BigQueryStorageOutputConfiguration bqOutputConfiguration, IHealthReporter healthReporter)
        {
            Requires.NotNull(bqOutputConfiguration, nameof(bqOutputConfiguration));
            Requires.NotNull(healthReporter, nameof(healthReporter));

            this.healthReporter = healthReporter;
            Initialize(bqOutputConfiguration);
        }

        private void Initialize(BigQueryStorageOutputConfiguration bqOutputConfiguration)
        {
            Debug.Assert(bqOutputConfiguration != null);
            Debug.Assert(this.healthReporter != null);

            if (String.IsNullOrEmpty(bqOutputConfiguration.ProjectId))
                throw new ArgumentException("ProjectId");
            if (String.IsNullOrEmpty(bqOutputConfiguration.DatasetId))
                throw new ArgumentException("DatasetId");
            if (String.IsNullOrEmpty(bqOutputConfiguration.TableId))
                throw new ArgumentException("TableId");

            this.Config = bqOutputConfiguration;

            // Instantiate protobuf message mapper
            _ProtobufMessageMapper = CreateProtobufMessageMapper();

            // Load table schema file
            if (Config.TableSchemaFile != null)
                TableSchema = LoadTableSchema(Config.TableSchemaFile);
            if (TableSchema == null)
                throw new Exception("table schema not set");
            _TimeZoneInfo = TimeZoneInfo.Utc;
            if (!string.IsNullOrEmpty(Config.TimeZoneId))
                _TimeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(Config.TimeZoneId);
            // Expand table id first time within force mode
            ExpandTableIdIfNecessary(force: true);
            // configure finished
            healthReporter.ReportHealthy("TableId: " + TableIdExpanded);

            var scopes = new[]
            {
                BigqueryService.Scope.Bigquery,
                BigqueryService.Scope.BigqueryInsertdata,
                BigqueryService.Scope.CloudPlatform,
                BigqueryService.Scope.DevstorageFullControl
            };
            var credential = GoogleCredential.GetApplicationDefault()
                .CreateScoped(scopes);

            _BQSvc = new BigqueryService(new BaseClientService.Initializer
            {
                HttpClientInitializer = credential,
            });
            _BackOff = new ExponentialBackOff();

            {
                var builder = new BigQueryWriteClientBuilder();
                // Change the keep-alive value from 60 to 50 seconds.
                // This is because the connection sometimes drops out.
                // However, we do not know if this is related.
                builder.GrpcChannelOptions.WithKeepAliveTime(TimeSpan.FromSeconds(50));
                _BigQueryWriteClient = builder.Build();
            }
            OpenStream();
        }

        IProtobufMessageMapper CreateProtobufMessageMapper()
        {
            try
            {
                var type = Type.GetType(Config.MapperQualifiedTypeName, throwOnError: true);
                return (IProtobufMessageMapper)Activator.CreateInstance(type);
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to instantiate protobuf message mapper({Config.MapperQualifiedTypeName})", ex);
            }
        }

        void HandleError(Exception ex, string message = null)
        {
            // We ignore these errors
            if (ex.Message.Contains("Cannot route on empty project id") ||     // It occurs when exiting
                ex.Message.Contains("Permission 'TABLES_UPDATE_DATA' denied")) // It occurs when table does not exists
            {
                this.healthReporter.ReportHealthy("Error ignored." + Environment.NewLine + ex.ToString());
                return;
            }

            if (message == null)
                message = "Failed to write events to Bq";

            ErrorHandlingPolicies.HandleOutputTaskError(ex, () =>
            {
                string errorMessage = $"{nameof(BigQueryStorageOutput)}: {message}." + Environment.NewLine + ex.ToString();
                this.healthReporter.ReportProblem(errorMessage, EventFlowContextIdentifiers.Output);
            });
        }

        /// <summary>
        /// Reconnect the stream if stream closed
        /// </summary>
        /// <param name="error"></param>
        /// <param name="retryCnt"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>true if the stream reconnected</returns>
        async Task<bool> ReconnectIfDiconnectedAsync(Exception error, int retryCnt, CancellationToken cancellationToken)
        {
            var needReconnect = error.Message.Contains("Closing the stream because it has been inactive for ") ||
                error.Message.Contains("Closing the stream because server is restarted") ||
                error.Message.Contains("The response ended prematurely while waiting for the next frame from the server.") ||
                error.Message.Contains("The HTTP/2 server reset the stream");
            if (!needReconnect)
                return false;
            _NeedReconnectStream = true;

            // Only one thread at a time can execute the table creating process
            await _ReconnectStreamLock.WaitAsync(cancellationToken);
            try
            {
                if (retryCnt > 10)
                    throw new Exception("Retry over");

                if (!_NeedReconnectStream)
                    return true;

                // Reconnect the stream
                this.healthReporter.ReportHealthy("Reconnect the stream cause stream closed." + Environment.NewLine + error.ToString());
                CloseStream();
                OpenStream();
                _NeedWriteSchema = true;
                _NeedReconnectStream = false;
                this.healthReporter.ReportHealthy("Stream reconnected.");
                return true;
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
            finally
            {
                _ReconnectStreamLock.Release();
            }
            return false;
        }

        async Task ProcessResponsesAsync()
        {
            var responses = _AppendRowsStream.GetResponseStream();
            try
            {
                while (await responses.MoveNextAsync())
                {
                    var response = responses.Current;
                    if (response.Error != null)
                    {
                        throw new Exception(response.Error.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
            finally
            {
                if (responses != null)
                {
                    await responses.DisposeAsync();
                }
            }
        }

        private void OpenStream()
        {
            if (_AppendRowsStream != null)
                throw new Exception("Stream opend already");

            _AppendRowsStream = _BigQueryWriteClient.AppendRows();
            _ProcessResponsesTask = Task.Run(ProcessResponsesAsync);
        }

        private void CloseStream()
        {
            if (_AppendRowsStream != null)
            {
                try
                {
                    _AppendRowsStream.WriteCompleteAsync().Wait();
                }
                catch (Exception ex)
                {
                    HandleError(ex);
                }
                if (_ProcessResponsesTask != null)
                {
                    // Wait for all the responses to be processed before disposing
                    _ProcessResponsesTask.Wait();
                    _ProcessResponsesTask.Dispose();
                    _ProcessResponsesTask = null;
                }
                _AppendRowsStream.Dispose();
                _AppendRowsStream = null;
            }
        }

        public void Dispose()
        {
            CloseStream();

            if (_BQSvc != null)
            {
                _BQSvc.Dispose();
                _BQSvc = null;
            }
        }

        public async Task SendEventsAsync(IReadOnlyCollection<EventData> events, long transmissionSequenceNumber, 
            CancellationToken cancellationToken)
        {
            if (events == null || events.Count == 0)
            {
                return;
            }

        RETRY:
            int retryCnt = 0;
            try
            {
                ExpandTableIdIfNecessary();
                await EnsureTableExistsAsync(cancellationToken);

                var requests = events.Select(s => CreateRowRequest(s)).Where(s => s != null).ToList();
                foreach (var request in requests)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    await _AppendRowsStream.WriteAsync(request);
                }

                this.healthReporter.ReportHealthy();
            }
            catch (Exception ex)
            {
                if (await ReconnectIfDiconnectedAsync(ex, retryCnt++, cancellationToken))
                {
                    goto RETRY;
                }
                HandleError(ex);
            }
        }

        async Task EnsureTableExistsAsync(CancellationToken cancellationToken)
        {
            if (!Config.AutoCreateTable)
                return;
            if (!_NeedCheckTableExists)
                return;

            // Only one thread at a time can execute the table creating process
            await _EnsureTableExistsLock.WaitAsync(cancellationToken);
            try
            {
                if (!_NeedCheckTableExists)
                    return;
                if (!await IsTableExistsAsync(TableIdExpanded, cancellationToken))
                {
                    await CreateTableAsync(TableIdExpanded, cancellationToken);
                    await WaitForCreateTableCompletedAsync(TableIdExpanded, cancellationToken);
                }
                _NeedCheckTableExists = false;
            }
            finally
            {
                _EnsureTableExistsLock.Release();
            }
        }

        async Task<bool> IsTableExistsAsync(string tableId, CancellationToken cancellationToken)
        {
            try
            {
                var table = await _BQSvc.Tables.Get(Config.ProjectId, Config.DatasetId, tableId)
                    .ExecuteAsync(cancellationToken);
                return true;
            }
            catch (GoogleApiException ex)
            {
                if (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound &&
                    ex.Message.Contains("Not found: Table"))
                {
                    return false;
                }
                throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        async Task WaitForCreateTableCompletedAsync(string tableId, CancellationToken cancellationToken)
        {
            var retry = 1;
            while (retry < _BackOff.MaxNumOfRetries)
            {
                if (await IsTableExistsAsync(tableId, cancellationToken))
                    return;

                retry++;
                await Task.Delay(_BackOff.GetNextBackOff(retry));
            }
        }

        async Task CreateTableAsync(string tableId, CancellationToken cancellationToken)
        {
            try
            {
                var table = new Table
                {
                    TableReference = new TableReference
                    {
                        ProjectId = Config.ProjectId,
                        DatasetId = Config.DatasetId,
                        TableId = tableId
                    },
                    Schema = TableSchema,
                };

                await _BQSvc.Tables.Insert(table, Config.ProjectId, Config.DatasetId)
                    .ExecuteAsync(cancellationToken);
                this.healthReporter.ReportHealthy(nameof(BigQueryStorageOutput) + $": Table({tableId}) created.",
                    EventFlowContextIdentifiers.Output);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (GoogleApiException ex)
            {
                if (ex.HttpStatusCode == System.Net.HttpStatusCode.Conflict &&
                    ex.Message.Contains("Already Exists: Table"))
                {
                    return;
                }

                this.healthReporter.ReportProblem(nameof(BigQueryStorageOutput) + ": Failed to create table." + Environment.NewLine + ex.ToString(), 
                    EventFlowContextIdentifiers.Output);
            }
            catch (Exception ex)
            {
                this.healthReporter.ReportProblem(nameof(BigQueryStorageOutput) + ": Failed to create table." + Environment.NewLine + ex.ToString(),
                    EventFlowContextIdentifiers.Output);
                throw;
            }
        }

        Google.Apis.Bigquery.v2.Data.TableSchema LoadTableSchema(string schemaFile)
        {
            try
            {
                var strJson = File.ReadAllText(schemaFile);
                var schema = new Google.Apis.Bigquery.v2.Data.TableSchema();
                schema.Fields = JsonConvert.DeserializeObject<List<Google.Apis.Bigquery.v2.Data.TableFieldSchema>>(strJson);
                return schema;
            }
            catch (Exception ex)
            {
                this.healthReporter.ReportProblem(nameof(BigQueryStorageOutput) + ": Failed to load schema." + Environment.NewLine + ex.ToString(),
                    EventFlowContextIdentifiers.Output);
                throw;
            }
        }

        void ExpandTableIdIfNecessary(bool force = false)
        {
            if (force || _TableIdExpandable)
            {
                this.TableIdExpanded = Regex.Replace(Config.TableId, @"(\{.+})", delegate (Match m) {
                    var pattern = m.Value.Substring(1, m.Value.Length - 2);
                    _TableIdExpandable = true;
                    return TimeZoneInfo.ConvertTime(DateTime.UtcNow, _TimeZoneInfo).ToString(pattern);
                });
            }
        }

        AppendRowsRequest CreateRowRequest(EventData eventEntry)
        {
            var eventRecord = _ProtobufMessageMapper.Map(eventEntry);
            if (eventRecord == null)
                return null;

            var request = new AppendRowsRequest
            {
                ProtoRows = new AppendRowsRequest.Types.ProtoData
                {
                    Rows = new ProtoRows { SerializedRows = { eventRecord.ToByteString() } }
                },
                WriteStreamAsWriteStreamName = new WriteStreamName(Config.ProjectId, Config.DatasetId, TableIdExpanded, "_default"),
            };
            if (_NeedWriteSchema)
            {
                request.ProtoRows.WriterSchema = new ProtoSchema { ProtoDescriptor = _ProtobufMessageMapper.GetDescriptorProto() };
                _NeedWriteSchema = false;
            }
            return request;
        }
    }
}
