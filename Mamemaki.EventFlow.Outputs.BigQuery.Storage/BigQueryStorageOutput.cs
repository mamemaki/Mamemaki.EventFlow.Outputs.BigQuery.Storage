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
        private BigQueryWriteClient.AppendRowsStream _AppendRowsStream;
        private Task _ProcessResponsesTask;

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

            var client = BigQueryWriteClient.Create();
            _AppendRowsStream = client.AppendRows();
            _ProcessResponsesTask = Task.Run(ProcessResponsesAsync);
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
            catch (Grpc.Core.RpcException ex)
            {
                // We ignore these errors
                if (!ex.Message.Contains("Cannot route on empty project id") &&     // It occurs when exiting
                    !ex.Message.Contains("Permission 'TABLES_UPDATE_DATA' denied")) // It occurs when table does not exists
                {
                    ErrorHandlingPolicies.HandleOutputTaskError(ex, () =>
                    {
                        string errorMessage = nameof(BigQueryStorageOutput) + ": Failed to write events to Bq." + Environment.NewLine + ex.ToString();
                        this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);
                    });
                }
            }
            catch (Exception ex)
            {
                ErrorHandlingPolicies.HandleOutputTaskError(ex, () =>
                {
                    string errorMessage = nameof(BigQueryStorageOutput) + ": Failed to write events to Bq." + Environment.NewLine + ex.ToString();
                    this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);
                });
            }
            finally
            {
                if (responses != null)
                {
                    await responses.DisposeAsync();
                }
            }
        }

        public void Dispose()
        {
            if (_AppendRowsStream != null)
            {
                try
                {
                    _AppendRowsStream.WriteCompleteAsync().Wait();
                }
                catch (Exception ex)
                {
                    ErrorHandlingPolicies.HandleOutputTaskError(ex, () =>
                    {
                        string errorMessage = nameof(BigQueryStorageOutput) + ": Failed to write events to Bq." + Environment.NewLine + ex.ToString();
                        this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);
                    });
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
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                ErrorHandlingPolicies.HandleOutputTaskError(ex, () =>
                {
                    string errorMessage = nameof(BigQueryStorageOutput) + ": Failed to write events to Bq." + Environment.NewLine + ex.ToString();
                    this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);
                });
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
