using Microsoft.Diagnostics.EventFlow.Configuration;

namespace Mamemaki.EventFlow.Outputs.BigQuery.Storage.Configuration
{
    public class BigQueryStorageOutputConfiguration : ItemConfiguration
    {
        /// <summary>
        /// GCP Project id
        /// </summary>
        public string ProjectId { get; set; }

        /// <summary>
        /// BigQuery Dataset id
        /// </summary>
        public string DatasetId { get; set; }

        /// <summary>
        /// Table id. The string enclosed in brackets can be expanded through DateTime.Format().
        /// e.g. "accesslog_{yyyyMMdd}" => accesslog_20150101
        /// <see cref="https://msdn.microsoft.com/en-us/library/vstudio/zdtaw1bw(v=vs.100).aspx"/>
        /// </summary>
        public string TableId { get; set; }

        /// <summary>
        /// Time zone id for table id expanding
        /// If omitted, UTC is used.
        /// </summary>
        public string TimeZoneId { get; set; }

        /// <summary>
        /// Json file path that bigquery table schema
        /// </summary>
        public string TableSchemaFile { get; set; }

        /// <summary>
        /// The protobuf message mapper class qualified type name
        /// The mapper class must implement IProtobufMessageMapper and have a default constructor.
        /// </summary>
        public string MapperQualifiedTypeName { get; set; }

        /// <summary>
        /// Create table if it does not exists
        /// </summary>
        public bool AutoCreateTable { get; set; }
    }
}
