using Google.Protobuf;
using Google.Protobuf.Reflection;
using Microsoft.Diagnostics.EventFlow;
using System;
using System.Collections.Generic;
using System.Text;

namespace Mamemaki.EventFlow.Outputs.BigQuery.Storage
{
    public interface IProtobufMessageMapper
    {
        /// <summary>
        /// Map EventData to IMessage
        /// </summary>
        /// <param name="eventEntry"></param>
        /// <returns>IMessage object. If null, the event will be discarded.</returns>
        IMessage Map(EventData eventEntry);

        /// <summary>
        /// Get descriptor for the message
        /// </summary>
        /// <returns></returns>
        DescriptorProto GetDescriptorProto();
    }
}
