using Google.Protobuf;
using Google.Protobuf.Reflection;
using Mamemaki.EventFlow.Outputs.BigQuery.Storage;
using Microsoft.Diagnostics.EventFlow;
using PlayGround.Protobuf;
using System;

namespace PlayGround
{
    internal class EventRecordProtobufMessageMapper : IProtobufMessageMapper
    {
        public DescriptorProto GetDescriptorProto()
        {
            return EventRecord.Descriptor.ToProto();
        }

        public IMessage Map(EventData eventEntry)
        {
            var eventRecord = new EventRecord();
            eventRecord.Timestamp = eventEntry.Timestamp.ToLocalTime().ToUnixTimeMicroseconds();
            if (eventEntry.TryGetPropertyValue("Message", out var message))
                eventRecord.Message = message as string;
            else
                return null;    // Discard no message event
            if (eventEntry.TryGetPropertyValue("UserId", out var userId))
                eventRecord.UserId = Convert.ToInt32(userId);
            if (eventEntry.TryGetPropertyValue("MachineName", out var machineName))
                eventRecord.MachineName = machineName as string;
            if (eventEntry.TryGetPropertyValue("TaskName", out var taskName))
                eventRecord.TaskName = taskName as string;
            return eventRecord;
        }
    }
}
