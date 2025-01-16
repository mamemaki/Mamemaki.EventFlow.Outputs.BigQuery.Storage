# Mamemaki.EventFlow.Outputs.BigQuery.Storage PlayGround

## Generate protobuf c# code

````
protoc -I=./protobuf --csharp_out=./protobuf_gen --csharp_opt=file_extension=.pb.g.cs ./protobuf/event_record.proto
````
