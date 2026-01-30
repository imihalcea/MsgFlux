using BenchmarkDotNet.Running;
using MsgFlux.Core.Benchmarks;

// var summary = BenchmarkRunner.Run<EngineBenchmarks>();
var summary = BenchmarkRunner.Run<SerializationBenchmarks>();