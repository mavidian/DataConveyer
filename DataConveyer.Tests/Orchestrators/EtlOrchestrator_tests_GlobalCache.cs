//EtlOrchestrator_tests_GlobalCache.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using DataConveyer.Tests.TestHelpers;
using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   public class EtlOrchestrator_tests_GlobalCache
   {
      //Variables to assert on:
      IGlobalCache _cacheInstance;
      int _callCount;
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "FNAME,LNAME";
         yield return "Loren,Asar";
         yield return "Lara,Gudroe";
         yield return "Shawna,Palaspas";
      }

      //Result of the tests are held here:
      private readonly List<KeyValRecord> _resultingRecords;  //container of the test results
      private readonly ActionBlock<KeyValRecord> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_GlobalCache()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Delimited
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.TextOutputConsumer = l => { };  //throwaway consumer

         //prepare extraction of the results from the pipeline
         _resultingRecords = new List<KeyValRecord>();
         _resultsExtractor = new ActionBlock<KeyValRecord>(c => _resultingRecords.Add(c));
      }


      /// <summary>
      /// Verifies if passed global cache instance is the same as the one used before (i.e. held in _cacheInstance).
      /// </summary>
      /// <param name="instance">Instance to compare against the one held in _cacheInstanc.</param>
      /// <returns>True if passed instance is either null or not null and the same as stored in _cacheInstance; false otherwise.</returns>
      private bool VerifyInstance(IGlobalCache instance)
      {
         _callCount++;
         //Note that if _cacheInstance is null, then it assigns instance to it (and returns true)
         if (instance == null) return true;
         if (_cacheInstance == null) _cacheInstance = instance; //not thread-safe
         return _cacheInstance == instance;
      }


      [Fact]
      public void GlobalCache_VariousHandlers_SameInstance()
      {
         //arrange
         _cacheInstance = null;
         _callCount = 0;
         bool cacheIsSameInstance = true;
         _config.HeadersInFirstInputRow = true;
         _config.ClusterMarker = (r, pr, n) =>
         {
            if (!VerifyInstance(r.GlobalCache)) cacheIsSameInstance = false;
            if (!VerifyInstance(pr?.GlobalCache)) cacheIsSameInstance = false;  //note that previous record is null for the 1st record
            return true;
         };
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            if (!VerifyInstance(r.GlobalCache)) cacheIsSameInstance = false;
            return r;
         };
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c =>
         {
            if (!VerifyInstance(c.GlobalCache)) cacheIsSameInstance = false;
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);
         var unclusteringBlock = (TransformManyBlock<KeyValCluster, KeyValRecord>)orchestratorPA.GetField("_unclusteringBlock");
         unclusteringBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         //act
         var result = orchestrator.ExecuteAsync().Result;
         _resultsExtractor.Completion.Wait();

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(0);  //no output invoked during the test
         result.GlobalCache.Should().BeSameAs(_cacheInstance);

         _resultingRecords.Count.Should().Be(3);

         cacheIsSameInstance.Should().BeTrue();  //meaning all instances were the same
         _callCount.Should().Be(12); // 3 handlers called 3 times (one making 2 calls)
      }


      [Fact]
      public void GlobalCache_ReplaceAndRetrieveValue_SameValueAtOtherHandler()
      {
         //arrange
         object valueRetrieved = null; //value retrieved from cache
         int valCntAtRec1 = -1;  //number of values stored in cache at 1st record (by RecordInitiator)
         string valueAtRec1 = null;
         int valCntAtRec2 = -1;  //number of values stored in cache at 2nd record (by RecordInitiator)
         string valueAtRec2 = null;
         int valCntAtRec3 = -1;  //number of values stored in cache at 3rd record (by RecordInitiator)
         string valueAtRec3 = null;
         _config.HeadersInFirstInputRow = true;
         _config.GlobalCacheElements = new string[] { "key|" };
         _config.RecordInitiator = (r, tb) =>
         {
            //intake is single threaded, so no need for thread safety
            var gc = r.GlobalCache;
            if (r.RecNo == 1) { valCntAtRec1 = gc.Count; valueAtRec1 = gc["key"] as string; }
            if (r.RecNo == 2) { valCntAtRec2 = gc.Count; valueAtRec2 = gc["key"] as string; }
            if (r.RecNo == 3) { valCntAtRec3 = gc.Count; valueAtRec3 = gc["key"] as string; }
            var oldVal = gc["key"] as string;
            gc.TryReplace("key", oldVal + "blah", oldVal);
            return true;
         };
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c =>
         {
            var cache = c.GlobalCache;
            cache.TryGet("key", out valueRetrieved);
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);
         var unclusteringBlock = (TransformManyBlock<KeyValCluster, KeyValRecord>)orchestratorPA.GetField("_unclusteringBlock");
         unclusteringBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         //act
         var result = orchestrator.ExecuteAsync().Result;
         _resultsExtractor.Completion.Wait();

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(0);  //no output invoked during the test

         _resultingRecords.Count.Should().Be(3);

         valCntAtRec1.Should().Be(1);
         valueAtRec1.Should().BeOfType<string>();
         valueAtRec1.Should().Be(string.Empty);
         valCntAtRec2.Should().Be(1);
         valueAtRec2.Should().BeOfType<string>();
         valueAtRec2.Should().Be("blah");
         valCntAtRec3.Should().Be(1);
         valueAtRec3.Should().BeOfType<string>();
         valueAtRec3.Should().Be("blahblah");
         valueRetrieved.Should().BeOfType<string>();
         valueRetrieved.Should().Be("blahblahblah");

         result.GlobalCache.Count.Should().Be(1);
         result.GlobalCache["key"].Should().BeOfType<string>();
         result.GlobalCache["key"].Should().Be("blahblahblah");

         var gcElems = result.GlobalCache.Elements;
         gcElems.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         gcElems.Should().ContainSingle();
         gcElems[0].Key.Should().Be("key");
         gcElems[0].Value.Should().Be("blahblahblah");
      }

      [Fact]
      public void GlobalCache_Elements_CorrectData()
      {
         //arrange
         IReadOnlyList<KeyValuePair<string, object>> elemsAtInitRec1 = null;
         IReadOnlyList<KeyValuePair<string, object>> elemsAtInitRec2 = null;
         IReadOnlyList<KeyValuePair<string, object>> elemsAtInitRec3 = null;
         IReadOnlyList<KeyValuePair<string, object>> elemsAtRouterRec1 = null;
         IReadOnlyList<KeyValuePair<string, object>> elemsAtRouterRec2 = null;
         IReadOnlyList<KeyValuePair<string, object>> elemsAtRouterRec3 = null;
         _config.HeadersInFirstInputRow = true;
         _config.GlobalCacheElements = new string[] { "key1", "key2|", "key3|24" };
         _config.RecordInitiator = (r, tb) =>
         {
            var gc = r.GlobalCache;
            if (r.RecNo == 1) { elemsAtInitRec1 = gc.Elements; gc.ReplaceValue<object, int>("key1", _ => 16); }
            if (r.RecNo == 2) { elemsAtInitRec2 = gc.Elements; gc.ReplaceValue<object, string>("key2", _ => "blah1"); }
            if (r.RecNo == 3) { elemsAtInitRec3 = gc.Elements; gc.IncrementValue("key3"); }
            return true;
         };
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c =>
         {
            var gc = c.GlobalCache;
            if (c.StartRecNo == 1) { elemsAtRouterRec1 = gc.Elements; gc.IncrementValue("key3"); gc.ReplaceValue<object, string>("key2", _ => "blah2"); }
            if (c.StartRecNo == 2) { elemsAtRouterRec2 = gc.Elements; gc.IncrementValue("key1"); }
            if (c.StartRecNo == 3) { elemsAtRouterRec3 = gc.Elements; }
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);
         var unclusteringBlock = (TransformManyBlock<KeyValCluster, KeyValRecord>)orchestratorPA.GetField("_unclusteringBlock");
         unclusteringBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         //act
         var result = orchestrator.ExecuteAsync().Result;
         _resultsExtractor.Completion.Wait();

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(0);  //no output invoked during the test

         _resultingRecords.Count.Should().Be(3);

         elemsAtInitRec1.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         elemsAtInitRec1.Should().HaveCount(3);
         elemsAtInitRec1.Single(e => e.Key == "key1").Value.Should().BeNull();
         elemsAtInitRec1.Single(e => e.Key == "key2").Value.Should().Be(string.Empty);
         elemsAtInitRec1.Single(e => e.Key == "key3").Value.Should().Be(24);

         elemsAtInitRec2.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         elemsAtInitRec2.Should().HaveCount(3);
         elemsAtInitRec2.Single(e => e.Key == "key1").Value.Should().Be(16);
         elemsAtInitRec2.Single(e => e.Key == "key2").Value.Should().Be(string.Empty);
         elemsAtInitRec2.Single(e => e.Key == "key3").Value.Should().Be(24);

         elemsAtInitRec3.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         elemsAtInitRec3.Should().HaveCount(3);
         elemsAtInitRec3.Single(e => e.Key == "key1").Value.Should().Be(16);
         elemsAtInitRec3.Single(e => e.Key == "key2").Value.Should().Be("blah1");
         elemsAtInitRec3.Single(e => e.Key == "key3").Value.Should().Be(24);

         elemsAtRouterRec1.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         elemsAtRouterRec1.Should().HaveCount(3);
         elemsAtRouterRec1.Single(e => e.Key == "key1").Value.Should().Be(16);
         elemsAtRouterRec1.Single(e => e.Key == "key2").Value.Should().Be("blah1");
         elemsAtRouterRec1.Single(e => e.Key == "key3").Value.Should().Be(25);

         elemsAtRouterRec2.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         elemsAtRouterRec2.Should().HaveCount(3);
         elemsAtRouterRec2.Single(e => e.Key == "key1").Value.Should().Be(16);
         elemsAtRouterRec2.Single(e => e.Key == "key2").Value.Should().Be("blah2");
         elemsAtRouterRec2.Single(e => e.Key == "key3").Value.Should().Be(26);

         elemsAtRouterRec3.Should().BeOfType<ReadOnlyCollection<KeyValuePair<string, object>>>();
         elemsAtRouterRec3.Should().HaveCount(3);
         elemsAtRouterRec3.Single(e => e.Key == "key1").Value.Should().Be(17);
         elemsAtRouterRec3.Single(e => e.Key == "key2").Value.Should().Be("blah2");
         elemsAtRouterRec3.Single(e => e.Key == "key3").Value.Should().Be(26);
      }


      [Theory]
      [Repeat(20)]
      public void GlobalCache_TryReplaceInConcurrentAccess_CorrectData(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //Note the following pattern for replacing GlobalCache values using TryReplace in a thread-safe manner:
         //       T prevVal; //, where T is one of: int, DateTime, decimal or string
         //       do { prevVal = (T)gc["key"]; }
         //       while (!gc.TryReplace("key", CalcNewVal(prevVal), prevVal));

         //arrange
         int attemptCnt = 0;
         _config.HeadersInFirstInputRow = true;
         _config.BufferSize = 1;
         _config.GlobalCacheElements = new string[] { "STR|Exes", "DT|5/12/2011" };
         _config.ClusterMarker = (r, pr, n) =>
         {
            int attempts = ReplaceVal<string, string>(r.GlobalCache, "STR", s => s + "X", 1);  //3 Xs (upper case) appended (one per record)
            for (int i = 0; i < attempts; i++) Interlocked.Increment(ref attemptCnt);
            return true;
         };
         _config.RecordboundTransformer = r =>
         {
            int attempts = ReplaceVal<DateTime, DateTime>(r.GlobalCache, "DT", d => d.AddMonths(1), 2);  //3 months added
            for (int i = 0; i < attempts; i++) Interlocked.Increment(ref attemptCnt);
            return r;
         };
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c =>
         {
            int attempts = ReplaceVal<string, string>(c.GlobalCache, "STR", s => s + "x", 0)  // 2 xs (lower case) appended
                         + ReplaceVal<DateTime, DateTime>(c.GlobalCache, "DT", d => d.AddDays(1), 3);  // 3 days added
            for (int i = 0; i < attempts; i++) Interlocked.Increment(ref attemptCnt);
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);
         var unclusteringBlock = (TransformManyBlock<KeyValCluster, KeyValRecord>)orchestratorPA.GetField("_unclusteringBlock");
         unclusteringBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         //act
         var result = orchestrator.ExecuteAsync().Result;
         _resultsExtractor.Completion.Wait();

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(0);  //no output invoked during the test

         ((string)result.GlobalCache["STR"]).ToLower().Should().Be("exesxxxxxx");
         ((DateTime)result.GlobalCache["DT"]).Should().Be(new DateTime(2011, 8, 15));
         attemptCnt.Should().BeGreaterOrEqualTo(12);  //3 + 3 + 6 + retries
      }


      /// <summary>
      /// Helper function to replace the value held in global cache in a thread-safe manner.
      /// </summary>
      /// <typeparam name="TIn">Type of the old value held in global cache.</typeparam>
      /// <typeparam name="TOut">Type of the new (replacement) value held in global cache.</typeparam>
      /// <param name="gc">Reference to global cache.</param>
      /// <param name="key">Key of the value held in global cache.</param>
      /// <param name="formula">Formula to calculate the new value based on the old value.</param>
      /// <param name="delay">Time lapse between retries.</param>
      /// <returns></returns>
      private int ReplaceVal<TIn, TOut>(IGlobalCache gc, string key, Func<TIn, TOut> formula, int delay)
      {
         int retVal = 0;
         TIn oldVal;
         TOut newVal;
         do
         {
            oldVal = (TIn)gc[key];
            newVal = formula(oldVal);
            retVal++;
            Thread.Sleep(delay);
         }
         while (!gc.TryReplace(key, newVal, oldVal));
         return retVal;
      }


      [Theory]
      [Repeat(20)]
      public void GlobalCache_ReplaceValueInConcurrentAccess_CorrectData(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.HeadersInFirstInputRow = true;
         _config.GlobalCacheElements = new string[] { "STR|Exes", "DT|5/12/2011" };
         _config.ClusterMarker = (r, pr, n) =>
         {
            r.GlobalCache.ReplaceValue<string, string>("STR", s => s + "X");
            return true;
         };
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c =>
         {
            c.GlobalCache.ReplaceValue<DateTime, DateTime>("DT", d => d.AddMonths(1));
            return true;
         };
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) =>
         {
            r.GlobalCache.ReplaceValue<string, string>("STR", s => s + "x");
            r.GlobalCache.ReplaceValue<DateTime, DateTime>("DT", d => d.AddDays(1));
            return 1;
         };

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);
         var unclusteringBlock = (TransformManyBlock<KeyValCluster, KeyValRecord>)orchestratorPA.GetField("_unclusteringBlock");
         unclusteringBlock.LinkTo(_resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });

         //act
         var result = orchestrator.ExecuteAsync().Result;
         _resultsExtractor.Completion.Wait();

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(4);
         result.ClustersRead.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         result.RowsWritten.Should().Be(0);  //no output invoked during the test

         ((string)result.GlobalCache["STR"]).ToLower().Should().Be("exesxxxxxx");
         ((DateTime)result.GlobalCache["DT"]).Should().Be(new DateTime(2011, 8, 15));
      }


      [Theory]
      [Repeat(20)]
      public void GlobalCache_IncrementValue_CorrectAggregateCounter(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         _config.GlobalCacheElements = new string[] { "counter|0" };
         _config.IntakeInitializer = gc => { gc.IncrementValue("counter"); return null; };  //0 + 1 = 1
         _config.OutputInitializer = gc => { gc.IncrementValue("counter", 2); return null; };  //1 + 2 = 3
         _config.InputDataKind = KindOfTextData.Delimited;
         _config.HeadersInFirstInputRow = true;
         var rawIntakeSupplier = new IntakeSupplierProvider(_intakeLines()).StringSupplier;
         _config.IntakeSupplier = gc => { gc.IncrementValue("counter", 3); return rawIntakeSupplier()?.ToExternalTuple(); };  //3 + 5*3 = 18  (called 5 times: 4 lines incl. header + 1 null(EOD))
         _config.IntakeDisposer = gc => { gc.IncrementValue("counter", 4); };  //18 + 4 = 22
         _config.RecordInitiator = (r, tb) => { r.GlobalCache.IncrementValue("counter", 5); return true; };  //22 + 3*5 = 37
         _config.ClusterMarker = (r, pr, n) => { r.GlobalCache.IncrementValue("counter", 6); return false; };  //37 + 3*6 = 55  (note that a single cluster created)
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = c => { c.GlobalCache.IncrementValue("counter", 7); return c; };  //55 + 7 = 62
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => { c.GlobalCache.IncrementValue("counter", 8); return 1; };  //62 + 8 = 70
         _config.OutputConsumer = (tpl, gc) => { gc.IncrementValue("counter", 9); };  //70 + 4*9 = 106  (throwaway consumer; executed 4 times - 3 lines no header + 1 null(EOD))
         _config.OutputDisposer = gc => { gc.IncrementValue("counter", 10); };  //106 + 10 = 116

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.GlobalCache["counter"].Should().BeOfType<int>();
         result.GlobalCache["counter"].Should().Be(116);  //total of all increments
      }


      // Order table for " outputVal = (outputVal + recNo) * recNo" formula in tests below:
      //  Order     Result
      //  1-2-3-4    124
      //  1-2-4-3    129
      //  1-3-2-4    128
      //  1-3-4-2    132
      //  1-4-2-3    141
      //  1-4-3-2    142
      //  2-1-3-4    112
      //  2-1-4-3    117
      //  2-3-1-4    104
      //  2-3-4-1    101
      //  2-4-1-3    108
      //  2-4-3-1    106
      //  3-1-2-4    112
      //  3-1-4-2    116
      //  3-2-1-4    108
      //  3-2-4-1    105
      //  3-4-1-2    110
      //  3-4-2-1    109
      //  4-1-2-3    123
      //  4-1-3-2    124
      //  4-2-1-3    120
      //  4-2-3-1    118
      //  4-3-1-2    120
      //  4-3-2-1    119


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void GlobalCache_NoAwaits_ProcessInOrder(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //Note: This test fails on occasion because the order cannot be forced based on time delays.
         //      When repeated 1,000 times, it usually fails once or twice.

         //arrange
         var outputVal = 0;
         var locker = new object();
         _config.ConcurrencyLevel = 4;  //so that each cluster has it's own thread
         _config.TransformerType = TransformerType.Recordbound;  //note that each record is in a separate cluster
         _config.RecordboundTransformer = r =>
         {  //in this test delays cause records to be processed in reverse order: 4 3 2 1
            var recNo = r.RecNo;
            switch (recNo)
            {
               case 1:
                  Thread.Sleep(8);
                  break;
               case 2:
                  Thread.Sleep(6);
                  break;
               case 3:
                  Thread.Sleep(4);
                  break;
               case 4:
                  Thread.Sleep(2);
                  break;
            }
            lock (locker)
            {
               outputVal = (outputVal + recNo) * recNo;
            }
            return r;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         outputVal.Should().Be(119); // 0 .. (0+4)*4=16 .. (16+3)*3=57 .. (57+2)*2=118 .. (118+1)*1=119
      }


      [Theory]
      [Repeat(20)]
      public void GlobalCache_AwaitSignal_Respected(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         var outputVal = 0;
         var locker = new object();
         _config.ConcurrencyLevel = 4;  //so that each cluster has it's own thread
         _config.TransformerType = TransformerType.Recordbound;  //note that each record is in a separate cluster
         _config.RecordboundTransformer = r =>
         {  //in this test, rec 2 waits for rec 1, rec 3 waits for rec 2 and rec 4 waits for rec 3
            // so that order 4 3 2 1 (based on delays) becomes: 1 2 3 4
            var recNo = r.RecNo;
            switch (recNo)
            {
               case 1:
                  Thread.Sleep(8);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  r.GlobalCache.RaiseSignal("GreenLightForRec2");
                  break;
               case 2:
                  r.GlobalCache.AwaitSignal("GreenLightForRec2");
                  Thread.Sleep(6);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  r.GlobalCache.RaiseSignal("GreenLightForRec3");
                  break;
               case 3:
                  r.GlobalCache.AwaitSignal("GreenLightForRec3");
                  Thread.Sleep(4);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  r.GlobalCache.RaiseSignal("GreenLightForRec4");
                  break;
               case 4:
                  r.GlobalCache.AwaitSignal("GreenLightForRec4");
                  Thread.Sleep(2);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  break;
            }
            return r;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         outputVal.Should().Be(124); // 0 .. (0+1)*1=1 .. (1+2)*2=6 .. (6+3)*3=27 .. (27+4)*4=124
      }


      [Theory]
      [Repeat(20)]
      public void GlobalCache_AwaitConditionGC_Respected(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         var outputVal = 0;
         var locker = new object();

         _config.ConcurrencyLevel = 4;  //so that each cluster has it's own thread
         _config.GlobalCacheElements = new string[] { "counter|0" };
         _config.TransformerType = TransformerType.Recordbound;  //note that each record is in a separate cluster
         _config.RecordboundTransformer = r =>
         {  //in this test, rec 3 waits for rec 1, rec 2 and rec 4 (to have counter sufficiently incremented)
            // so that order 4 3 2 1 (based on delays) most likely becomes: 4 2 1 3,
            // i.e. most likely outputVal is 120, i.e. 4 2 1 3(0 .. (0+4)*4=16 .. (16+2)*2=36 .. (36+1)*1=37 .. (37+3)*3=120)
            // However, delays do not guarantee order and the only one guaranteed via AwaitCondition, i.e. is that recNo 3 is processed last.
            // Hence, output value can be any of 129, 141, 117, 108, 123 or 120.
            var recNo = r.RecNo;
            switch (recNo)
            {
               case 1:
                  Thread.Sleep(8);
                  r.GlobalCache.IncrementValue("counter");
                  break;
               case 2:
                  Thread.Sleep(6);
                  r.GlobalCache.IncrementValue("counter");
                  break;
               case 3:
                  r.GlobalCache.AwaitCondition(gc => (int)gc["counter"] >= 3);
                  Thread.Sleep(4);
                  break;
               case 4:
                  Thread.Sleep(2);
                  r.GlobalCache.IncrementValue("counter");
                  break;
            }
            lock (locker)
            {
               outputVal = (outputVal + recNo) * recNo;
            }
            return r;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         outputVal.Should().BeOneOf(129, 141, 117, 108, 123, 120);  //see comment above
      }


      [Theory]
      [Repeat(20)]
      public void GlobalCache_AwaitConditionClosure_Respected(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         var outputVal = 0;
         var counter = 0;
         var locker = new object();
         _config.ConcurrencyLevel = 4;  //so that each cluster has it's own thread
         _config.TransformerType = TransformerType.Recordbound;  //note that each record is in a separate cluster
         _config.RecordboundTransformer = r =>
         {  //in this test, recs 4, 2 and 1 wait to have counter sufficiently incremented
            // so that order becomes: 3-4-2-1
            var recNo = r.RecNo;

            switch (recNo)
            {
               case 1:
                  r.GlobalCache.AwaitCondition(gc => counter >= 3);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  Interlocked.Increment(ref counter); //not even needed for the test
                  break;
               case 2:
                  r.GlobalCache.AwaitCondition(gc => counter >= 2);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  Interlocked.Increment(ref counter);
                  break;
               case 3:
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  Interlocked.Increment(ref counter);
                  break;
               case 4:
                  r.GlobalCache.AwaitCondition(gc => counter >= 1);
                  lock (locker) { outputVal = (outputVal + recNo) * recNo; }
                  Interlocked.Increment(ref counter);
                  break;
            }
            return r;
         };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         outputVal.Should().Be(109); // order of 3-4-2-1
      }

   }
}
