//ClusterMarker_tests.cs
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   //Alias for the tuple that holds definition of the TraceableAsserter output, i.e. (Ext,Header,Formatter,ExcFormatter):
   using AsserterOutput = ValueTuple<string, string, Func<KeyValCluster, string>, Func<Exception, IEnumerable<string>>>;

   public class ClusterMarker_tests
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=\"Mary, Ann\",@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=523";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=623";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=723";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=823";
         yield return "@pABCD_ID=XYZ00883,@pNAME=Cindy,@pNUM=923,@pRECTYPE=ABCD";
         yield return "EOF";
      }

      //Result of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container


      //Executor of a series of asserts; in case assert faile, a text file (or files) is/are saved for manual examination.
      private readonly TraceableAsserter<KeyValCluster> _traceableAsserter;

      public ClusterMarker_tests()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Keyword
         };
         var sn = 0;  //closure to facilitate SourceNo calculation (note that GetStringTupleSupplier calls the sourceNoEval function (provided as parameter below) exactly once per iteration)
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).GetStringTupleSupplier(() => sn++ < 3 ? 1 : 2));  //first 3 - source 1, rest - source 2
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         // The DeferTransformation.Indefinitely setting below prevents Data Conveyer from linking clusteringBlock to transformingBlock.
         // Without it, transformingBlock could randomly "steal" clusters from _resultsExtractor, which would cause randowm test failures.
         _config.DeferTransformation = DeferTransformation.Indefinitely;                                                                              
         _config.AllowOnTheFlyInputFields = true;
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         //prepare extraction of the results from the pipeline
         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));

         // AsserterOutput tuple: Item1=Ext, Item2=Header, Item3=Formatter, Item4=ExcFormatter
         string ShowRec(ICluster c, int idx) => idx >= c.Count ? string.Empty : c[idx]?["NUM"] + "{" + c[idx].Count + "}"; // e.g. 223{4} means record with 4 items and item NUM = 223
         AsserterOutput asserterOutputToCsv = (".csv",
                                                "ClstrNo,NoOfRecs,Rec1,Rec2,Rec3,Rec4,Rec5,Rec6,Rec7,Rec8,Rec9,Rec10",
                                                c => $"{ c.ClstrNo },{ c.Count },{ ShowRec(c,0) },{ ShowRec(c,1) },{ ShowRec(c,2) },{ ShowRec(c,3) },{ ShowRec(c,4) },{ ShowRec(c,5) },{ ShowRec(c,6) },{ ShowRec(c,7) },{ ShowRec(c,8) },{ ShowRec(c,9) }",
                                                ex => ex.ToString().Split("\r\n").Select(l => "\"" + l.Replace('"','\'') + "\"")
                                              );
         _traceableAsserter = new TraceableAsserter<KeyValCluster>("ClusterTestFailures\\", asserterOutputToCsv);
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_ByRecordContentsStart_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_ByRecordContentsStart_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(2);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(4);
            resultingClusters[1].Count.Should().Be(6);

            resultingClusters[0][3]["NUM"].Should().Be("423");
            resultingClusters[1][5].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_ByRecordContentsEnd_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote end of the cluster
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_ByRecordContentsEnd_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(3);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(1);
            resultingClusters[1].Count.Should().Be(4);
            resultingClusters[2].Count.Should().Be(5);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[1][0]["NUM"].Should().Be("223");
            resultingClusters[2][0]["NUM"].Should().Be("623");
            resultingClusters[2][4].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_ByContentsDifferenceStart_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec?["NAME"] != (string)prevRec?["NAME"]; };  //different NAME denotes start of cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_ByContentsDifferenceStart_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(5);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(1);
            resultingClusters[1].Count.Should().Be(5);
            resultingClusters[2].Count.Should().Be(2);
            resultingClusters[3].Count.Should().Be(1);
            resultingClusters[4].Count.Should().Be(1);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[1][0]["NUM"].Should().Be("223");
            resultingClusters[1][4]["NUM"].Should().Be("623");
            resultingClusters[2][0]["NUM"].Should().Be("723");
            resultingClusters[3][0]["NUM"].Should().Be("923");
            resultingClusters[4][0].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_ByContentsDifferenceEnd_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //not a very practical scenario when difference in contents ends a cluster, but still a good test

         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec?["NAME"] != (string)prevRec?["NAME"]; };  //different NAME denotes end of cluster
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_ByContentsDifferenceEnd_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(5);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(1);
            resultingClusters[1].Count.Should().Be(1);
            resultingClusters[2].Count.Should().Be(5);
            resultingClusters[3].Count.Should().Be(2);
            resultingClusters[4].Count.Should().Be(1);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[1][0]["NUM"].Should().Be("223");
            resultingClusters[2][0]["NUM"].Should().Be("323");
            resultingClusters[2][4]["NUM"].Should().Be("723");
            resultingClusters[3][0]["NUM"].Should().Be("823");
            resultingClusters[4][0].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_OnEveryRecordStart_SingleRecordClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_OnEveryRecordStart_SingleRecordClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
            {
               resultingClusters.Should().HaveCount(10);
               resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
               resultingClusters.Select(c => c.Count).Should().OnlyContain(c => c == 1);  //each cluster has 1 record
               resultingClusters.Select(c => c.Records.Count).Should().OnlyContain(c => c == 1);

               resultingClusters[0][0]["NUM"].Should().Be("123");
               resultingClusters[2][0]["NUM"].Should().Be("323");
               resultingClusters[3][0]["NUM"].Should().Be("423");
               resultingClusters[9][0].Count.Should().Be(1); //just EOF
            });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_OnEveryRecordEnd_SingleRecordClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_OnEveryRecordEnd_SingleRecordClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
            {
               resultingClusters.Should().HaveCount(10);
               resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
               resultingClusters.Select(c => c.Count).Should().OnlyContain(c => c == 1);  //each cluster has 1 record
               resultingClusters.Select(c => c.Records.Count).Should().OnlyContain(c => c == 1);

               resultingClusters[0][0]["NUM"].Should().Be("123");
               resultingClusters[2][0]["NUM"].Should().Be("323");
               resultingClusters[3][0]["NUM"].Should().Be("423");
               resultingClusters[9][0].Count.Should().Be(1); //just EOF
            });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_NoClusteringStart_SingleCluster(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return false; };
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_NoClusteringStart_SingleCluster) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
            {
               resultingClusters.Should().HaveCount(1);
               resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
               resultingClusters[0].Count.Should().Be(10);
               resultingClusters[0].Records.Should().HaveCount(10);

               resultingClusters[0][0]["NUM"].Should().Be("123");
               resultingClusters[0][2]["NUM"].Should().Be("323");
               resultingClusters[0][3]["NUM"].Should().Be("423");
               resultingClusters[0][9].Count.Should().Be(1); //just EOF
            });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_NoClusteringEnd_SingleCluster(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return false; };
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_NoClusteringEnd_SingleCluster) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
            {
               resultingClusters.Should().HaveCount(1);
               resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
               resultingClusters[0].Count.Should().Be(10);
               resultingClusters[0].Records.Should().HaveCount(10);

               resultingClusters[0][0]["NUM"].Should().Be("123");
               resultingClusters[0][2]["NUM"].Should().Be("323");
               resultingClusters[0][3]["NUM"].Should().Be("423");
               resultingClusters[0][9].Count.Should().Be(1); //just EOF
            });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_BySourceChangeStart_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return rec?.SourceNo != prevRec?.SourceNo; };  //different Source denotes start of cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_BySourceChangeStart_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(2);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(3);
            resultingClusters[1].Count.Should().Be(7);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[0][2]["NUM"].Should().Be("323");
            resultingClusters[1][0]["NUM"].Should().Be("423");
            resultingClusters[1][6].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_BySourceChangeEnd_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //this is another not so practical setup of cluster marker, as in case of the first record previous record is null, so that break occurs after the 1st record

         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return rec?.SourceNo != prevRec?.SourceNo; };  //different Source denotes end of cluster
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_BySourceChangeEnd_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(3);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(1);
            resultingClusters[1].Count.Should().Be(3);
            resultingClusters[2].Count.Should().Be(6);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[1][0]["NUM"].Should().Be("223");
            resultingClusters[1][2]["NUM"].Should().Be("423");
            resultingClusters[2][0]["NUM"].Should().Be("523");
            resultingClusters[2][5].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_Every3RecsStart_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return recCnt % 3 == 0; };  //break when 3 records accumulated so far
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_Every3RecsStart_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(4);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(3);
            resultingClusters[1].Count.Should().Be(3);
            resultingClusters[2].Count.Should().Be(3);
            resultingClusters[3].Count.Should().Be(1);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[0][2]["NUM"].Should().Be("323");
            resultingClusters[1][0]["NUM"].Should().Be("423");
            resultingClusters[1][2]["NUM"].Should().Be("623");
            resultingClusters[2][0]["NUM"].Should().Be("723");
            resultingClusters[2][2]["NUM"].Should().Be("923");
            resultingClusters[3][0].Count.Should().Be(1); //just EOF
         });
      }


      [Theory]
      [Repeat(5)]
      public void ClusterMarker_Every3RecsEnd_CorrectClusters(int iterationNumber, int totalRepeats)
      {
         //Interestingly, when breaking every n-th record, the cluster breaks are the same regardless if MarkerStartsCluster is true or false.
         //This is because if false (marker points to last record), then current record is already accounted for among accumulated records.

         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return recCnt % 3 == 0; };  //break when 3 records accumulated so far
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync(); //this task will never complete due to DeferTransformation.Indefinitely
         _resultsExtractor.Completion.Wait();

         //assert
         _traceableAsserter.ExecuteAssertsWithSaveOnFailure(
            $"{ nameof(this.ClusterMarker_Every3RecsEnd_CorrectClusters) } (#{ iterationNumber } of { totalRepeats }) run on { DateTime.Now }",
            _resultingClusters.ToArray().ToList(), resultingClusters =>
         {
            resultingClusters.Should().HaveCount(4);
            resultingClusters.Sum(c => c.Count).Should().Be(10);  //total records
            resultingClusters[0].Count.Should().Be(3);
            resultingClusters[1].Count.Should().Be(3);
            resultingClusters[2].Count.Should().Be(3);
            resultingClusters[3].Count.Should().Be(1);

            resultingClusters[0][0]["NUM"].Should().Be("123");
            resultingClusters[0][2]["NUM"].Should().Be("323");
            resultingClusters[1][0]["NUM"].Should().Be("423");
            resultingClusters[1][2]["NUM"].Should().Be("623");
            resultingClusters[2][0]["NUM"].Should().Be("723");
            resultingClusters[2][2]["NUM"].Should().Be("923");
            resultingClusters[3][0].Count.Should().Be(1); //just EOF
         });
      }

   }
}
