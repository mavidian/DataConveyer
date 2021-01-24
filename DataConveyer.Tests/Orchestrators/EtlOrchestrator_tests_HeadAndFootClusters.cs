//EtlOrchestrator_tests_HeadAndFootClusters.cs
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
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   public class EtlOrchestrator_tests_HeadAndFootClusters
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Susan,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=\"Mary,Ann\",@pNUM=423";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=523";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=623";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Joan,@pNUM=723";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=823";
         yield return "@pABCD_ID=XYZ00883,@pNAME=Cindy,@pNUM=923,@pRECTYPE=ABCD";
         yield return "EOF=";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_HeadAndFootClusters()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Keyword
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         _config.DeferTransformation = DeferTransformation.Indefinitely;  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }

      [Fact]
      public void ProcessPipeline_NoHeadOrFootCluster_Baseline()
      {
         //arrange
         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters[0].ClstrNo.Should().Be(1);
         resultingClusters[1].ClstrNo.Should().Be(2);

         ValidateCoreClusters(resultingClusters);
      }

      [Fact]
      public void ProcessPipeline_HeadClusterOnly_CorrectData()
      {
         //arrange
         _config.PrependHeadCluster = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Count.Should().Be(3);
         resultingClusters[0].Count.Should().Be(0);  //head cluster
         resultingClusters[1].Count.Should().Be(4);
         resultingClusters[2].Count.Should().Be(6);

         resultingClusters[0].ClstrNo.Should().Be(1);  //head cluster
         resultingClusters[1].ClstrNo.Should().Be(2);
         resultingClusters[2].ClstrNo.Should().Be(3);

         resultingClusters[0].StartRecNo.Should().Be(Constants.HeadClusterRecNo);  //head cluster
         resultingClusters[0].StartSourceNo.Should().Be(1);

         ValidateCoreClusters(resultingClusters.Skip(1).ToList());
      }


      [Fact]
      public void ProcessPipeline_FootClusterOnly_CorrectData()
      {
         //arrange
         _config.AppendFootCluster = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Count.Should().Be(3);
         resultingClusters[0].Count.Should().Be(4);
         resultingClusters[1].Count.Should().Be(6);
         resultingClusters[2].Count.Should().Be(0);  //foot cluster

         resultingClusters[0].ClstrNo.Should().Be(1);
         resultingClusters[1].ClstrNo.Should().Be(2);
         resultingClusters[2].ClstrNo.Should().Be(3);  //foot cluster

         ValidateCoreClusters(resultingClusters.Take(2).ToList());

         resultingClusters[2].StartRecNo.Should().Be(Constants.FootClusterRecNo);  //foot cluster
         resultingClusters[2].StartSourceNo.Should().Be(1);
      }


      [Fact]
      public void ProcessPipeline_BothHeadAndFootClusters_CorrectData()
      {
         //arrange
         _config.PrependHeadCluster = true;
         _config.AppendFootCluster = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Count.Should().Be(4);
         resultingClusters[0].Count.Should().Be(0);  //head cluster
         resultingClusters[1].Count.Should().Be(4);
         resultingClusters[2].Count.Should().Be(6);
         resultingClusters[3].Count.Should().Be(0);  //foot cluster

         resultingClusters[0].ClstrNo.Should().Be(1);  //head cluster
         resultingClusters[1].ClstrNo.Should().Be(2);
         resultingClusters[2].ClstrNo.Should().Be(3);
         resultingClusters[3].ClstrNo.Should().Be(4);  //foot cluster

         resultingClusters[0].StartRecNo.Should().Be(Constants.HeadClusterRecNo);  //head cluster
         resultingClusters[0].StartSourceNo.Should().Be(1);

         ValidateCoreClusters(resultingClusters.Skip(1).Take(2).ToList());

         resultingClusters[3].StartRecNo.Should().Be(Constants.FootClusterRecNo);  //foot cluster
         resultingClusters[3].StartSourceNo.Should().Be(1);
      }


      [Theory]
      [Repeat(2)]
      public void ProcessPipeline_LongTransformation_HeadClusterFirst(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         int firstClstr = -100;  //indicator of first cluster that got processed (-100 = nothing yet; 13 = "regular" cluster; 42 = head cluster)

         _config.PrependHeadCluster = true;
         _config.ConcurrencyLevel = 3;  //so that 2 regular clusters can start at the same time as head cluster
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = clstr =>
         {
            if (clstr.StartRecNo == Constants.HeadClusterRecNo)
            {  //head cluster
               Thread.Sleep(2);  //to allow 1st regular cluster go first
               Interlocked.CompareExchange(ref firstClstr, 42, -100);
            }
            else
            {  //"regular" cluster
               Interlocked.CompareExchange(ref firstClstr, 13, -100);
            }
            return clstr;
         };
         _config.DeferTransformation = DeferTransformation.NotDeferred;
         _config.DeferOutput = DeferOutput.Indefinitely;  //so that Output won't steal resulting clusters

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);
         resultingClusters[0].Count.Should().Be(0);  //head cluster
         resultingClusters[1].Count.Should().Be(4);
         resultingClusters[2].Count.Should().Be(6);
         ValidateCoreClusters(resultingClusters.Skip(1).ToList());

         firstClstr.Should().Be(42);  //i.e. head cluster got processed before any other clusters
      }


      [Theory]
      [Repeat(2)]
      public void ProcessPipeline_LongTransformation_FootClusterLast(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //arrange
         int highClstrNo = 0;  //high# (i.e. last processed cluster#) assigned by "regular" clusters
         int highClstrNoAtFoot = 0;  //high# encountered by foot cluster

         _config.AppendFootCluster = true;
         _config.ConcurrencyLevel = 3;  //so that foot cluster can start at the same time as 2 regular clusters
         _config.TransformerType = TransformerType.Clusterbound;
         _config.ClusterboundTransformer = clstr =>
         {
            if (clstr.StartRecNo != Constants.FootClusterRecNo)
            {  //"regular" (non-foot) cluster
                  Thread.Sleep(2);  //to allow foot cluster go first
                  int highSoFar;
               do
               {
                  highSoFar = highClstrNo;
                  if (highSoFar > clstr.ClstrNo) break;
               }
               while (Interlocked.CompareExchange(ref highClstrNo, clstr.ClstrNo, highSoFar) != highSoFar);
            }
            else
            {  //foot cluster
                  highClstrNoAtFoot = highClstrNo;
            }
            return clstr;
         };
         _config.DeferTransformation = DeferTransformation.NotDeferred;
         _config.DeferOutput = DeferOutput.Indefinitely;  //so that Output won't steal resulting clusters

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);
         resultingClusters[0].Count.Should().Be(4);
         resultingClusters[1].Count.Should().Be(6);
         ValidateCoreClusters(resultingClusters.Take(2).ToList());
         resultingClusters[2].Count.Should().Be(0);  //foot cluster

         highClstrNoAtFoot.Should().Be(2);  //i.e. 2nd cluster got processed before foot cluster
      }


      private void ValidateCoreClusters(List<KeyValCluster> coreClusters)
      {
         //coreClusters are resulting clusers less head or foot clusters
         coreClusters.Should().HaveCount(2);

         coreClusters[0].Count.Should().Be(4);
         coreClusters[1].Count.Should().Be(6);

         var kvRec = coreClusters[0][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["NAME"].Should().Be("Mary");
         kvRec["NUM"].Should().Be("123");

         kvRec = coreClusters[0][2];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Susan");

         kvRec = coreClusters[1][0];
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["NAME"].Should().Be("Mary");
         kvRec["NUM"].Should().Be("523");

         kvRec = coreClusters[1][4];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Cindy");
         kvRec["NUM"].Should().Be("923");

         kvRec = coreClusters[1][5];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().Be(string.Empty);
      }

   }
}
