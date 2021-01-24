//SourceNumbers_tests.cs
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Intake
{
   public class SourceNumbers_tests
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=123";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=223";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=323";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=423";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=523";
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=623";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=723";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=823";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=923";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public SourceNumbers_tests()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Keyword,
            InputKeyPrefix = "@p",
            //no type definitions (everything string)
            ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; },  //records having @pRECTYPE=XYZ denote start of the cluster
            MarkerStartsCluster = true,  //predicate matches the first record in cluster 
            AllowOnTheFlyInputFields = true,
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessIntake_RoundRobin_CorrectSourceNumbers()
      {
         //arrange
         var sn = 0;  //closure to facilitate SourceNo calculation (note that GetStringTupleSupplier calls the sourceNoEval function (provided as parameter below) exactly once per iteration)
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).GetStringTupleSupplier(() => sn++ % 3 + 1));  //assign sourceNo in a round-robin fashion: 1,2,3,1,2,3,1,2,3

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);

         resultingClusters[0].Count.Should().Be(5);
         resultingClusters[0].StartRecNo.Should().Be(1);
         resultingClusters[0].StartSourceNo.Should().Be(1);
         resultingClusters[0][0].RecNo.Should().Be(1);
         resultingClusters[0][0].SourceNo.Should().Be(1);
         resultingClusters[0][1].RecNo.Should().Be(2);
         resultingClusters[0][1].SourceNo.Should().Be(2);
         resultingClusters[0][2].RecNo.Should().Be(3);
         resultingClusters[0][2].SourceNo.Should().Be(3);
         resultingClusters[0][3].RecNo.Should().Be(4);
         resultingClusters[0][3].SourceNo.Should().Be(1);
         resultingClusters[0][4].RecNo.Should().Be(5);
         resultingClusters[0][4].SourceNo.Should().Be(2);

         resultingClusters[1].Count.Should().Be(4);
         resultingClusters[1].StartRecNo.Should().Be(6);
         resultingClusters[1].StartSourceNo.Should().Be(3);
         resultingClusters[1][0].RecNo.Should().Be(6);
         resultingClusters[1][0].SourceNo.Should().Be(3);
         resultingClusters[1][1].RecNo.Should().Be(7);
         resultingClusters[1][1].SourceNo.Should().Be(1);
         resultingClusters[1][2].RecNo.Should().Be(8);
         resultingClusters[1][2].SourceNo.Should().Be(2);
         resultingClusters[1][3].RecNo.Should().Be(9);
         resultingClusters[1][3].SourceNo.Should().Be(3);
      }


      [Fact]
      public void ProcessIntake_Sequential_CorrectSourceNumbers()
      {
         //arrange
         var sn = 0;  //closure to facilitate SourceNo calculation (note that GetStringTupleSupplier calls the sourceNoEval function (provided as parameter below) exactly once per iteration)
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).GetStringTupleSupplier(() => sn++ < 5 ? 1 : 2));  //assign sourceNo sequentially: 1st 5 recs - 1, last 4 - 2 (it coincidentally matches cluster splits)

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);

         resultingClusters[0].Count.Should().Be(5);
         resultingClusters[0].StartRecNo.Should().Be(1);
         resultingClusters[0].StartSourceNo.Should().Be(1);
         resultingClusters[0][0].RecNo.Should().Be(1);
         resultingClusters[0][0].SourceNo.Should().Be(1);
         resultingClusters[0][1].RecNo.Should().Be(2);
         resultingClusters[0][1].SourceNo.Should().Be(1);
         resultingClusters[0][2].RecNo.Should().Be(3);
         resultingClusters[0][2].SourceNo.Should().Be(1);
         resultingClusters[0][3].RecNo.Should().Be(4);
         resultingClusters[0][3].SourceNo.Should().Be(1);
         resultingClusters[0][4].RecNo.Should().Be(5);
         resultingClusters[0][4].SourceNo.Should().Be(1);

         resultingClusters[1].Count.Should().Be(4);
         resultingClusters[1].StartRecNo.Should().Be(6);
         resultingClusters[1].StartSourceNo.Should().Be(2);
         resultingClusters[1][0].RecNo.Should().Be(6);
         resultingClusters[1][0].SourceNo.Should().Be(2);
         resultingClusters[1][1].RecNo.Should().Be(7);
         resultingClusters[1][1].SourceNo.Should().Be(2);
         resultingClusters[1][2].RecNo.Should().Be(8);
         resultingClusters[1][2].SourceNo.Should().Be(2);
         resultingClusters[1][3].RecNo.Should().Be(9);
         resultingClusters[1][3].SourceNo.Should().Be(2);
      }

   }
}
