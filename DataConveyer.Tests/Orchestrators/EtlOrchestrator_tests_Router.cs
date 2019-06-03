//EtlOrchestrator_tests_Router.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   public class EtlOrchestrator_tests_Router
   {
      OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=123";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=223";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=323";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=423";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=523";
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=623";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=723";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=823";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@pNUM=923";
      }

      //Output from the pipiline
      ConcurrentQueue<Tuple<ExternalLine, int>> _resultingLines;  //Item2=targetNo

      public EtlOrchestrator_tests_Router()
      {
         _config = new OrchestratorConfig();
         _config.InputDataKind = KindOfTextData.Keyword;
         var sn = 0;  //closure to facilitate SourceNo calculation (note that GetStringTupleSupplier calls the sourceNoEval function (provided as parameter below) exactly once per iteration)
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).GetStringTupleSupplier(() => sn++ % 3 + 1));  //assign sourceNo in a round-robin fashion: 1,2,3,1,2,3,1,2,3
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         _config.SetOutputConsumer( tpl => _resultingLines.Enqueue(tpl));  //Item1=ExternalLine/Xrecord, Item2=targetNo

         _resultingLines = new ConcurrentQueue<Tuple<ExternalLine, int>>();
      }


      [Fact]
      public void processRoundRobinSources_SingleTargetRouter_TargetNoAlwaysOne()
      {
         //arrange
         _config.RouterType = RouterType.SingleTarget;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         _resultingLines.Should().HaveCount(10); //incl. EOD mark (null)
         _resultingLines.Skip(9).First().Should().BeNull();

         var targetNumbers = _resultingLines.Take(9).Select(t => t.Item2).ToList();
         targetNumbers.Should().HaveCount(9);
         targetNumbers.Should().OnlyContain(tn => tn == 1);  //all targetNo=1
      }


      [Fact]
      public void processRoundRobinSources_DefaultRouter_SameAsSingleTarget()
      {
         //no Router defined in config - default router is SingleTarget (every record routed to target 1)

         //arrange
         _config.OutputDataKind = KindOfTextData.XML;  //if Xrecord type, then ClstrNo is populated in ExternalLine (unrelated to routing, but can't hurt)

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         _resultingLines.Should().HaveCount(10); //incl. EOD mark (null)
         _resultingLines.Skip(9).First().Should().BeNull();

         var targetNumbers = _resultingLines.Take(9).Select(t => t.Item2).ToList();
         targetNumbers.Should().HaveCount(9);
         targetNumbers.Should().OnlyContain(tn => tn == 1);  //all targetNo=1

         //Asserts below are unrelated to routing, but itcan't hurt to verify that 1st 5 records were for clstr 1 and the remaining ones for clstr 2
         var clusterNumbers = _resultingLines.Take(9).Select(t => t.Item1.ClstrNo);
         clusterNumbers.Take(5).Should().OnlyContain(cn => cn == 1);
         clusterNumbers.Skip(5).Take(4).Should().OnlyContain(cn => cn == 2);
      }


      [Fact]
      public void processRoundRobinSources_SourceToTargetRouter_TargetNoSameAsSourceNo()
      {
         //arrange
         _config.RouterType = RouterType.SourceToTarget;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         _resultingLines.Should().HaveCount(10); //incl. EOD mark (null)
         _resultingLines.Skip(9).First().Should().BeNull();

         var targetNumbers = _resultingLines.Take(9).Select(t => t.Item2).ToList();
         targetNumbers.Should().HaveCount(9);
         targetNumbers.Where((tn, i) => i % 3 == 0).Should().OnlyContain(tn => tn == 1);
         targetNumbers.Where((tn, i) => i % 3 == 1).Should().OnlyContain(tn => tn == 2);
         targetNumbers.Where((tn, i) => i % 3 == 2).Should().OnlyContain(tn => tn == 3);
      }


      [Fact]
      public void processRoundRobinSources_PerClusterRouter_CorrectTargetNos()
      {
         //arrange
         _config.RouterType = RouterType.PerCluster;
         _config.ClusterRouter = c => c.ClstrNo + 10;   //1st cluster (5 recs) goes to target 11, the 2nd (4 recs) to 12

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         _resultingLines.Should().HaveCount(10); //incl. EOD mark (null)
         _resultingLines.Skip(9).First().Should().BeNull();

         var targetNumbers = _resultingLines.Take(9).Select(t => t.Item2).ToList();
         targetNumbers.Should().HaveCount(9);
         targetNumbers.Where((tn, i) => i < 5).Should().OnlyContain(tn => tn == 11);
         targetNumbers.Where((tn, i) => i >= 5).Should().OnlyContain(tn => tn == 12);
      }


      [Fact]
      public void processRoundRobinSources_PerRecordRouter_CorrectTargetNos()
      {
         //arrange
         _config.ExplicitTypeDefinitions = "NUM|I";  // NUM field is int
         _config.RouterType = RouterType.PerRecord;
         _config.RecordRouter = (r, c) => c.ClstrNo == 1 ? (int)(r["NUM"]) - 23 : (int)(r["NUM"]) + 27;  //1st 5 recs: 100, 200, .. last 4 recs: 650, 750, ..

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(9);
         counts.ClustersRead.Should().Be(2);
         counts.RowsWritten.Should().Be(9);
         counts.ClustersWritten.Should().Be(2);

         _resultingLines.Should().HaveCount(10); //incl. EOD mark (null)
         _resultingLines.Skip(9).First().Should().BeNull();

         var targetNumbers = _resultingLines.Take(9).Select(t => t.Item2).ToList();
         targetNumbers.Should().HaveCount(9);
         targetNumbers[0].Should().Be(100);
         targetNumbers[1].Should().Be(200);
         targetNumbers[2].Should().Be(300);
         targetNumbers[3].Should().Be(400);
         targetNumbers[4].Should().Be(500);
         targetNumbers[5].Should().Be(650);
         targetNumbers[6].Should().Be(750);
         targetNumbers[7].Should().Be(850);
         targetNumbers[8].Should().Be(950);
      }

   }
}
