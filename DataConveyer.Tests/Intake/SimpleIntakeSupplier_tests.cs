//SimpleIntakeSupplier_tests.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Intake
{
   public class SimpleIntakeSupplier_tests
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "Line 01";
         yield return "Line 02";
         yield return "Line 03";
         yield return "Line 04";
         yield return "Line 05";
         yield return "Line 06";
         yield return "Line 07";
         yield return "Line 08";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public SimpleIntakeSupplier_tests()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Raw
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.DeferTransformation = DeferTransformation.Indefinitely;  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)

         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
      }


      [Fact]
      public void ProcessIntake_IntakeSupplier_SourceNoAlways1()
      {
         //arrange
         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(8); // 8 single record clusters
         ValidateResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList());
      }


      [Fact]
      public void ProcessIntake_SimpleSupplier_SourceNoAlways1()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).ExternalTupleSupplier);

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(8); // 8 single record clusters
         ValidateResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList());
      }


      [Fact]
      public void ProcessIntake_AsyncIntakeSupplier_SourceNoAlways1()
      {
         //arrange
         _config.AsyncIntake = true;
         _config.SetAsyncIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).AsyncStringSupplier);

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(8); // 8 single record clusters
         ValidateResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList());
      }


      [Fact]
      public void ProcessIntake_SimpleAsyncSupplier_SourceNoAlways1()
      {
         //arrange
         _config.AsyncIntake = true;
         _config.SetAsyncIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).AsyncExternalTupleSupplier);

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(8); // 8 single record clusters
         ValidateResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList());
      }


      private void ValidateResultingRecords(List<IRecord> resultingRecords)
      {
         resultingRecords.Should().HaveCount(8);
         resultingRecords.Should().OnlyContain(r => r.SourceNo == 1);
         resultingRecords[0].RecNo.Should().Be(1);
         resultingRecords[0]["RAW_REC"].Should().Be("Line 01");
         resultingRecords[1].RecNo.Should().Be(2);
         resultingRecords[1]["RAW_REC"].Should().Be("Line 02");
         resultingRecords[2].RecNo.Should().Be(3);
         resultingRecords[2]["RAW_REC"].Should().Be("Line 03");
         resultingRecords[3].RecNo.Should().Be(4);
         resultingRecords[3]["RAW_REC"].Should().Be("Line 04");
         resultingRecords[4].RecNo.Should().Be(5);
         resultingRecords[4]["RAW_REC"].Should().Be("Line 05");
         resultingRecords[5].RecNo.Should().Be(6);
         resultingRecords[5]["RAW_REC"].Should().Be("Line 06");
         resultingRecords[6].RecNo.Should().Be(7);
         resultingRecords[6]["RAW_REC"].Should().Be("Line 07");
         resultingRecords[7].RecNo.Should().Be(8);
         resultingRecords[7]["RAW_REC"].Should().Be("Line 08");
      }

   }
}
