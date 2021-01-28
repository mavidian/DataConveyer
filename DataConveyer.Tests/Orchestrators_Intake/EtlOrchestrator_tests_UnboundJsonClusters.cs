//EtlOrchestrator_tests_UnboundJsonClusters.cs
//
// Copyright © 2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System.IO;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_UnboundJsonClusters
   {
      private readonly OrchestratorConfig _config;
      private readonly TextReader _intakeReader;

      private const string _intake = @"{
  ""Name"": ""Alice""
}
[
  [
    {
      ""Name"": ""Bob""
    },
    {
      ""Name"": ""Charlie""
    },
    {
      ""Name"": ""Dana""
    }
  ],
  {
    ""Name"": ""Ed""
  }
]
{
  ""Name"": ""Frank""
}";

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_UnboundJsonClusters()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.UnboundJSON,
            IntakeReader = () => _intakeReader,
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
      };

         _intakeReader = new StringReader(_intake);

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessUnboundJsonIntake_Baseline_OneRecordClusters()
      {
         // All defaults, no cluster detected on intake; default behavior is cluster on every record

         //arrange
         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Should().HaveCount(6);

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);

         clstr = resultingClusters[3];
         clstr.ClstrNo.Should().Be(4);
         clstr.Count.Should().Be(1);

         clstr = resultingClusters[4];
         clstr.ClstrNo.Should().Be(5);
         clstr.Count.Should().Be(1);

         clstr = resultingClusters[5];
         clstr.ClstrNo.Should().Be(6);
         clstr.Count.Should().Be(1);
      }

      [Fact]
      public void ProcessUnboundJsonIntake_ClstrOnIntake_ClustersByJsonArrays()
      {
         // DetectClusters with default marker creates clusters according to JSON hierarchy (any change in array nesting = new cluster).

         //arrange
         _config.XmlJsonIntakeSettings = "DetectClusters";
         _config.AllowOnTheFlyInputFields = true;  // without it, JSON/XML inake creates empty records, and here we want to read names

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Should().HaveCount(4);

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         clstr[0]["Name"].Should().Be("Alice");

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(3);
         clstr[0]["Name"].Should().Be("Bob");
         clstr[1]["Name"].Should().Be("Charlie");
         clstr[2]["Name"].Should().Be("Dana");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);
         clstr[0]["Name"].Should().Be("Ed");

         clstr = resultingClusters[3];
         clstr.ClstrNo.Should().Be(4);
         clstr.Count.Should().Be(1);
         clstr[0]["Name"].Should().Be("Frank");
      }


      [Fact]
      public void ProcessUnboundJsonIntake_ClstrOnIntakeWithClusterMarker_CombinedClustering()
      {
         //Hybrid clustering: advanced scenario where ClusterMarker reads cluster numbers assigned by JSON intake to make final determination on clustering.

         //arrange
         _config.XmlJsonIntakeSettings = "DetectClusters";
         _config.ClusterMarker = (r, pr, n) =>
         {  //respect original JSON clustering, but split clusters containing more than 2 records
            return (pr == null) || (r.ClstrNo != pr.ClstrNo) || n == 2;
         };

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Should().HaveCount(5);

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(2);

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);

         clstr = resultingClusters[3];
         clstr.ClstrNo.Should().Be(4);
         clstr.Count.Should().Be(1);

         clstr = resultingClusters[4];
         clstr.ClstrNo.Should().Be(5);
         clstr.Count.Should().Be(1);
      }

   }
}
