//EtlOrchestrator_tests_XmlClusterAtIntake.cs
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
using System.IO;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_XmlClusterAtIntake
   {
      private readonly OrchestratorConfig _config;
      private readonly TextReader _intakeReader;

      private const string _intake = @"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Family>
</Family>
<Family>
	<Member>
		<ID>1</ID>
		<FName>Paul</FName>
		<LName>Smith</LName>
		<DOB>1/12/1988</DOB>
	</Member>
</Family>
<Family/>
<Family>
	<Member>
		<ID>2</ID>
		<FName>John</FName>
		<LName>Green</LName>
		<DOB>8/23/1967</DOB>
	</Member>
	<Member>
		<ID>4</ID>
		<FName>Johny</FName>
		<LName>Green</LName>
		<DOB>5/3/1997</DOB>
	</Member>
</Family>
<Family>
	<Member>
		<ID>3</ID>
		<FName>Joseph</FName>
		<LName>Doe</LName>
		<DOB>11/6/1994</DOB>
	</Member>
</Family>
<Family><Dummy/></Family>";

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_XmlClusterAtIntake()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.XML,
            IntakeReader = () => _intakeReader,
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _intakeReader = new StringReader(_intake);

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessXmlIntake_ClstrOnIntakeAssigned_CorrectData()
      {
         //arrange
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "ClusterNode|Family,RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Should().HaveCount(3);  //3 clusters

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         var rec = clstr[0];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["LName"].Should().Be("Smith");
         rec["DOB"].Should().Be("1/12/1988");

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(2);  //2 records in 2nd cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(2);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");
         rec = clstr[1];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(3);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("4");
         rec["FName"].Should().Be("Johny");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("5/3/1997");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  //4 inner nodes
         rec.RecNo.Should().Be(4);
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
      }


      [Fact]
      public void ProcessXmlIntakeAsync_ClstrOnIntakeAssigned_CorrectData()
      {
         //arrange
         _config.AsyncIntake = true;
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "ClusterNode|Family,RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Should().HaveCount(3);  //3 clusters

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         var rec = clstr[0];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["LName"].Should().Be("Smith");
         rec["DOB"].Should().Be("1/12/1988");

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(2);  //2 records in 2nd cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(2);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");
         rec = clstr[1];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(3);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("4");
         rec["FName"].Should().Be("Johny");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("5/3/1997");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  //4 inner nodes
         rec.RecNo.Should().Be(4);
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
      }


      [Fact]
      public void ProcessXmlIntake_ClstrOnIntakeWithClusterMarker_CorrectData()
      {
         //arrange
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "ClusterNode|Family,RecordNode|Member";
         _config.ClusterMarker = (r, pr, n) =>
         {  //go by the initial ClstrNo values (assigned at XML intake), except for splicing Green and Doe families together
            if (pr == null) return true;
            if (r.ClstrNo == pr.ClstrNo) return false;
            return (string)r["LName"] != "Doe" || (string)pr["LName"] != "Green";
         };

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Should().HaveCount(2); // 1st clstr: Smith, 2nd clstr: Green & Doe together

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         var rec = clstr[0];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["LName"].Should().Be("Smith");
         rec["DOB"].Should().Be("1/12/1988");

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(3);  //2 Greens plus 1 Doe
         rec = clstr[0];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(2);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");
         rec = clstr[1];
         rec.Count.Should().Be(4);  // 4 inner nodes
         rec.RecNo.Should().Be(3);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("4");
         rec["FName"].Should().Be("Johny");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("5/3/1997");
         rec = clstr[2];
         rec.Count.Should().Be(4);  //4 inner nodes
         rec.RecNo.Should().Be(4);
         rec.ClstrNo.Should().Be(2);  //here, Doe is clustered together with Green, i.e. ClstrNo 2
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
      }

   }
}
