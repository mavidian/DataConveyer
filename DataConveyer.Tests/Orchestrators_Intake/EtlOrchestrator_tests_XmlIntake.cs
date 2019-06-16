//EtlOrchestrator_tests_XmlIntake.cs
//
// Copyright © 2018-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using System.IO;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_XmlIntake
   {
      private readonly OrchestratorConfig _config;
      private readonly TextReader _intakeReader;

      private const string _intake = @"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Root>
	<Members>
		<Member>This contents will be ignored.</Member>
	</Members>
	<Members q=""good"">
		<Member>
			<ID>1</ID>
			<FName>Paul</FName>
			<LName>Smith</LName>
			<DOB>1/12/1988</DOB>
			<Empty1></Empty1>
			<Empty2/>
		</Member>
		<Member no=""2"">
			<ID>2</ID>
			<FName blah=""blah"">John</FName>
			<LName>Green</LName>
			<DOB>8/23/1967</DOB>
		</Member>
		<Member>
			<ID>3</ID>
			<FName>Joseph</FName>
			<LName>Doe</LName>
			<DOB>11/6/1994</DOB>
			<FName>Dup'd Joseph</FName>
		</Member>
	</Members>
	<Members q=""good"">
		<Member>This contents will also be ignored.</Member>
	</Members>
</Root>";

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_XmlIntake()
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
      public void ProcessXmlIntake_SimpleInput_CorrectData()
      {
         //arrange
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "CollectionNode|Root/Members[@q=\"good\"],RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);  //3 clusters, each with a single record

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         var rec = clstr[0];
         rec.Count.Should().Be(6);
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["Empty2"].Should().Be(string.Empty);

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  //just 4 inner nodes (attributes are not included)
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");  //attribute on the inner node doesn't matter
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  //5 inner nodes, but 1 dup
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  //dups ignored (by default)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
      }


      [Fact]
      public void ProcessXmlIntake_SimpleInputWithTypeDefs_CorrectData()
      {
         //arrange
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "CollectionNode|Root/Members[@q=\"good\"],RecordNode|Member,IncludeAttributes|true";
         _config.ExplicitTypeDefinitions = "ID|I,DOB|D";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Count.Should().Be(3);  //3 clusters, each with a single record

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         var rec = clstr[0];
         rec.Count.Should().Be(6);
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be(1);
         rec["FName"].Should().Be("Paul");
         rec["Empty2"].Should().Be(string.Empty);

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(6);  //2 attributes + 4 inner nodes
         rec.ClstrNo.Should().Be(2);
         rec["@no"].Should().Be("2");
         rec["ID"].Should().Be(2);
         rec["FName"].Should().Be("John");
         rec["FName.@blah"].Should().Be("blah");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be(new DateTime(1967, 8, 23));

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);  //5 inner nodes, but 1 dup
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be(3);
         rec["FName"].Should().Be("Joseph");  //dups ignored (by default)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be(new DateTime(1994, 11, 6));
      }


      [Fact]
      public void ProcessXmlIntake_InputFieldsDefined_CorrectData()
      {
         //arrange
         _config.InputFields = "ID,FName";
         _config.XmlJsonIntakeSettings = "CollectionNode|Root/Members[@q=\"good\"],RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);  //3 clusters, each with a single record

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(1);  //a single record cluster
         var rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["Empty2"].Should().BeNull();

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);  //1 attribute + 4 inner nodes
         rec.ClstrNo.Should().Be(2);
         rec["no"].Should().BeNull();
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");  //attribute on the inner node doesn't matter
         rec["LName"].Should().BeNull();

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);  //5 inner nodes, but 1 dup
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  //dups ignored (by default)
         rec["LName"].Should().BeNull();
         rec["dummy"].Should().BeNull();
      }

   }
}
