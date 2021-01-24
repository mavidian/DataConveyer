//EtlOrchestrator_tests_XmlIntakeClusters.cs
//
// Copyright © 2019-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   public class EtlOrchestrator_tests_XmlIntakeClusters
   {
      private readonly OrchestratorConfig _config;
      private readonly TextReader _intakeReader;

      private const string _intake = @"<?xml version=""1.0"" encoding=""UTF-8"" ?>
<Root>
  <Family a=""A"" b=""BB"">
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
    </Members>
  </Family>
  <Family x=""XX"" yy=""YYY"">
    <Members>
      <Member>
        <ID>3</ID>
        <FName>Joseph</FName>
        <LName>Doe</LName>
        <DOB>11/6/1994</DOB>
        <FName>Dup'd Joseph</FName>
      </Member>
    </Members>
  </Family>
  <Family>
    <Members q=""good"">
      <Member>Empty member record</Member>
    </Members>
  </Family>
</Root>";

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_XmlIntakeClusters()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.XML,
            IntakeReader = () => _intakeReader,
            AllowOnTheFlyInputFields = true,
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _intakeReader = new StringReader(_intake);

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessXmlIntake_baseline_EmptyTraceBin()
      {
         //arrange
         _config.XmlJsonIntakeSettings = "CollectionNode|Root,ClusterNode|Family/Members,RecordNode|Member,IncludeExplicitText|true";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(2);
         var rec = clstr[0];
         rec.Count.Should().Be(6);
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["Empty2"].Should().Be(string.Empty);
         rec.TraceBin.Should().BeNull();

         rec = clstr[1];
         rec.Count.Should().Be(4);  //just 4 inner nodes (attributes are not included)
         rec.RecNo.Should().Be(2);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");  //attribute on the inner node doesn't matter
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");
         rec.TraceBin.Should().BeNull();

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);
         rec = clstr[0];
         rec.Count.Should().Be(4);  //5 inner nodes, but 1 dup
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  //dups ignored (by default)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
         rec.TraceBin.Should().BeNull();

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);
         rec = clstr[0];
         rec.Count.Should().Be(1);
         rec.ClstrNo.Should().Be(3);
         rec["__explicitText__"].Should().Be("Empty member record");
         rec.TraceBin.Should().BeNull();
      }


      [Fact]
      public void ProcessXmlIntakeAsync_baseline_EmptyTraceBin()
      {
         //arrange
         _config.AsyncIntake = true;
         _config.XmlJsonIntakeSettings = "CollectionNode|Root,ClusterNode|Family/Members,RecordNode|Member,IncludeExplicitText|true";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(2);
         var rec = clstr[0];
         rec.Count.Should().Be(6);
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["Empty2"].Should().Be(string.Empty);
         rec.TraceBin.Should().BeNull();

         rec = clstr[1];
         rec.Count.Should().Be(4);  //just 4 inner nodes (attributes are not included)
         rec.RecNo.Should().Be(2);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");  //attribute on the inner node doesn't matter
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");
         rec.TraceBin.Should().BeNull();

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);
         rec = clstr[0];
         rec.Count.Should().Be(4);  //5 inner nodes, but 1 dup
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  //dups ignored (by default)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
         rec.TraceBin.Should().BeNull();

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);
         rec = clstr[0];
         rec.Count.Should().Be(1);
         rec.ClstrNo.Should().Be(3);
         rec["__explicitText__"].Should().Be("Empty member record");
         rec.TraceBin.Should().BeNull();
      }


      [Fact]
      public void ProcessXmlIntake_AddClusterDataToTraceBin_TraceBinPopulated()
      {
         //arrange
         _config.XmlJsonIntakeSettings = "CollectionNode|Root,ClusterNode|Family/Members,RecordNode|Member,IncludeExplicitText|true,AddClusterDataToTraceBin|true";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(3);

         var clstr = resultingClusters[0];
         clstr.ClstrNo.Should().Be(1);
         clstr.Count.Should().Be(2);
         var rec = clstr[0];
         rec.Count.Should().Be(6);
         rec.RecNo.Should().Be(1);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("1");
         rec["FName"].Should().Be("Paul");
         rec["Empty2"].Should().Be(string.Empty);
         rec.TraceBin.Should().HaveCount(3);
         rec.TraceBin["Family.a"].Should().Be("A");
         rec.TraceBin["Family.b"].Should().Be("BB");
         rec.TraceBin["Family.Members.q"].Should().Be("good");

         rec = clstr[1];
         rec.Count.Should().Be(4);  //just 4 inner nodes (attributes are not included)
         rec.RecNo.Should().Be(2);
         rec.ClstrNo.Should().Be(1);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");  //attribute on the inner node doesn't matter
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");
         rec.TraceBin.Should().HaveCount(3);
         rec.TraceBin["Family.a"].Should().Be("A");
         rec.TraceBin["Family.b"].Should().Be("BB");
         rec.TraceBin["Family.Members.q"].Should().Be("good");

         clstr = resultingClusters[1];
         clstr.ClstrNo.Should().Be(2);
         clstr.Count.Should().Be(1);
         rec = clstr[0];
         rec.Count.Should().Be(4);  //5 inner nodes, but 1 dup
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  //dups ignored (by default)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");
         rec.TraceBin.Should().HaveCount(2);
         rec.TraceBin["Family.x"].Should().Be("XX");
         rec.TraceBin["Family.yy"].Should().Be("YYY");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);
         rec = clstr[0];
         rec.Count.Should().Be(1);
         rec.ClstrNo.Should().Be(3);
         rec["__explicitText__"].Should().Be("Empty member record");
         rec.TraceBin.Should().ContainSingle();
         rec.TraceBin["Family.Members.q"].Should().Be("good");
      }

   }
}
