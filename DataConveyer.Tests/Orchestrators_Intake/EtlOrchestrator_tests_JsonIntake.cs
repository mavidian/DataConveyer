﻿//EtlOrchestrator_tests_JsonIntake.cs
//
// Copyright © 2018-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   public class EtlOrchestrator_tests_JsonIntake
   {
      private readonly OrchestratorConfig _config;
      private readonly TextReader _intakeReader;

      private const string _intake = @"{
  ""Root"": {
    ""Members"": [
      {
        ""Member"": [
          {
            ""ID"": ""1"",
            ""FName"": ""Paul"",
            ""LName"": ""Smith"",
            ""DOB"": ""1/12/1988"",
            ""Empty1"": """",
            ""Empty2"": """"
          },
          {
            ""ID"": ""2"",
            ""FName"": ""John"",
            ""LName"": ""Green"",
            ""DOB"": ""8/23/1967""
          },
          {
            ""ID"": ""3"",
            ""FName"": [
              ""Joseph"",
              ""Dup'd Joseph""
            ],
            ""LName"": ""Doe"",
            ""DOB"": ""11/6/1994""
          }
        ]
      },
      {
        ""Member"": {
          ""ID"": ""-1"",
          ""Data"": ""Same level, not ignored in JSON.""
        }
      },
      {
        ""Member"": {
          ""ID"": ""-2"",
          ""Data"": ""Same level, not ignored in JSON.""
         }
       }
    ]
  }
}";

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_JsonIntake()
      {
         _config = new OrchestratorConfig
         {
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _intakeReader = new StringReader(_intake);

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessJsonIntake_SimpleInput_CorrectData()
      {
         //arrange
         _config.IntakeReader = () => _intakeReader;
         _config.InputDataKind = KindOfTextData.JSON;
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "CollectionNode|Root/Members,RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(5);  //5 clusters, each with a single record

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
         rec.Count.Should().Be(4);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  ///Dup'd Joseph ignored when KeyValRecord got constructed (ActionOnDuplicateKey.IgnoreItem)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");

         clstr = resultingClusters[3];
         clstr.ClstrNo.Should().Be(4);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.ClstrNo.Should().Be(4);
         rec["ID"].Should().Be("-1");
         rec["Data"].Should().Be("Same level, not ignored in JSON.");

         clstr = resultingClusters[4];
         clstr.ClstrNo.Should().Be(5);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.ClstrNo.Should().Be(5);
         rec["ID"].Should().Be("-2");
         rec["Data"].Should().Be("Same level, not ignored in JSON.");
      }


      [Fact]
      public void ProcessJsonIntakeAsync_SimpleInput_CorrectData()
      {
         //arrange
         _config.AsyncIntake = true;
         _config.IntakeReader = () => _intakeReader;
         _config.InputDataKind = KindOfTextData.JSON;
         _config.AllowOnTheFlyInputFields = true;
         _config.XmlJsonIntakeSettings = "CollectionNode|Root/Members,RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(5);  //5 clusters, each with a single record

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
         rec.Count.Should().Be(4);
         rec.ClstrNo.Should().Be(2);
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");
         rec["LName"].Should().Be("Green");
         rec["DOB"].Should().Be("8/23/1967");

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(4);
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  ///Dup'd Joseph ignored when KeyValRecord got constructed (ActionOnDuplicateKey.IgnoreItem)
         rec["LName"].Should().Be("Doe");
         rec["DOB"].Should().Be("11/6/1994");

         clstr = resultingClusters[3];
         clstr.ClstrNo.Should().Be(4);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.ClstrNo.Should().Be(4);
         rec["ID"].Should().Be("-1");
         rec["Data"].Should().Be("Same level, not ignored in JSON.");

         clstr = resultingClusters[4];
         clstr.ClstrNo.Should().Be(5);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.ClstrNo.Should().Be(5);
         rec["ID"].Should().Be("-2");
         rec["Data"].Should().Be("Same level, not ignored in JSON.");
      }


      [Fact]
      public void ProcessJsonIntake_InputFieldsDefined_CorrectData()
      {
         //arrange
         _config.IntakeReader = () => _intakeReader;
         _config.InputDataKind = KindOfTextData.JSON;
         _config.InputFields = "ID,FName";
         _config.XmlJsonIntakeSettings = "CollectionNode|Root/Members,RecordNode|Member";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         _resultingClusters.Count.Should().Be(5);  //5 clusters, each with a single record

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
         rec.Count.Should().Be(2);
         rec.ClstrNo.Should().Be(2);
         rec["no"].Should().BeNull();
         rec["ID"].Should().Be("2");
         rec["FName"].Should().Be("John");
         rec["LName"].Should().BeNull();

         clstr = resultingClusters[2];
         clstr.ClstrNo.Should().Be(3);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(2);
         rec.ClstrNo.Should().Be(3);
         rec["ID"].Should().Be("3");
         rec["FName"].Should().Be("Joseph");  ///Dup'd Joseph ignored when KeyValRecord got constructed (ActionOnDuplicateKey.IgnoreItem)
         rec["LName"].Should().BeNull();
         rec["dummy"].Should().BeNull();

         clstr = resultingClusters[3];
         clstr.ClstrNo.Should().Be(4);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(1);  //only ID defined, Data didn't make it
         rec.ClstrNo.Should().Be(4);
         rec["ID"].Should().Be("-1");

         clstr = resultingClusters[4];
         clstr.ClstrNo.Should().Be(5);
         clstr.Count.Should().Be(1);  //a single record cluster
         rec = clstr[0];
         rec.Count.Should().Be(1);  //only ID defined, Data didn't make it
         rec.ClstrNo.Should().Be(5);
         rec["ID"].Should().Be("-2");
      }

   }
}
