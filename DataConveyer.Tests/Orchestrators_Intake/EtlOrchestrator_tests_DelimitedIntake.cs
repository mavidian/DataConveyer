//EtlOrchestrator_tests_DelimitedIntake.cs
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
using Mavidian.DataConveyer.Intake;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_DelimitedIntake
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "ID,Name,City,Country,IATA/FAA,ICAO,Latitude,Longitude,Altitude,Offset from UTC,DST,Timezone";
         yield return "1,\"Goroka\",\"Goroka\",  \"Papua New Guinea\",\"GKA\",\"AYGA\",-6.081689,145.391881,5282,10,true,\"Pacific/Port_Moresby\"";
         yield return "2,\"Madang\",\"Madang\",\"Papua New Guinea\",\"MAG\",\"AYMD\",-5.207083,145.7887,20,10,true,\"Pacific/Port_Moresby\"";
         yield return "3,\"Mount Hagen\",\"Mount Hagen\",\"Papua New Guinea\",\"HGU\",\"AYMH\",-5.826789,144.295861,5388,10,false,\"Pacific/Port_Moresby\"";
         yield return "4,\"Nadzab\",\"Nadzab\",\"Papua New Guinea\",\"LAE\",\"AYNZ\",-6.569828,146.726242,239,10,false,\"Pacific/Port_Moresby\"";

      }

      private IEnumerable<string> _intakeLines_fieldsAddedOnTheFly()
      {
         yield return "ID,Name";   //header row defines 2 fields
         yield return "1,Joe,12";  //3 fields
         yield return "2,Jim";     //2 fields
         yield return "3,,15,x";   //4 fields
         yield return "4";         //1 field
         yield return "5,";        //2 fields
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_DelimitedIntake()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Delimited,
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessPipeline_SimpleConfig_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(4);  //4 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(12);

         var kvRec = resultingClusters[0][0];
         kvRec["ID"].Should().Be("1");
         kvRec["Country"].Should().Be("Papua New Guinea");
         kvRec["IATA/FAA"].Should().Be("GKA");
         kvRec["Latitude"].Should().Be("-6.081689");
         kvRec["Offset from UTC"].Should().Be("10");
         kvRec["Timezone"].Should().Be("Pacific/Port_Moresby");

         kvRec = resultingClusters[3][0];
         kvRec[0].Should().Be("4");
         kvRec[1].Should().Be("Nadzab");
         kvRec["ICAO"].Should().Be("AYNZ");
         kvRec[11].Should().Be("Pacific/Port_Moresby");
         kvRec[12].Should().BeNull();
      }

      [Fact]
      public void ProcessPipeline_NoHeaderRow_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.AllowOnTheFlyInputFields = true;  //otherwise, the field count would be 0

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(5);  //5 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(12);

         var kvRec = resultingClusters[0][0];  //header row is interpreted as first data row, field names are defaults
         kvRec["Fld001"].Should().Be("ID");
         kvRec["Fld004"].Should().Be("Country");
         kvRec["Fld005"].Should().Be("IATA/FAA");
         kvRec["Fld007"].Should().Be("Latitude");
         kvRec["Fld010"].Should().Be("Offset from UTC");
         kvRec["Fld012"].Should().Be("Timezone");

         kvRec = resultingClusters[1][0];
         kvRec["Fld001"].Should().Be("1");
         kvRec["Fld004"].Should().Be("Papua New Guinea");
         kvRec["Fld005"].Should().Be("GKA");
         kvRec["Fld007"].Should().Be("-6.081689");
         kvRec["Fld010"].Should().Be("10");
         kvRec["Fld012"].Should().Be("Pacific/Port_Moresby");

         kvRec = resultingClusters[4][0];
         kvRec[0].Should().Be("4");
         kvRec[1].Should().Be("Nadzab");
         kvRec["Fld006"].Should().Be("AYNZ");
         kvRec[11].Should().Be("Pacific/Port_Moresby");
         kvRec[12].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_DisallowOnTheFlyFields_ItemsExcluded()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines_fieldsAddedOnTheFly()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.AllowOnTheFlyInputFields = false;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(5);  //5 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(2);  //each record should only have ID and Name fields

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(2);           //3rd field (12) is excluded
         kvRec.Keys[0].Should().Be("ID");
         kvRec[0].Should().Be("1");
         kvRec.GetItem("ID").Key.Should().Be("ID");
         kvRec["ID"].Should().Be("1");
         kvRec.GetItem(1).Key.Should().Be("Name");
         kvRec[1].Should().Be("Joe");

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(2);
         kvRec["ID"].Should().Be("2");
         kvRec["Name"].Should().Be("Jim");

         kvRec = resultingClusters[2][0];
         kvRec.Count.Should().Be(2);            //3rd and 4th fields are excluded
         kvRec["ID"].Should().Be("3");
         kvRec["Name"].Should().Be(string.Empty);

         kvRec = resultingClusters[3][0];
         kvRec.Count.Should().Be(1);       //there isn't even Name field in this record
         kvRec["ID"].Should().Be("4");

         kvRec = resultingClusters[4][0];
         kvRec.Count.Should().Be(2);
         kvRec["ID"].Should().Be("5");
         kvRec["Name"].Should().Be(string.Empty);

      }

      [Fact]
      public void ProcessPipeline_TypedFields_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.RetainQuotes = true;
         _config.ExplicitTypeDefinitions = "Offset from UTC|I,Latitude|M,Longitude|M,Altitude|M,ID|I,DST|B";
         _config.TypeDefiner = fn => new ItemDef(ItemType.String, null);  //anything, but the fields defined in ExpliciTypeDefinitions will be String

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(4);  //4 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(12);

         var kvRec = resultingClusters[0][0];
         kvRec["ID"].Should().Be(1);
         kvRec["Country"].Should().Be("\"Papua New Guinea\"");
         kvRec["IATA/FAA"].Should().Be("\"GKA\"");
         kvRec["Latitude"].Should().Be(-6.081689m);
         kvRec["Offset from UTC"].Should().Be(10);
         kvRec["Timezone"].Should().Be("\"Pacific/Port_Moresby\"");
         kvRec["DST"].Should().Be(true);

         kvRec = resultingClusters[2][0];
         kvRec[0].Should().Be(3);
         kvRec[1].Should().Be("\"Mount Hagen\"");
         kvRec["ICAO"].Should().Be("\"AYMH\"");
         kvRec[11].Should().Be("\"Pacific/Port_Moresby\"");
         kvRec[12].Should().BeNull();
         kvRec["DST"].Should().Be(false);
      }


      [Fact]
      public void ProcessPipeline_FieldsAddedOnTheFly_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines_fieldsAddedOnTheFly()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(5);  //5 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(4);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(3);  //2 orig fields (from header row) plus 1 added
         kvRec.Keys[0].Should().Be("ID");
         kvRec[0].Should().Be("1");
         kvRec.GetItem(1).Key.Should().Be("Name");
         kvRec[1].Should().Be("Joe");
         kvRec.Keys[2].Should().Be("Fld003");
         kvRec[2].Should().Be("12");
         kvRec["Fld003"].Should().Be("12");

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(2);
         kvRec["Name"].Should().Be("Jim");

         kvRec = resultingClusters[2][0];
         kvRec.Count.Should().Be(4);
         kvRec["Name"].Should().Be(string.Empty);  //In Delimited, 2 consecutive separators mean empty string in between
         kvRec["Fld003"].Should().Be("15");
         kvRec["Fld004"].Should().Be("x");

         kvRec = resultingClusters[3][0];
         kvRec.Count.Should().Be(1);
         kvRec["ID"].Should().Be("4");
         kvRec["Name"].Should().BeNull();  //non-existent

         kvRec = resultingClusters[4][0];
         kvRec.Count.Should().Be(2);
         kvRec["ID"].Should().Be("5");
         kvRec["Name"].Should().Be(string.Empty);  //exists, as there is trailing comma
      }


      [Fact]
      public void ProcessPipeline_SimpleConfigTabDelimited_CorrectData()
      {
         //arrange
         _config.InputFieldSeparator = '\t';
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines().Select(l => l.Replace(',','\t'))).StringSupplier);
         _config.HeadersInFirstInputRow = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(4);  //4 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(12);

         var kvRec = resultingClusters[0][0];
         kvRec["ID"].Should().Be("1");
         kvRec["Country"].Should().Be("Papua New Guinea");
         kvRec["IATA/FAA"].Should().Be("GKA");
         kvRec["Latitude"].Should().Be("-6.081689");
         kvRec["Offset from UTC"].Should().Be("10");
         kvRec["Timezone"].Should().Be("Pacific/Port_Moresby");

         kvRec = resultingClusters[3][0];
         kvRec[0].Should().Be("4");
         kvRec[1].Should().Be("Nadzab");
         kvRec["ICAO"].Should().Be("AYNZ");
         kvRec[11].Should().Be("Pacific/Port_Moresby");
         kvRec[12].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_FieldsOnTheFlyTabDelimited_CorrectData()
      {
         //arrange
         _config.InputFieldSeparator = '\t';
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines_fieldsAddedOnTheFly().Select(l => l.Replace(',','\t'))).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(5);  //5 single-record clusters

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Should().HaveCount(4);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(3);  //2 orig fields (from header row) plus 1 added
         kvRec.Keys[0].Should().Be("ID");
         kvRec[0].Should().Be("1");
         kvRec.GetItem(1).Key.Should().Be("Name");
         kvRec[1].Should().Be("Joe");
         kvRec.Keys[2].Should().Be("Fld003");
         kvRec[2].Should().Be("12");
         kvRec["Fld003"].Should().Be("12");

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(2);
         kvRec["Name"].Should().Be("Jim");

         kvRec = resultingClusters[2][0];
         kvRec.Count.Should().Be(4);
         kvRec["Name"].Should().Be(string.Empty);  //In Delimited, 2 consecutive separators mean empty string in between
         kvRec["Fld003"].Should().Be("15");
         kvRec["Fld004"].Should().Be("x");

         kvRec = resultingClusters[3][0];
         kvRec.Count.Should().Be(1);
         kvRec["ID"].Should().Be("4");
         kvRec["Name"].Should().BeNull();  //non-existent

         kvRec = resultingClusters[4][0];
         kvRec.Count.Should().Be(2);
         kvRec["ID"].Should().Be("5");
         kvRec["Name"].Should().Be(string.Empty);  //exists, as there is trailing comma
      }



   }
}
