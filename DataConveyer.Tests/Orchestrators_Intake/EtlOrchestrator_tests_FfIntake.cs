//EtlOrchestrator_tests_FfIntake.cs
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
using Mavidian.DataConveyer.Intake;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_FfIntake
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "1966  11  34943905";
         yield return "1966  12  43004840";
         yield return "1967  01  48517313";
         yield return "1967  02  47431583";
         yield return "1967  03  42977055";
         yield return "1967  04  33111782";
         yield return "1967  05\"  279438\"";  //3rd value surrounded by quotes
         yield return "1967  06  14367907";
         yield return "1967  07  8209985";
         yield return "1967  08  5308387";
      }

      private IEnumerable<string> _intakeLinesWithHeaderRow()
      {
         yield return "YEAR  MONTH NUMERIC DATA";
         yield return "1966  12    43004840";
         yield return "1967  01    48517313";
         yield return "1967  07    8209985";
         yield return "1967  08    5308387";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_FfIntake()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Flat,
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessPipeline_TrimValues_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.RetainQuotes = false;
         _config.TrimInputValues = true;
         _config.InputFields = "Year|4,Month|4,Data|12";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(10);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1966");
         kvRec["Year"].Should().Be("1966");
         kvRec.Keys[1].Should().Be("Month");
         kvRec[1].Should().Be("11");
         kvRec["Month"].Should().Be("11");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("34943905");
         kvRec["Data"].Should().Be("34943905");

         kvRec = resultingClusters[9][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1967");
         kvRec["Year"].Should().Be("1967");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be("08");
         kvRec["Month"].Should().Be("08");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("5308387");
         kvRec["Data"].Should().Be("5308387");

         resultingClusters[6][0]["Data"].Should().Be("279438");  //quotes are stripped
      }
      [Fact]
      public void ProcessPipeline_RetainQuotes_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.RetainQuotes = true;
         _config.TrimInputValues = true;
         _config.InputFields = "Year|4,Month|4,Data|12";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(10);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1966");
         kvRec["Year"].Should().Be("1966");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be("11");
         kvRec["Month"].Should().Be("11");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("34943905");
         kvRec["Data"].Should().Be("34943905");

         kvRec = resultingClusters[9][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1967");
         kvRec["Year"].Should().Be("1967");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be("08");
         kvRec["Month"].Should().Be("08");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("5308387");
         kvRec["Data"].Should().Be("5308387");

         resultingClusters[6][0]["Data"].Should().Be("\"  279438\"");  //quotes are retained
      }


      [Fact]
      public void ProcessPipeline_NoTrimValues_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.InputFields = "Year|4,Month|4,Data|12";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(10);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1966");
         kvRec["Year"].Should().Be("1966");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be("  11");
         kvRec["Month"].Should().Be("  11");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("  34943905");
         kvRec["Data"].Should().Be("  34943905");

         kvRec = resultingClusters[9][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1967");
         kvRec["Year"].Should().Be("1967");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be("  08");
         kvRec["Month"].Should().Be("  08");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("  5308387");  //note records ends w/o filling all width of the last field (11 of 12 chars)
         kvRec["Data"].Should().Be("  5308387");
      }


      [Fact]
      public void ProcessPipeline_NoInputFieldsDefined_RecordsWithNoFields()
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

         resultingClusters.Should().HaveCount(9);  //1st row interpreted as header row

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(0);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(0);

         kvRec = resultingClusters[8][0];
         kvRec.Count.Should().Be(0);
      }


      [Fact]
      public void ProcessPipeline_EmptyInputFieldsDefined_RecordsWithDefaultField()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.InputFields = string.Empty;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(10);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(1);

         //Empty input fields means a single field with a default name and a default width

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("Fld001");  //default field name
         kvRec[0].Should().Be("1966  11  ");   //default field length is 10, hence first 10 characters
         kvRec["Fld001"].Should().Be("1966  11  ");
         kvRec.GetItem(0).Key.Should().Be("Fld001");
         kvRec.GetItem(0).StringValue.Should().Be("1966  11  ");

         kvRec = resultingClusters[9][0];
         kvRec.Keys[0].Should().Be("Fld001");
         kvRec[0].Should().Be("1967  08  ");
         kvRec["Fld001"].Should().Be("1967  08  ");
         kvRec.GetItem(0).Key.Should().Be("Fld001");
         kvRec.GetItem(0).StringValue.Should().Be("1967  08  ");
      }


      [Fact]
      public void ProcessPipeline_TypedFields_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.TrimInputValues = true;
         _config.InputFields = "Year|4,Month|4,Data|12";
         _config.ExplicitTypeDefinitions = "Year|S,Data|M,Month|I";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(10);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1966");
         kvRec["Year"].Should().Be("1966");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be(11);
         kvRec["Month"].Should().Be(11);
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be(34943905m);
         kvRec["Data"].Should().Be(34943905m);

         kvRec = resultingClusters[8][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1967");
         kvRec["Year"].Should().Be("1967");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be(7);
         kvRec["Month"].Should().Be(7);
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be(8209985m);
         kvRec["Data"].Should().Be(8209985m);
      }


      [Fact]
      public void ProcessPipeline_HeaderRow_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLinesWithHeaderRow()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.TrimInputValues = true;
         _config.InputFields = "|4,|7,|13";

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(4);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("YEAR");
         kvRec[0].Should().Be("1966");
         kvRec["YEAR"].Should().Be("1966");
         kvRec.GetItem(1).Key.Should().Be("MONTH");
         kvRec[1].Should().Be("12");
         kvRec["MONTH"].Should().Be("12");
         kvRec.GetItem(2).Key.Should().Be("NUMERIC DATA");
         kvRec[2].Should().Be("43004840");
         kvRec["NUMERIC DATA"].Should().Be("43004840");

         kvRec = resultingClusters[2][0];
         kvRec.Keys[0].Should().Be("YEAR");
         kvRec[0].Should().Be("1967");
         kvRec["YEAR"].Should().Be("1967");
         kvRec.GetItem(1).Key.Should().Be("MONTH");
         kvRec[1].Should().Be("07");
         kvRec["MONTH"].Should().Be("07");
         kvRec.GetItem(2).Key.Should().Be("NUMERIC DATA");
         kvRec[2].Should().Be("8209985");
         kvRec["NUMERIC DATA"].Should().Be("8209985");
      }


      [Fact]
      public void ProcessPipeline_DefaultInputFieldWidth_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLinesWithHeaderRow()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.TrimInputValues = true;
         _config.InputFields = "|4,|7,|";  //3rd field width will be the default of 8, which will chop off the the trailing characters
         _config.DefaultInputFieldWidth = 8;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(4);

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         var kvRec = resultingClusters[0][0];
         kvRec.GetItem(2).Key.Should().Be("NUMERIC");  //trailing chars chopped off
         kvRec[2].Should().Be("4300484");  //last char chopped off 
         kvRec["NUMERIC DATA"].Should().BeNull();  //bad key (field name)
         kvRec["NUMERIC"].Should().Be("4300484");

         kvRec = resultingClusters[2][0];
         kvRec.GetItem(2).Key.Should().Be("NUMERIC");
         kvRec[2].Should().Be("8209985");  //nothing chopped here, fit within 8
         kvRec["NUMERIC DATA"].Should().BeNull();
         kvRec["NUMERIC"].Should().Be("8209985");
      }


      [Fact]
      public void ProcessPipeline_DifferentialClustering_CorrectSplit()
      {
         //This test verifies record clustering when comparing previous to current record contents

         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = false;
         _config.RetainQuotes = false;
         _config.TrimInputValues = true;
         _config.InputFields = "Year|4,Month|4,Data|12";
         _config.ClusterMarker = (rec, prevRec, recCnt) =>
         {
            if (prevRec == null) return true;  //needed to prevent null reference exception for the first record
            return (string)rec["Year"] != (string)prevRec["Year"];
         };  //records with the same Year value are clustered together
         _config.MarkerStartsCluster = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);  //1966 & 1967

         ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse.Count.Should().Be(3);

         resultingClusters[0].Count.Should().Be(2);
         resultingClusters[1].Count.Should().Be(8);

         var kvRec = resultingClusters[0][0];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1966");
         kvRec["Year"].Should().Be("1966");
         kvRec.Keys[1].Should().Be("Month");
         kvRec[1].Should().Be("11");
         kvRec["Month"].Should().Be("11");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("34943905");
         kvRec["Data"].Should().Be("34943905");

         kvRec = resultingClusters[1][7];
         kvRec.Keys[0].Should().Be("Year");
         kvRec[0].Should().Be("1967");
         kvRec["Year"].Should().Be("1967");
         kvRec.GetItem(1).Key.Should().Be("Month");
         kvRec[1].Should().Be("08");
         kvRec["Month"].Should().Be("08");
         kvRec.GetItem(2).Key.Should().Be("Data");
         kvRec[2].Should().Be("5308387");
         kvRec["Data"].Should().Be("5308387");
      }


   }
}
