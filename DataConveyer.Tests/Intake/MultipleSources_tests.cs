//MultipleSources_tests.cs
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Intake
{
   public class MultipleSources_tests
   {
      private readonly OrchestratorConfig _config;

      private const string FirstSource = "First of First\r\nSecond of First\r\nThird of First";
      private const string SecondSource = "First of Second\r\nSecond of Second";
      private const string ThirdSource = "First of Third\r\nSecond of Third\r\nThird of Third\r\nFourth of Third";

      private readonly StreamReader[] _intakeReaders;

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public MultipleSources_tests()
      {
         _config = new OrchestratorConfig
         {
            IntakeReaders = () => _intakeReaders,
            InputDataKind = KindOfTextData.Delimited,  //a kind that CanHaveHeaderRow is needed for these tests 
            AllowOnTheFlyInputFields = true,     //so that Fld001 can be added on the fly
            InputFileNames = "dummy",  //irrelevant as _intakeReaders are assigned directly in these tests
            DeferTransformation = DeferTransformation.Indefinitely  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)
         };

         //prepare extraction of the results from the pipeline
         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));

         var firstStream = new MemoryStream(Encoding.UTF8.GetBytes(FirstSource));
         var secondStream = new MemoryStream(Encoding.UTF8.GetBytes(SecondSource));
         var thirdStream = new MemoryStream(Encoding.UTF8.GetBytes(ThirdSource));

         _intakeReaders = (new MemoryStream[] { firstStream, secondStream, thirdStream }).Select(s => new StreamReader(s, Encoding.UTF8)).ToArray();
      }


      [Fact]
      public void processIntake_NoHeadersNoRepeat_CorrectRowsRead()
      {
         //arrange
         _config.HeadersInFirstInputRow = false;
         _config.InputHeadersRepeated = false;

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(9); // 9 single record clusters

         ValidateNineResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList(), "Fld001");
      }

      [Fact]
      public void processIntake_NoHeadersRepeat_CorrectRowsRead()
      {
         //arrange
         _config.HeadersInFirstInputRow = false;
         _config.InputHeadersRepeated = true;

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(9); // 9 single record clusters

         ValidateNineResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList(), "Fld001");
      }


      [Fact]
      public void processIntake_Defaults_CorrectRowsRead()
      {
         //should be same as NoHeadersRepeat, i.e. HeadersInFirstInputRow = false and InputHeadersRepeated = true

         //arrange
         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(9); // 9 single record clusters

         ValidateNineResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList(), "Fld001");
      }


      [Fact]
      public void processIntake_HeadersNoRepeat_CorrectRowsRead()
      {
         //arrange
         _config.HeadersInFirstInputRow = true;
         _config.InputHeadersRepeated = false;

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(8); // 8 single record clusters

         var resultingRecords = _resultingClusters.SelectMany(c => c.Records).ToList();

         resultingRecords.Should().HaveCount(8);
         resultingRecords[0].RecNo.Should().Be(1);
         resultingRecords[0].SourceNo.Should().Be(1);
         resultingRecords[0]["First of First"].Should().Be("Second of First");  //note that field name (First of First) is taken from header row
         resultingRecords[1].RecNo.Should().Be(2);
         resultingRecords[1].SourceNo.Should().Be(1);
         resultingRecords[1]["First of First"].Should().Be("Third of First");
         resultingRecords[2].RecNo.Should().Be(3);
         resultingRecords[2].SourceNo.Should().Be(2);
         resultingRecords[2]["First of First"].Should().Be("First of Second");
         resultingRecords[3].RecNo.Should().Be(4);
         resultingRecords[3].SourceNo.Should().Be(2);
         resultingRecords[3]["First of First"].Should().Be("Second of Second");
         resultingRecords[4].RecNo.Should().Be(5);
         resultingRecords[4].SourceNo.Should().Be(3);
         resultingRecords[4]["First of First"].Should().Be("First of Third");
         resultingRecords[5].RecNo.Should().Be(6);
         resultingRecords[5].SourceNo.Should().Be(3);
         resultingRecords[5]["First of First"].Should().Be("Second of Third");
         resultingRecords[6].RecNo.Should().Be(7);
         resultingRecords[6].SourceNo.Should().Be(3);
         resultingRecords[6]["First of First"].Should().Be("Third of Third");
         resultingRecords[7].RecNo.Should().Be(8);
         resultingRecords[7].SourceNo.Should().Be(3);
         resultingRecords[7]["First of First"].Should().Be("Fourth of Third");
      }


      [Fact]
      public void processIntake_HeadersRepeat_CorrectRowsRead()
      {
         //arrange
         _config.HeadersInFirstInputRow = true;
         _config.InputHeadersRepeated = true;

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(6); // 6 single record clusters

         var resultingRecords = _resultingClusters.SelectMany(c => c.Records).ToList();

         resultingRecords.Should().HaveCount(6);
         resultingRecords[0].RecNo.Should().Be(1);
         resultingRecords[0].SourceNo.Should().Be(1);
         resultingRecords[0]["First of First"].Should().Be("Second of First");  //note that field name (First of First) is taken from header row
         resultingRecords[1].RecNo.Should().Be(2);
         resultingRecords[1].SourceNo.Should().Be(1);
         resultingRecords[1]["First of First"].Should().Be("Third of First");
         resultingRecords[2].RecNo.Should().Be(3);
         resultingRecords[2].SourceNo.Should().Be(2);
         resultingRecords[2]["First of First"].Should().Be("Second of Second");  //header rows in subsequent sources are simply ignored, so the field name is still First of First and not First of Second
         resultingRecords[3].RecNo.Should().Be(4);
         resultingRecords[3].SourceNo.Should().Be(3);
         resultingRecords[3]["First of First"].Should().Be("Second of Third");
         resultingRecords[4].RecNo.Should().Be(5);
         resultingRecords[4].SourceNo.Should().Be(3);
         resultingRecords[4]["First of First"].Should().Be("Third of Third");
         resultingRecords[5].RecNo.Should().Be(6);
         resultingRecords[5].SourceNo.Should().Be(3);
         resultingRecords[5]["First of First"].Should().Be("Fourth of Third");
      }


      [Fact]
      public void processIntake_HeadersNoRepeatForUnsupportingKind_CorrectRowsRead()
      {
         //this is a border case - Raw and headers should never be used together (Data Conveyer simply treats 1st row as data row and not header in this case)

         //arrange
         _config.InputDataKind = KindOfTextData.Raw;  //Raw kind has CanHaveHeaderRow = false 
         _config.HeadersInFirstInputRow = true;
         _config.InputHeadersRepeated = false;

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(9); // 9 single record clusters

         ValidateNineResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList(), "RAW_REC");
      }


      [Fact]
      public void processIntake_HeadersRepeatForUnsupportingKind_CorrectRowsRead()
      {
         //this is a border case - Raw and headers should never be used together (Data Conveyer simply treats 1st row as data row and not header in this case)

         //arrange
         _config.InputDataKind = KindOfTextData.Raw;  //Raw kind has CanHaveHeaderRow = false 
         _config.HeadersInFirstInputRow = true;
         _config.InputHeadersRepeated = true;

         var orchestrator = TestUtilities.GetTestOrchestrator<KeyValCluster>(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().HaveCount(9); // 9 single record clusters

         ValidateNineResultingRecords(_resultingClusters.SelectMany(c => c.Records).ToList(), "RAW_REC");
      }


      private void ValidateNineResultingRecords(List<IRecord> resultingRecords, string fieldName)
      {
         // fieldName - name of the (only) field in each record, e.g. Fld001 or RAW_REC
         resultingRecords.Should().HaveCount(9);
         resultingRecords[0].RecNo.Should().Be(1);
         resultingRecords[0].SourceNo.Should().Be(1);
         resultingRecords[0][fieldName].Should().Be("First of First");
         resultingRecords[1].RecNo.Should().Be(2);
         resultingRecords[1].SourceNo.Should().Be(1);
         resultingRecords[1][fieldName].Should().Be("Second of First");
         resultingRecords[2].RecNo.Should().Be(3);
         resultingRecords[2].SourceNo.Should().Be(1);
         resultingRecords[2][fieldName].Should().Be("Third of First");
         resultingRecords[3].RecNo.Should().Be(4);
         resultingRecords[3].SourceNo.Should().Be(2);
         resultingRecords[3][fieldName].Should().Be("First of Second");
         resultingRecords[4].RecNo.Should().Be(5);
         resultingRecords[4].SourceNo.Should().Be(2);
         resultingRecords[4][fieldName].Should().Be("Second of Second");
         resultingRecords[5].RecNo.Should().Be(6);
         resultingRecords[5].SourceNo.Should().Be(3);
         resultingRecords[5][fieldName].Should().Be("First of Third");
         resultingRecords[6].RecNo.Should().Be(7);
         resultingRecords[6].SourceNo.Should().Be(3);
         resultingRecords[6][fieldName].Should().Be("Second of Third");
         resultingRecords[7].RecNo.Should().Be(8);
         resultingRecords[7].SourceNo.Should().Be(3);
         resultingRecords[7][fieldName].Should().Be("Third of Third");
         resultingRecords[8].RecNo.Should().Be(9);
         resultingRecords[8].SourceNo.Should().Be(3);
         resultingRecords[8][fieldName].Should().Be("Fourth of Third");
      }
   }
}
